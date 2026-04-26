"""
LiteLLM / Ollama ↔ MCP bridge — chat with your data using any LLM provider.

Usage:
    python ollama_client.py                                        # stdio, ollama/llama3.2
    python ollama_client.py --model ollama/mistral                 # different Ollama model
    python ollama_client.py --model gpt-4o                        # OpenAI
    python ollama_client.py --model claude-3-5-sonnet-20241022    # Anthropic
    python ollama_client.py --model gemini/gemini-1.5-pro         # Google
    python ollama_client.py --sse http://host/sse                  # k8s SSE deployment

Provider API keys via env vars:
    OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, AZURE_API_KEY, ...
    Ollama models (ollama/*) use the native Ollama SDK — no API key needed.
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import litellm
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

SERVER_SCRIPT = Path(__file__).parent / "server.py"
SCHEMAS_DIR = Path(__file__).parent / "schemas"


def _to_tool_schema(tool, model: str = "") -> dict:
    schema = dict(tool.inputSchema)
    if not model.startswith("gemini/"):
        schema.setdefault("additionalProperties", False)
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": schema,
        },
    }


def _build_system_prompt_from_context(ctx: dict) -> str:
    from semantic import build_system_prompt, load_entities
    return build_system_prompt(load_entities(SCHEMAS_DIR))


# ── Ollama-native agentic loop ─────────────────────────────────────────────────

async def _ollama_chat_loop(session: ClientSession, model: str) -> None:
    """Use the native Ollama SDK — preserves exact original behaviour."""
    import ollama

    ollama_model = model[len("ollama/"):]  # strip prefix for SDK call

    tools_resp = await session.list_tools()
    tools = [_to_tool_schema(t) for t in tools_resp.tools]

    ctx_result = await session.call_tool("get_semantic_context", {})
    ctx = json.loads(ctx_result.content[0].text)
    system_prompt = _build_system_prompt_from_context(ctx)

    print(f"Model  : {model}")
    print(f"Tables : {', '.join(e['name'] for e in ctx.get('entities', []))}")
    print(f"Tools  : {', '.join(t['function']['name'] for t in tools)}")
    print("Type 'quit' to exit.\n")

    history = [{"role": "system", "content": system_prompt}]

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not user_input or user_input.lower() in ("quit", "exit"):
            break

        history.append({"role": "user", "content": user_input})

        while True:
            response = ollama.chat(model=ollama_model, messages=history, tools=tools)
            msg = response.message
            history.append(msg)

            if not msg.tool_calls:
                print(f"\nAssistant: {msg.content}\n")
                break

            for call in msg.tool_calls:
                name = call.function.name
                args = call.function.arguments or {}
                print(f"  → {name}({json.dumps(args)})")

                result = await session.call_tool(name, args)
                content = result.content[0].text if result.content else ""
                history.append({"role": "tool", "content": content})


# ── LiteLLM agentic loop (cloud providers) ────────────────────────────────────

def _normalize_tool_calls(msg) -> list[dict] | None:
    """
    Normalize tool calls to {id, name, arguments(dict)}.
    Handles msg.tool_calls (standard) and content-embedded JSON (some models).
    """
    if msg.tool_calls:
        result = []
        for tc in msg.tool_calls:
            args = tc.function.arguments
            if isinstance(args, str):
                args = json.loads(args or "{}")
            result.append({
                "id": tc.id or f"call_{tc.function.name}",
                "name": tc.function.name,
                "arguments": args,
            })
        return result or None

    # Fallback: some models embed tool calls as JSON in content
    content = (msg.content or "").strip()
    if not (content.startswith("{") or content.startswith("[")):
        return None
    try:
        parsed = json.loads(content)
        # Handle {"toolCalls": [...]} wrapper
        if isinstance(parsed, dict) and "toolCalls" in parsed:
            items = parsed["toolCalls"]
        elif isinstance(parsed, list):
            items = parsed
        else:
            items = [parsed]
        if not items or "function" not in items[0]:
            return None
        result = []
        for item in items:
            fn = item["function"]
            args = fn.get("arguments", {})
            if isinstance(args, str):
                args = json.loads(args or "{}")
            result.append({
                "id": item.get("id") or f"call_{fn['name']}",
                "name": fn["name"],
                "arguments": args,
            })
        return result or None
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


async def _litellm_chat_loop(session: ClientSession, model: str) -> None:
    tools_resp = await session.list_tools()
    tools = [_to_tool_schema(t, model) for t in tools_resp.tools]

    ctx_result = await session.call_tool("get_semantic_context", {})
    ctx = json.loads(ctx_result.content[0].text)
    system_prompt = _build_system_prompt_from_context(ctx)

    print(f"Model  : {model}")
    print(f"Tables : {', '.join(e['name'] for e in ctx.get('entities', []))}")
    print(f"Tools  : {', '.join(t['function']['name'] for t in tools)}")
    print("Type 'quit' to exit.\n")

    history = [{"role": "system", "content": system_prompt}]

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not user_input or user_input.lower() in ("quit", "exit"):
            break

        history.append({"role": "user", "content": user_input})

        while True:
            response = await litellm.acompletion(
                model=model, messages=history, tools=tools
            )
            msg = response.choices[0].message
            tool_calls = _normalize_tool_calls(msg)

            assistant_entry = {"role": "assistant", "content": msg.content or ""}
            if tool_calls:
                assistant_entry["tool_calls"] = [
                    {
                        "id": tc["id"],
                        "type": "function",
                        "function": {
                            "name": tc["name"],
                            "arguments": json.dumps(tc["arguments"]),
                        },
                    }
                    for tc in tool_calls
                ]
                assistant_entry["content"] = ""
            history.append(assistant_entry)

            if not tool_calls:
                print(f"\nAssistant: {msg.content}\n")
                break

            for tc in tool_calls:
                name = tc["name"]
                args = tc["arguments"]
                print(f"  → {name}({json.dumps(args)})")

                result = await session.call_tool(name, args)
                content = result.content[0].text if result.content else ""

                history.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": content,
                })


# ── Entry point ────────────────────────────────────────────────────────────────

async def chat_loop(session: ClientSession, model: str) -> None:
    if model.startswith("ollama/"):
        await _ollama_chat_loop(session, model)
    else:
        await _litellm_chat_loop(session, model)


async def main(model: str, sse_url: str | None) -> None:
    if sse_url:
        from mcp.client.sse import sse_client

        async with sse_client(sse_url) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                await chat_loop(session, model)
    else:
        params = StdioServerParameters(
            command=sys.executable,
            args=[str(SERVER_SCRIPT)],
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                await chat_loop(session, model)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chat with your data via LiteLLM + MCP")
    parser.add_argument(
        "--model",
        default=os.environ.get("LLM_MODEL", "ollama/llama3.2"),
        help=(
            "Model string. Examples:\n"
            "  ollama/llama3.2              (default, local Ollama — native SDK)\n"
            "  gpt-4o                       (OpenAI)\n"
            "  claude-3-5-sonnet-20241022   (Anthropic)\n"
            "  gemini/gemini-1.5-pro        (Google)\n"
            "  azure/<deployment-name>      (Azure OpenAI)\n"
        ),
    )
    parser.add_argument(
        "--sse",
        metavar="URL",
        help="SSE endpoint URL for k8s deployment (e.g. http://localhost:8000/sse)",
    )
    args = parser.parse_args()

    asyncio.run(main(args.model, args.sse))
