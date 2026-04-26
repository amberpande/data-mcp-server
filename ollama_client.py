"""
LiteLLM ↔ MCP bridge — chat with your data using any LLM provider.

Usage:
    python ollama_client.py                                        # stdio, ollama/llama3.2
    python ollama_client.py --model ollama/mistral                 # different Ollama model
    python ollama_client.py --model gpt-4o                        # OpenAI
    python ollama_client.py --model claude-3-5-sonnet-20241022    # Anthropic
    python ollama_client.py --model gemini/gemini-1.5-pro         # Google
    python ollama_client.py --sse http://host/sse                  # k8s SSE deployment

Provider API keys via env vars:
    OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY, AZURE_API_KEY, ...
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


def _normalize_tool_calls(msg) -> list[dict] | None:
    """
    Return a normalised list of {id, name, arguments(dict)} or None.

    LiteLLM populates msg.tool_calls for most providers. Some Ollama models
    instead embed the tool call as JSON text in msg.content — this fallback
    handles that case so both paths go through the same execution loop.
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

    # Fallback: tool call(s) embedded as JSON in content
    content = (msg.content or "").strip()
    if not (content.startswith("{") or content.startswith("[")):
        return None
    try:
        parsed = json.loads(content)
        items = parsed if isinstance(parsed, list) else [parsed]
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


def _to_litellm_tool(tool, model: str = "") -> dict:
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
    """Convert get_semantic_context JSON into a grounding system prompt."""
    from semantic import build_system_prompt, load_entities
    return build_system_prompt(load_entities(SCHEMAS_DIR))


async def chat_loop(session: ClientSession, model: str) -> None:
    tools_resp = await session.list_tools()
    tools = [_to_litellm_tool(t, model) for t in tools_resp.tools]

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

        # Agentic loop — keep invoking tools until the model gives a final answer
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
                # Don't echo the raw tool-call JSON as if it were a final answer
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
            "LiteLLM model string. Examples:\n"
            "  ollama/llama3.2              (default, local Ollama)\n"
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
