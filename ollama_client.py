"""
Ollama ↔ MCP bridge — chat with your data using a local LLM.

Usage:
    python ollama_client.py                          # stdio, llama3.2
    python ollama_client.py --model mistral          # different model
    python ollama_client.py --sse http://host/sse    # k8s SSE deployment
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import ollama
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

SERVER_SCRIPT = Path(__file__).parent / "server.py"
SCHEMAS_DIR = Path(__file__).parent / "schemas"


def _to_ollama_tool(tool) -> dict:
    return {
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description or "",
            "parameters": tool.inputSchema,
        },
    }


def _build_system_prompt_from_context(ctx: dict) -> str:
    """Convert get_semantic_context JSON into a grounding system prompt."""
    from semantic import EntityDef, build_system_prompt
    entities = {}
    for e in ctx.get("entities", []):
        entities[e["name"]] = EntityDef(
            name=e["name"],
            display_name=e.get("display_name"),
            description=e.get("description"),
            source=e["source_table"],
            keys=e["primary_keys"],
        )
    # Use semantic.py's build_system_prompt directly from YAML files (simpler)
    from semantic import load_entities
    return build_system_prompt(load_entities(SCHEMAS_DIR))


async def chat_loop(session: ClientSession, model: str) -> None:
    tools_resp = await session.list_tools()
    tools = [_to_ollama_tool(t) for t in tools_resp.tools]

    # Bootstrap rich system prompt from the semantic layer
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
            response = ollama.chat(model=model, messages=history, tools=tools)
            msg = response.message

            history.append(msg)  # ollama SDK accepts Message objects in history

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
    parser = argparse.ArgumentParser(description="Chat with your data via Ollama + MCP")
    parser.add_argument(
        "--model",
        default=os.environ.get("OLLAMA_MODEL", "llama3.2"),
        help="Ollama model name (default: llama3.2)",
    )
    parser.add_argument(
        "--sse",
        metavar="URL",
        help="SSE endpoint URL for k8s deployment (e.g. http://localhost:8000/sse)",
    )
    args = parser.parse_args()

    asyncio.run(main(args.model, args.sse))
