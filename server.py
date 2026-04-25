"""
Data Engineering MCP Server
============================
Teaches MCP concepts through three core primitives:
  - Resources : expose data Claude can *read*
  - Tools     : functions Claude can *call*
  - Prompts   : reusable prompt templates Claude can *invoke*
"""

import json
import os
import duckdb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.server.stdio
import mcp.types as types

load_dotenv()

from semantic import load_entities, lookup_metric, all_metrics, build_semantic_context, build_system_prompt  # noqa: E402

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"
SCHEMAS_DIR = Path(__file__).parent / "schemas"
DATASETS: dict[str, pd.DataFrame] = {}   # in-memory cache keyed by name
ENTITIES = load_entities(SCHEMAS_DIR)     # semantic layer — loaded once at startup

app = Server("data-engineering-mcp")

# ---------------------------------------------------------------------------
# Snowflake helpers — lazy connection, opened on first use
# ---------------------------------------------------------------------------

_SF_CONN = None


def _get_sf_conn():
    global _SF_CONN
    if _SF_CONN is None:
        import snowflake.connector
        _SF_CONN = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
            role=os.environ.get("SNOWFLAKE_ROLE"),
        )
    return _SF_CONN


def _run_sf_query(sql: str) -> list[dict]:
    import snowflake.connector
    conn = _get_sf_conn()
    cur = conn.cursor(snowflake.connector.DictCursor)
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()

# ---------------------------------------------------------------------------
# CONCEPT 1 — RESOURCES
# Resources are *data* Claude can read passively (like GET endpoints).
# You define them at the list_resources / read_resource hooks.
# ---------------------------------------------------------------------------

@app.list_resources()
async def list_resources() -> list[types.Resource]:
    """Advertise which resources exist so Claude knows what to ask for."""
    resources = []
    for csv_file in DATA_DIR.glob("*.csv"):
        name = csv_file.stem                        # "sales", "employees", …
        resources.append(types.Resource(
            uri=f"data://{name}",                   # any URI scheme you choose
            name=f"{name.capitalize()} Dataset",
            description=f"CSV dataset: {csv_file.name}",
            mimeType="text/csv",
        ))
    return resources


@app.read_resource()
async def read_resource(uri: types.AnyUrl) -> str:
    """Return raw CSV content when Claude requests a specific resource."""
    # uri looks like  data://sales
    name = str(uri).removeprefix("data://")
    csv_path = DATA_DIR / f"{name}.csv"
    if not csv_path.exists():
        raise ValueError(f"Dataset '{name}' not found")
    return csv_path.read_text()


# ---------------------------------------------------------------------------
# CONCEPT 2 — TOOLS
# Tools are *actions* Claude can invoke (like POST endpoints).
# You define them at list_tools / call_tool hooks.
# ---------------------------------------------------------------------------

@app.list_tools()
async def list_tools() -> list[types.Tool]:
    """Declare every tool with its name, description, and JSON-schema input."""
    return [
        types.Tool(
            name="load_dataset",
            description="Load a CSV file into memory and return a preview.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Dataset name (e.g. 'sales' or 'employees')"},
                    "rows": {"type": "integer", "description": "Number of preview rows (default 5)", "default": 5},
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="get_schema",
            description="Return column names, data types, and null counts for a loaded dataset.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Dataset name"},
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="get_statistics",
            description="Return descriptive statistics (min, max, mean, std, quartiles) for numeric columns.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Dataset name"},
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="run_sql",
            description=(
                "Run an arbitrary SQL query against a loaded dataset using DuckDB. "
                "The table name inside SQL must match the dataset name (e.g. SELECT * FROM sales LIMIT 3)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Dataset name to query"},
                    "query": {"type": "string", "description": "SQL query string"},
                },
                "required": ["name", "query"],
            },
        ),
        types.Tool(
            name="list_loaded_datasets",
            description="List all datasets currently loaded in memory.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="snowflake_query",
            description="Run a SQL query against Snowflake and return results.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL query to execute"},
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="snowflake_list_tables",
            description="List all tables in the connected Snowflake database and schema.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="snowflake_describe_table",
            description="Return column names and types for a Snowflake table.",
            inputSchema={
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "Table name (optionally schema-qualified)"},
                },
                "required": ["table"],
            },
        ),
        types.Tool(
            name="get_semantic_context",
            description=(
                "Return the full data model: all tables, columns, canonical metric formulas, "
                "and join relationships. Call this at the start of any Snowflake analysis session."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="lookup_metric",
            description=(
                "Look up the canonical SQL formula for a business metric by name "
                "(e.g. 'revenue', 'order_count', 'avg_discount'). "
                "Always call this before writing SQL that involves aggregations."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Metric name or business term"},
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="validate_sql",
            description=(
                "Validate a SQL query against Snowflake using EXPLAIN (zero data scanned). "
                "Always call this before snowflake_query to catch schema or syntax errors early."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL query to validate"},
                },
                "required": ["query"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    """Route incoming tool calls and return results as TextContent."""

    # --- load_dataset -------------------------------------------------------
    if name == "load_dataset":
        dataset_name = arguments["name"]
        rows = arguments.get("rows", 5)
        csv_path = DATA_DIR / f"{dataset_name}.csv"
        if not csv_path.exists():
            return [types.TextContent(type="text", text=f"Error: '{dataset_name}.csv' not found in {DATA_DIR}")]

        df = pd.read_csv(csv_path)
        DATASETS[dataset_name] = df                     # cache for later tools

        result = {
            "dataset": dataset_name,
            "rows_total": len(df),
            "columns": list(df.columns),
            "preview": df.head(rows).to_dict(orient="records"),
        }
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

    # --- get_schema ---------------------------------------------------------
    elif name == "get_schema":
        dataset_name = arguments["name"]
        df = _require_loaded(dataset_name)
        schema = {
            col: {
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isna().sum()),
                "sample": str(df[col].iloc[0]) if len(df) > 0 else None,
            }
            for col in df.columns
        }
        return [types.TextContent(type="text", text=json.dumps(schema, indent=2))]

    # --- get_statistics -----------------------------------------------------
    elif name == "get_statistics":
        dataset_name = arguments["name"]
        df = _require_loaded(dataset_name)
        stats = df.describe(include="all").fillna("").to_dict()
        # make values JSON-serialisable
        clean = {col: {k: str(v) for k, v in inner.items()} for col, inner in stats.items()}
        return [types.TextContent(type="text", text=json.dumps(clean, indent=2))]

    # --- run_sql ------------------------------------------------------------
    elif name == "run_sql":
        dataset_name = arguments["name"]
        query = arguments["query"]
        df = _require_loaded(dataset_name)

        # DuckDB can query pandas DataFrames directly — very common in data eng
        con = duckdb.connect()
        con.register(dataset_name, df)
        result_df = con.execute(query).df()
        con.close()

        result = {
            "query": query,
            "rows_returned": len(result_df),
            "data": result_df.to_dict(orient="records"),
        }
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

    # --- list_loaded_datasets -----------------------------------------------
    elif name == "list_loaded_datasets":
        if not DATASETS:
            return [types.TextContent(type="text", text="No datasets loaded yet. Use load_dataset first.")]
        info = {n: {"rows": len(df), "columns": list(df.columns)} for n, df in DATASETS.items()}
        return [types.TextContent(type="text", text=json.dumps(info, indent=2))]

    # --- snowflake_query ----------------------------------------------------
    elif name == "snowflake_query":
        rows = _run_sf_query(arguments["query"])
        result = {"rows_returned": len(rows), "data": rows}
        return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=str))]

    # --- snowflake_list_tables ----------------------------------------------
    elif name == "snowflake_list_tables":
        rows = _run_sf_query("SHOW TABLES")
        tables = [r["name"] for r in rows]
        return [types.TextContent(type="text", text=json.dumps(tables, indent=2))]

    # --- snowflake_describe_table -------------------------------------------
    elif name == "snowflake_describe_table":
        table = arguments["table"]
        rows = _run_sf_query(f"DESCRIBE TABLE {table}")
        return [types.TextContent(type="text", text=json.dumps(rows, indent=2, default=str))]

    # --- get_semantic_context -----------------------------------------------
    elif name == "get_semantic_context":
        context = build_semantic_context(ENTITIES)
        return [types.TextContent(type="text", text=json.dumps(context, indent=2))]

    # --- lookup_metric -------------------------------------------------------
    elif name == "lookup_metric":
        metric_name = arguments["name"]
        result = lookup_metric(ENTITIES, metric_name)
        if result is None:
            return [types.TextContent(type="text", text=json.dumps({
                "error": f"Metric '{metric_name}' not found in semantic layer.",
                "available_metrics": all_metrics(ENTITIES),
            }, indent=2))]
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

    # --- validate_sql --------------------------------------------------------
    elif name == "validate_sql":
        query = arguments["query"]
        try:
            _run_sf_query(f"EXPLAIN USING TABULAR {query}")
            return [types.TextContent(type="text", text=json.dumps({"valid": True, "message": "SQL is valid."}))]
        except Exception as e:
            return [types.TextContent(type="text", text=json.dumps({"valid": False, "error": str(e)}))]

    else:
        return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


# ---------------------------------------------------------------------------
# CONCEPT 3 — PROMPTS
# Prompts are reusable templates that help users start complex workflows.
# ---------------------------------------------------------------------------

@app.list_prompts()
async def list_prompts() -> list[types.Prompt]:
    return [
        types.Prompt(
            name="analyze_dataset",
            description="Walk through a full exploratory analysis of a dataset.",
            arguments=[
                types.PromptArgument(name="dataset_name", description="Name of the dataset to analyse", required=True),
            ],
        ),
        types.Prompt(
            name="compare_datasets",
            description="Compare two datasets and highlight key differences.",
            arguments=[
                types.PromptArgument(name="dataset_a", description="First dataset name", required=True),
                types.PromptArgument(name="dataset_b", description="Second dataset name", required=True),
            ],
        ),
    ]


@app.get_prompt()
async def get_prompt(name: str, arguments: dict | None) -> types.GetPromptResult:
    args = arguments or {}

    if name == "analyze_dataset":
        dataset = args.get("dataset_name", "<dataset>")
        return types.GetPromptResult(
            description=f"Full EDA for {dataset}",
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.TextContent(
                        type="text",
                        text=(
                            f"Please perform a full exploratory analysis of the '{dataset}' dataset.\n\n"
                            "Steps:\n"
                            f"1. Load it with load_dataset(name='{dataset}')\n"
                            f"2. Inspect the schema with get_schema(name='{dataset}')\n"
                            f"3. Get statistics with get_statistics(name='{dataset}')\n"
                            "4. Write and run 2-3 insightful SQL queries to surface trends\n"
                            "5. Summarise your findings in plain language"
                        ),
                    ),
                )
            ],
        )

    elif name == "compare_datasets":
        a = args.get("dataset_a", "<dataset_a>")
        b = args.get("dataset_b", "<dataset_b>")
        return types.GetPromptResult(
            description=f"Comparison of {a} vs {b}",
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.TextContent(
                        type="text",
                        text=(
                            f"Compare the '{a}' and '{b}' datasets.\n\n"
                            f"1. Load both datasets\n"
                            "2. Compare their schemas\n"
                            "3. Highlight structural and statistical differences\n"
                            "4. Suggest how they could be joined or related"
                        ),
                    ),
                )
            ],
        )

    raise ValueError(f"Unknown prompt: {name}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_loaded(name: str) -> pd.DataFrame:
    """Raise a clear error if the dataset hasn't been loaded yet."""
    if name not in DATASETS:
        raise ValueError(f"Dataset '{name}' is not loaded. Call load_dataset(name='{name}') first.")
    return DATASETS[name]


# ---------------------------------------------------------------------------
# Entry point — stdio locally, SSE in Kubernetes (set MCP_TRANSPORT=sse)
# ---------------------------------------------------------------------------

def _init_options() -> InitializationOptions:
    return InitializationOptions(
        server_name="data-engineering-mcp",
        server_version="0.1.0",
        capabilities=app.get_capabilities(
            notification_options=NotificationOptions(),
            experimental_capabilities={},
        ),
    )


async def main():
    transport = os.environ.get("MCP_TRANSPORT", "stdio")

    if transport == "sse":
        from mcp.server.sse import SseServerTransport
        from starlette.responses import PlainTextResponse
        import uvicorn

        sse = SseServerTransport("/messages/")

        async def asgi_app(scope, receive, send):
            if scope["type"] == "lifespan":
                await receive()
                await send({"type": "lifespan.startup.complete"})
                await receive()
                await send({"type": "lifespan.shutdown.complete"})
            elif scope["type"] == "http":
                path = scope.get("path", "")
                if path == "/sse":
                    async with sse.connect_sse(scope, receive, send) as streams:
                        await app.run(streams[0], streams[1], _init_options())
                elif path.startswith("/messages"):
                    await sse.handle_post_message(scope, receive, send)
                else:
                    resp = PlainTextResponse("Not found", status_code=404)
                    await resp(scope, receive, send)

        port = int(os.environ.get("PORT", 8000))
        config = uvicorn.Config(asgi_app, host="0.0.0.0", port=port)
        await uvicorn.Server(config).serve()
    else:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await app.run(read_stream, write_stream, _init_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
