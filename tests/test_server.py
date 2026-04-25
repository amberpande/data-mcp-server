import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from server import call_tool  # noqa: E402

# ── CSV / DuckDB tools ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_load_dataset_returns_preview():
    result = await call_tool("load_dataset", {"name": "sales"})
    data = json.loads(result[0].text)
    assert data["dataset"] == "sales"
    assert "preview" in data


@pytest.mark.asyncio
async def test_get_schema_after_load():
    await call_tool("load_dataset", {"name": "sales"})
    result = await call_tool("get_schema", {"name": "sales"})
    schema = json.loads(result[0].text)
    assert "region" in schema


@pytest.mark.asyncio
async def test_run_sql_count():
    await call_tool("load_dataset", {"name": "sales"})
    result = await call_tool("run_sql", {"name": "sales", "query": "SELECT COUNT(*) AS n FROM sales"})
    data = json.loads(result[0].text)
    assert data["rows_returned"] == 1


@pytest.mark.asyncio
async def test_list_loaded_datasets():
    await call_tool("load_dataset", {"name": "sales"})
    result = await call_tool("list_loaded_datasets", {})
    data = json.loads(result[0].text)
    assert "sales" in data


@pytest.mark.asyncio
async def test_get_schema_unloaded_raises():
    with pytest.raises(ValueError, match="not loaded"):
        await call_tool("get_schema", {"name": "__nonexistent__"})


# ── Snowflake tools (mocked) ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_snowflake_list_tables(mock_sf):
    result = await call_tool("snowflake_list_tables", {})
    tables = json.loads(result[0].text)
    assert tables == ["ORDERS", "CUSTOMERS"]


@pytest.mark.asyncio
async def test_snowflake_query(mock_sf):
    result = await call_tool("snowflake_query", {"query": "SELECT 1"})
    data = json.loads(result[0].text)
    assert data["rows_returned"] == 1
    assert data["data"] == [{"col": "val"}]


@pytest.mark.asyncio
async def test_snowflake_describe_table(mock_sf):
    result = await call_tool("snowflake_describe_table", {"table": "ORDERS"})
    rows = json.loads(result[0].text)
    assert isinstance(rows, list)


@pytest.fixture
def mock_sf(monkeypatch):
    import server

    def fake_run(sql):
        if sql == "SHOW TABLES":
            return [{"name": "ORDERS"}, {"name": "CUSTOMERS"}]
        return [{"col": "val"}]

    monkeypatch.setattr(server, "_run_sf_query", fake_run)
    monkeypatch.setattr(server, "_SF_CONN", None)
