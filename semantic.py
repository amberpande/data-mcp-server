"""
Semantic layer — loads entity YAML schemas and provides:
  - lookup_metric()        find canonical SQL formula for a business term
  - build_semantic_context() structured context dict for MCP tool responses
  - build_system_prompt()    rich text prompt injected into Ollama/Claude system message
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Data models (mirrors semantic-layer/backend/core/schema/models.py)
# ---------------------------------------------------------------------------

class JoinCondition(BaseModel):
    source_field: str
    target_field: str


class RelationDef(BaseModel):
    target_entity: str
    rel_type: str
    rel_join_type: str = "left"
    join_on: list[JoinCondition]


class AttributeDef(BaseModel):
    name: str
    source_field: str
    display_name: str | None = None
    description: str | None = None


class MetricDef(BaseModel):
    name: str
    display_name: str | None = None
    description: str | None = None
    aggregation: str  # SUM | COUNT | COUNT_DISTINCT | AVG | MIN | MAX
    source_field: str
    granularity: str
    filter: str | None = None
    default: Any = None

    @property
    def sql_expression(self) -> str:
        col = self.source_field
        if self.aggregation == "COUNT_DISTINCT":
            expr = f"COUNT(DISTINCT {col})"
        elif self.filter:
            if self.aggregation == "COUNT":
                expr = f"COUNT(CASE WHEN {self.filter} THEN 1 END)"
            else:
                expr = f"{self.aggregation}(CASE WHEN {self.filter} THEN {col} END)"
        else:
            expr = f"{self.aggregation}({col})"
        return expr


class EntityDef(BaseModel):
    name: str
    display_name: str | None = None
    description: str | None = None
    source: str
    keys: list[str]
    relations: list[RelationDef] = Field(default_factory=list)
    attributes: list[AttributeDef] = Field(default_factory=list)
    metrics: list[MetricDef] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_entities(schemas_dir: Path) -> dict[str, EntityDef]:
    entities: dict[str, EntityDef] = {}
    for path in sorted(schemas_dir.rglob("*.yaml")):
        raw = yaml.safe_load(path.read_text())
        if isinstance(raw, dict) and raw.get("type") == "entity":
            entity = EntityDef.model_validate(raw)
            entities[entity.name] = entity
    return entities


# ---------------------------------------------------------------------------
# Metric lookup — exact name match, then fuzzy display-name match
# ---------------------------------------------------------------------------

def lookup_metric(entities: dict[str, EntityDef], name: str) -> dict | None:
    name_lower = name.lower().replace(" ", "_")
    for entity in entities.values():
        for m in entity.metrics:
            if m.name == name_lower or (m.display_name or "").lower().replace(" ", "_") == name_lower:
                return {
                    "metric": m.name,
                    "display_name": m.display_name,
                    "entity": entity.name,
                    "table": entity.source,
                    "sql": m.sql_expression,
                    "description": m.description,
                }
    return None


def all_metrics(entities: dict[str, EntityDef]) -> list[dict]:
    return [
        {
            "metric": m.name,
            "display_name": m.display_name,
            "entity": entity_name,
            "table": entity.source,
            "sql": m.sql_expression,
        }
        for entity_name, entity in entities.items()
        for m in entity.metrics
    ]


# ---------------------------------------------------------------------------
# Structured context — returned as JSON by get_semantic_context tool
# ---------------------------------------------------------------------------

def build_semantic_context(entities: dict[str, EntityDef]) -> dict:
    return {
        "entities": [
            {
                "name": e.name,
                "display_name": e.display_name,
                "description": e.description,
                "source_table": e.source,
                "primary_keys": e.keys,
                "attributes": [
                    {"name": a.name, "column": a.source_field, "display_name": a.display_name}
                    for a in e.attributes
                ],
                "metrics": [
                    {
                        "name": m.name,
                        "display_name": m.display_name,
                        "sql": m.sql_expression,
                        "description": m.description,
                    }
                    for m in e.metrics
                ],
                "joins": [
                    {
                        "to": r.target_entity,
                        "type": r.rel_type,
                        "on": [
                            f"{e.name}.{jc.source_field} = {r.target_entity}.{jc.target_field}"
                            for jc in r.join_on
                        ],
                    }
                    for r in e.relations
                ],
            }
            for e in entities.values()
        ]
    }


# ---------------------------------------------------------------------------
# System prompt — injected into LLM at chat start
# ---------------------------------------------------------------------------

_SNOWFLAKE_OPTIMISATION_RULES = """
SNOWFLAKE SQL OPTIMISATION RULES (apply every time you write SQL):

Aggregation & filtering
  - Use APPROX_COUNT_DISTINCT(col) instead of COUNT(DISTINCT col) for columns with
    high cardinality (>1 M distinct values) unless exact precision is required.
  - Use IFF(cond, true_val, false_val) instead of CASE WHEN for binary conditions.
  - Use ZEROIFNULL(expr) instead of COALESCE(expr, 0) for numeric nulls.
  - Filter as early as possible — push WHERE clauses into CTEs, not outer queries.
  - Never use HAVING to filter on a non-aggregate; move it to WHERE.

Joins
  - Always put the larger table on the left side of a JOIN (Snowflake uses the left
    table as the build side for hash joins).
  - Avoid cross joins; always include an ON clause.
  - When filtering a joined table, put the filter inside a CTE or subquery rather
    than in the outer WHERE — this reduces the rows fed into the join.

Window functions
  - Use QUALIFY to filter window-function results instead of wrapping in a subquery:
      SELECT ..., ROW_NUMBER() OVER (...) AS rn FROM t QUALIFY rn = 1
  - Prefer RANK() / DENSE_RANK() over ROW_NUMBER() when ties are meaningful.

Date & time
  - Use DATE_TRUNC('month', col) for period grouping.
  - Use DATEADD(day, n, col) / DATEDIFF(day, a, b) for date arithmetic.
  - Cast date strings with TO_DATE('2024-01-01') not CAST('...' AS DATE).

Query structure
  - Always name every column explicitly — never use SELECT *.
  - Use CTEs (WITH … AS) for multi-step logic; do not nest more than 2 subquery levels.
  - Place LIMIT at the outermost query only; never inside a CTE.
  - Include ORDER BY only when the order of results is required by the question.
  - Use column aliases in GROUP BY when grouping on expressions:
      SELECT DATE_TRUNC('month', o_orderdate) AS month, COUNT(*) FROM orders GROUP BY month

Result formatting
  - Round monetary values to 2 decimal places: ROUND(SUM(col), 2).
  - Format percentages as: ROUND(100.0 * numerator / NULLIF(denominator, 0), 2).
  - Use NULLIF(denominator, 0) in all divisions to avoid divide-by-zero errors.
"""


def build_system_prompt(entities: dict[str, EntityDef]) -> str:
    lines = [
        "You are a precise data analyst. You have access to MCP tools that query "
        "CSV datasets and a Snowflake data warehouse.\n",
        "WORKFLOW RULES:",
        "1. Always call lookup_metric before writing SQL for any business term.",
        "2. Always call validate_sql before snowflake_query to catch schema/syntax errors.",
        "3. Use the exact metric formulas defined in DATA MODEL — never invent aggregations.",
        "4. Use the exact join conditions defined in DATA MODEL — never guess foreign keys.",
        _SNOWFLAKE_OPTIMISATION_RULES,
        "DATA MODEL:",
    ]

    for e in entities.values():
        lines.append(f"\n[{e.name.upper()}] → {e.source}")
        if e.description:
            lines.append(f"  {e.description}")

        lines.append("  Columns: " + ", ".join(e.keys + [a.source_field for a in e.attributes]))

        if e.metrics:
            lines.append("  Metrics (canonical formulas — use exactly):")
            for m in e.metrics:
                desc = f"  # {m.description}" if m.description else ""
                lines.append(f"    {m.name} = {m.sql_expression}{desc}")

        if e.relations:
            lines.append("  Joins:")
            for r in e.relations:
                for jc in r.join_on:
                    lines.append(
                        f"    {r.rel_join_type.upper()} JOIN {r.target_entity} "
                        f"ON {e.name}.{jc.source_field} = {r.target_entity}.{jc.target_field}"
                    )

    return "\n".join(lines)
