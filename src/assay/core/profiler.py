# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Data profiler: generates statistical profiles and suggests checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from assay.connectors.base import Connection


@dataclass
class ColumnProfile:
    """Statistical profile of a single column."""

    name: str
    dtype: str
    total_count: int = 0
    null_count: int = 0
    null_pct: float = 0.0
    distinct_count: int = 0
    distinct_pct: float = 0.0

    # Numeric stats
    min_value: Any = None
    max_value: Any = None
    mean_value: float | None = None
    median_value: float | None = None
    stddev_value: float | None = None

    # String stats
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None

    # Top values
    top_values: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class TableProfile:
    """Statistical profile of a table."""

    table: str
    row_count: int
    column_count: int
    columns: list[ColumnProfile] = field(default_factory=list)


def profile_table(
    connection: Connection,
    table: str,
    sample_size: int | None = None,
) -> TableProfile:
    """Generate a statistical profile of a table."""
    # Get row count
    result = connection.execute(f"SELECT COUNT(*) as cnt FROM {table}")
    row_count = result[0]["cnt"]

    # Get column info
    col_info = connection.get_columns(table)

    source_expr = table
    if sample_size and row_count > sample_size:
        source_expr = f"(SELECT * FROM {table} USING SAMPLE {sample_size})"

    columns: list[ColumnProfile] = []

    for col in col_info:
        col_name = col["name"]
        col_type = col["type"].lower()

        # Basic stats: nulls and distinct count
        basic = connection.execute(
            f"SELECT "
            f"COUNT(*) as total, "
            f"COUNT(*) - COUNT({col_name}) as null_count, "
            f"COUNT(DISTINCT {col_name}) as distinct_count "
            f"FROM {source_expr}"
        )[0]

        total = basic["total"]
        null_count = basic["null_count"]
        distinct_count = basic["distinct_count"]

        profile = ColumnProfile(
            name=col_name,
            dtype=col_type,
            total_count=total,
            null_count=null_count,
            null_pct=round(null_count / total * 100, 2) if total > 0 else 0,
            distinct_count=distinct_count,
            distinct_pct=round(distinct_count / total * 100, 2) if total > 0 else 0,
        )

        # Numeric stats
        is_numeric = any(t in col_type for t in [
            "int", "float", "double", "decimal", "numeric", "real", "bigint", "smallint",
            "number", "money",
        ])
        if is_numeric:
            num_stats = connection.execute(
                f"SELECT "
                f"MIN({col_name}) as min_val, "
                f"MAX({col_name}) as max_val, "
                f"AVG({col_name}::DOUBLE) as mean_val, "
                f"MEDIAN({col_name}::DOUBLE) as median_val, "
                f"STDDEV({col_name}::DOUBLE) as stddev_val "
                f"FROM {source_expr} WHERE {col_name} IS NOT NULL"
            )[0]
            profile.min_value = num_stats["min_val"]
            profile.max_value = num_stats["max_val"]
            profile.mean_value = round(float(num_stats["mean_val"]), 4) if num_stats["mean_val"] else None
            profile.median_value = round(float(num_stats["median_val"]), 4) if num_stats["median_val"] else None
            profile.stddev_value = round(float(num_stats["stddev_val"]), 4) if num_stats["stddev_val"] else None

        # String stats
        is_string = any(t in col_type for t in ["varchar", "text", "char", "string"])
        if is_string:
            str_stats = connection.execute(
                f"SELECT "
                f"MIN(LENGTH({col_name})) as min_len, "
                f"MAX(LENGTH({col_name})) as max_len, "
                f"AVG(LENGTH({col_name})) as avg_len "
                f"FROM {source_expr} WHERE {col_name} IS NOT NULL"
            )[0]
            profile.min_length = str_stats["min_len"]
            profile.max_length = str_stats["max_len"]
            profile.avg_length = round(float(str_stats["avg_len"]), 1) if str_stats["avg_len"] else None

        # Top values (for columns with reasonable cardinality)
        if distinct_count <= 50:
            top = connection.execute(
                f"SELECT {col_name} as value, COUNT(*) as count "
                f"FROM {source_expr} WHERE {col_name} IS NOT NULL "
                f"GROUP BY {col_name} ORDER BY count DESC LIMIT 10"
            )
            profile.top_values = [{"value": r["value"], "count": r["count"]} for r in top]

        columns.append(profile)

    return TableProfile(
        table=table,
        row_count=row_count,
        column_count=len(columns),
        columns=columns,
    )


def suggest_checks(profile: TableProfile) -> list[dict[str, Any]]:
    """Suggest quality checks based on a table profile."""
    checks: list[dict[str, Any]] = []

    # Always suggest row_count
    checks.append({"row_count": {"min": max(1, profile.row_count // 2)}})

    not_null_cols = []
    unique_cols = []

    for col in profile.columns:
        # Suggest not_null for columns with 0% nulls
        if col.null_pct == 0:
            not_null_cols.append(col.name)

        # Suggest unique for columns that are 100% distinct
        if col.distinct_pct == 100 and col.total_count > 1:
            unique_cols.append(col.name)

        # Suggest accepted_values for low-cardinality columns
        if 0 < col.distinct_count <= 20 and col.top_values:
            values = [str(v["value"]) for v in col.top_values]
            checks.append({
                "accepted_values": {
                    "column": col.name,
                    "values": values,
                }
            })

        # Suggest range for numeric columns
        if col.min_value is not None and col.max_value is not None:
            # Add 10% margin
            try:
                min_val = float(col.min_value)
                max_val = float(col.max_value)
            except (TypeError, ValueError):
                min_val = None
                max_val = None
            if min_val is not None and max_val is not None:
                margin = (max_val - min_val) * 0.1 if max_val != min_val else abs(min_val) * 0.1
                checks.append({
                    "range": {
                        "column": col.name,
                        "min": round(min_val - margin, 2),
                        "max": round(max_val + margin, 2),
                    }
                })

    if not_null_cols:
        checks.insert(0, {"not_null": not_null_cols})

    for col in unique_cols:
        checks.insert(1, {"unique": col})

    return checks


def checks_to_yaml(checks: list[dict[str, Any]], source_type: str, table: str) -> str:
    """Convert suggested checks to assay.yaml format."""
    import yaml

    config = {
        "source": {"type": source_type, "table": table},
        "checks": checks,
    }
    return yaml.dump(config, default_flow_style=False, sort_keys=False)
