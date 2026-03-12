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

"""Connector factory: creates the right connector from source config."""

from __future__ import annotations

import os

from assay.core.compiler import SourceConfig


def create_connector(source: SourceConfig):
    """Create a connector based on source type."""
    source_type = source.type.lower()
    connection = _resolve_connection(source.connection)

    if source_type == "duckdb":
        from assay.connectors.duckdb import DuckDBConnector
        database = connection if connection else ":memory:"
        return DuckDBConnector(database=database)

    if source_type in ("postgres", "postgresql"):
        from assay.connectors.postgres import PostgresConnector
        if not connection:
            msg = "PostgreSQL connector requires a connection string"
            raise ValueError(msg)
        return PostgresConnector(connection_string=connection)

    if source_type in ("mysql", "sqlite", "snowflake", "bigquery", "redshift", "databricks"):
        from assay.connectors.postgres import SQLAlchemyConnector
        if not connection:
            msg = f"{source_type} connector requires a connection string"
            raise ValueError(msg)
        return SQLAlchemyConnector(connection_string=connection)

    msg = f"Unknown source type: {source_type}. Supported: duckdb, postgres, mysql, sqlite"
    raise ValueError(msg)


def _resolve_connection(connection: str) -> str:
    """Resolve environment variables in connection strings."""
    if not connection:
        return connection
    if connection.startswith("${") and connection.endswith("}"):
        env_var = connection[2:-1]
        value = os.environ.get(env_var)
        if value is None:
            msg = f"Environment variable {env_var} is not set"
            raise ValueError(msg)
        return value
    return os.path.expandvars(connection)
