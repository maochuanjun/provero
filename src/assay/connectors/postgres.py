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

"""PostgreSQL connector via SQLAlchemy."""

from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class SQLAlchemyConnection:
    """SQLAlchemy-based connection wrapper."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._conn = engine.connect()

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        result = self._conn.execute(text(query), params or {})
        columns = list(result.keys())
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def get_columns(self, table: str) -> list[dict[str, Any]]:
        result = self._conn.execute(text(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_name = :table "
            "ORDER BY ordinal_position"
        ), {"table": table})
        return [
            {"name": row[0], "type": row[1], "nullable": row[2] == "YES"}
            for row in result.fetchall()
        ]

    def close(self) -> None:
        self._conn.close()
        self._engine.dispose()


class PostgresConnector:
    """Connector for PostgreSQL databases."""

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def connect(self) -> SQLAlchemyConnection:
        engine = create_engine(self.connection_string)
        return SQLAlchemyConnection(engine)

    def disconnect(self, connection: SQLAlchemyConnection) -> None:
        connection.close()


class SQLAlchemyConnector:
    """Generic connector for any SQLAlchemy-supported database."""

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def connect(self) -> SQLAlchemyConnection:
        engine = create_engine(self.connection_string)
        return SQLAlchemyConnection(engine)

    def disconnect(self, connection: SQLAlchemyConnection) -> None:
        connection.close()
