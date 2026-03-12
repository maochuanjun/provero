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

"""Tests for the connector factory."""

from __future__ import annotations

import os

import pytest

from assay.connectors.duckdb import DuckDBConnector
from assay.connectors.factory import create_connector
from assay.core.compiler import SourceConfig


class TestCreateConnector:
    def test_duckdb(self):
        source = SourceConfig(type="duckdb")
        connector = create_connector(source)
        assert isinstance(connector, DuckDBConnector)

    def test_duckdb_with_database(self):
        source = SourceConfig(type="duckdb", connection=":memory:")
        connector = create_connector(source)
        assert isinstance(connector, DuckDBConnector)

    def test_unknown_type_raises(self):
        source = SourceConfig(type="unknown_db")
        with pytest.raises(ValueError, match="Unknown source type"):
            create_connector(source)

    def test_postgres_without_connection_raises(self):
        source = SourceConfig(type="postgres", connection="")
        with pytest.raises(ValueError, match="requires a connection string"):
            create_connector(source)

    def test_env_var_resolution(self):
        os.environ["TEST_ASSAY_DB"] = "postgresql://localhost/test"
        try:
            source = SourceConfig(type="postgres", connection="${TEST_ASSAY_DB}")
            # This will create a PostgresConnector (won't actually connect)
            from assay.connectors.postgres import PostgresConnector
            connector = create_connector(source)
            assert isinstance(connector, PostgresConnector)
        finally:
            del os.environ["TEST_ASSAY_DB"]

    def test_missing_env_var_raises(self):
        source = SourceConfig(type="postgres", connection="${NONEXISTENT_VAR_ASSAY}")
        with pytest.raises(ValueError, match="not set"):
            create_connector(source)
