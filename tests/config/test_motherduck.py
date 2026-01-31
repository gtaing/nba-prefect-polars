import os
import tempfile
from pathlib import Path

import polars as pl
import pytest

from config.motherduck import DuckDB, DBMode


@pytest.fixture
def local_db(monkeypatch, tmp_path):
    """Create a DuckDB instance in local mode with a temporary database."""
    monkeypatch.setenv("DUCKDB_MODE", "local")

    db = DuckDB()
    # Override the local path to use temp directory
    db.LOCAL_DB_PATH = tmp_path / "test.duckdb"

    yield db

    db.close()


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample parquet file for testing."""
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "score": [85.5, 92.0, 78.5],
    })
    filepath = tmp_path / "sample.parquet"
    df.write_parquet(filepath)
    return str(filepath)


class TestDBMode:
    def test_local_mode_from_env(self, monkeypatch):
        monkeypatch.setenv("DUCKDB_MODE", "local")
        db = DuckDB()
        assert db.mode == DBMode.LOCAL
        db.close()

    def test_production_mode_from_env(self, monkeypatch):
        monkeypatch.setenv("DUCKDB_MODE", "production")
        monkeypatch.setenv("motherduck_token", "test_token")
        db = DuckDB()
        assert db.mode == DBMode.PRODUCTION

    def test_production_mode_is_default(self, monkeypatch):
        monkeypatch.delenv("DUCKDB_MODE", raising=False)
        monkeypatch.setenv("motherduck_token", "test_token")
        db = DuckDB()
        assert db.mode == DBMode.PRODUCTION

    def test_production_mode_requires_token(self, monkeypatch):
        monkeypatch.setenv("DUCKDB_MODE", "production")
        monkeypatch.delenv("motherduck_token", raising=False)
        db = DuckDB()
        with pytest.raises(ValueError, match="motherduck_token"):
            _ = db.conn_str


class TestDuckDBLocal:
    def test_create_table_from_parquet(self, local_db, sample_parquet):
        local_db.create_table_from_file(sample_parquet, "test_table")

        assert local_db.table_exists("test_table")
        assert local_db.get_table_row_count("test_table") == 3

    def test_create_table_replaces_existing(self, local_db, sample_parquet, tmp_path):
        # Create initial table
        local_db.create_table_from_file(sample_parquet, "test_table")
        assert local_db.get_table_row_count("test_table") == 3

        # Create new parquet with different data
        df = pl.DataFrame({"id": [1, 2], "value": [10, 20]})
        new_filepath = tmp_path / "new_sample.parquet"
        df.write_parquet(new_filepath)

        # Replace table
        local_db.create_table_from_file(str(new_filepath), "test_table")
        assert local_db.get_table_row_count("test_table") == 2

    def test_table_exists_false_for_missing(self, local_db):
        assert local_db.table_exists("nonexistent_table") is False

    def test_get_row_count_zero_for_missing(self, local_db):
        assert local_db.get_table_row_count("nonexistent_table") == 0

    def test_connection_reuse(self, local_db, sample_parquet):
        # Access connection twice
        conn1 = local_db.conn
        conn2 = local_db.conn

        # Should be the same connection object
        assert conn1 is conn2

    def test_close_and_reconnect(self, local_db, sample_parquet):
        local_db.create_table_from_file(sample_parquet, "test_table")
        local_db.close()

        # Should be able to reconnect and see the table
        assert local_db.table_exists("test_table")
