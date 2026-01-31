import polars as pl
import pytest

from config import export as export_module
from config.export import export_to_duckdb
from config import motherduck


@pytest.fixture
def local_db_mode(monkeypatch, tmp_path):
    """Configure DuckDB to use local mode with a temp database."""
    monkeypatch.setenv("DUCKDB_MODE", "local")

    # Create a fresh DuckDB instance for tests
    db = motherduck.DuckDB()
    db.LOCAL_DB_PATH = tmp_path / "test_export.duckdb"

    # Patch the nba_db in the export module (where it's used)
    monkeypatch.setattr(export_module, "nba_db", db)

    yield db

    db.close()


@pytest.fixture
def team_stats_parquet(tmp_path):
    """Create sample team stats parquet file."""
    df = pl.DataFrame({
        "team": ["LAL", "BOS", "GSW"],
        "team_name": ["Lakers", "Celtics", "Warriors"],
        "season": [2024, 2024, 2024],
        "wins": [45, 50, 42],
        "losses": [37, 32, 40],
        "team_pts": [115.2, 118.5, 120.1],
    })
    filepath = tmp_path / "team_season_stats.parquet"
    df.write_parquet(filepath)
    return str(filepath)


@pytest.fixture
def player_stats_parquet(tmp_path):
    """Create sample player stats parquet file."""
    df = pl.DataFrame({
        "player_name": ["LeBron James", "Jayson Tatum", "Stephen Curry"],
        "team": ["LAL", "BOS", "GSW"],
        "season": [2024, 2024, 2024],
        "pts": [25.7, 26.9, 26.4],
        "reb": [7.3, 8.1, 4.5],
        "ast": [8.3, 4.9, 5.1],
    })
    filepath = tmp_path / "player_season_stats.parquet"
    df.write_parquet(filepath)
    return str(filepath)


class TestExportToDuckDB:
    def test_export_team_stats(self, local_db_mode, team_stats_parquet):
        result = export_to_duckdb.fn(team_stats_parquet, "team_season_stats")

        assert result == "team_season_stats"
        assert local_db_mode.table_exists("team_season_stats")
        assert local_db_mode.get_table_row_count("team_season_stats") == 3

    def test_export_player_stats(self, local_db_mode, player_stats_parquet):
        result = export_to_duckdb.fn(player_stats_parquet, "player_season_stats")

        assert result == "player_season_stats"
        assert local_db_mode.table_exists("player_season_stats")
        assert local_db_mode.get_table_row_count("player_season_stats") == 3

    def test_export_preserves_data(self, local_db_mode, team_stats_parquet):
        export_to_duckdb.fn(team_stats_parquet, "team_season_stats")

        # Query the data back
        result = local_db_mode.conn.execute(
            "SELECT team, wins FROM team_season_stats ORDER BY wins DESC"
        ).fetchall()

        assert result[0] == ("BOS", 50)
        assert result[1] == ("LAL", 45)
        assert result[2] == ("GSW", 42)

    def test_export_multiple_tables(self, local_db_mode, team_stats_parquet, player_stats_parquet):
        export_to_duckdb.fn(team_stats_parquet, "team_season_stats")
        export_to_duckdb.fn(player_stats_parquet, "player_season_stats")

        assert local_db_mode.table_exists("team_season_stats")
        assert local_db_mode.table_exists("player_season_stats")
