from prefect import flow

from players.seasons.task import get_player_season_stats
from teams.season_stats import get_team_season_stats
from config.export import export_to_duckdb


@flow(log_prints=True)
def season_stats():
    # Extract and transform stats to parquet
    player_stats_fpath = get_player_season_stats()
    team_stats_fpath = get_team_season_stats()

    print(f"Player stats: {player_stats_fpath}")
    print(f"Team stats: {team_stats_fpath}")

    # Export to DuckDB
    export_to_duckdb(player_stats_fpath, "player_season_stats")
    export_to_duckdb(team_stats_fpath, "team_season_stats")


if __name__ == "__main__":
    season_stats()