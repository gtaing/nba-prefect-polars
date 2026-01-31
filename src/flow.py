import os

from prefect import flow

from players.seasons.task import get_player_season_stats
from teams.season_stats import get_team_season_stats


@flow(log_prints=True)
def season_stats():
    player_stats_fpath = get_player_season_stats()
    team_stats_fpath = get_team_season_stats()

    print(player_stats_fpath)
    print(team_stats_fpath)


if __name__ == "__main__":

    flow.from_source(
        source="https://github.com/gtaing/nba-prefect-polars.git",
        entrypoint="src/flow.py:season_stats"
    ).deploy(
        name="nba-season-stats-deployment",
        work_pool_name="my-managed-pool",
    )
