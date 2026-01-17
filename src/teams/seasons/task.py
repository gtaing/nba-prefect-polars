from prefect import task

from config import bucket_conf
from config.bucket import nba_bucket
from teams.seasons.processor import TeamSeasonProcessor


@task(log_prints=True)
def get_team_season_stats() -> str:
    raw_game_stats_path = bucket_conf.raw.games_detail
    destination_path = bucket_conf.processed.team_season_stats

    processor = TeamSeasonProcessor()
    season_stats = processor.run(raw_game_stats_path)

    return nba_bucket.sink_parquet(season_stats, destination_path)
