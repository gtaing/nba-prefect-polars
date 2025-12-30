from prefect import task

from config.bucket import nba_bucket
from config import bucket_conf
from games.scope import get_game_id_season
from players import PLAYERS_METRICS
from players.seasons.processor import PlayerSeasonProcessor


@task(log_prints=True)
def get_player_season_stats():
    # Relevant paths
    game_stats_path = bucket_conf.raw.player_stats
    destination_path = bucket_conf.processed.player_season_stats

    # Input stats and relevant scope for NBA games
    game_stats = nba_bucket.scan_parquet(filepath=game_stats_path)
    scope_game_ids = get_game_id_season()

    processor = PlayerSeasonProcessor(metrics=PLAYERS_METRICS)

    season_stats = processor.run(game_stats, scope_game_ids)

    return nba_bucket.sink_parquet(season_stats, destination_path)
