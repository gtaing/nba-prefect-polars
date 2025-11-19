import polars as pl

from config.bucket import nba_bucket


def get_game_id_season() -> pl.LazyFrame:
    """
    Get the game IDs for seasons starting from 2015.
    """
    games_detail = nba_bucket.scan_parquet("raw/games_detail.parquet")
    game_year = pl.col("game_date").str.to_datetime().dt.year()

    return (
        games_detail
        .with_columns(game_year.alias("year"))
        .group_by("season_id", "game_id")
        .agg(pl.max("year").alias("season"))
    )
