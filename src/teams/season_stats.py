import polars as pl


from polars import LazyFrame
from prefect import task
from config.bucket import nba_bucket
from config import bucket_conf
from teams import TEAM_CONFIG_MAP, TEAM_METRICS


def get_transformed_games(games_detail: LazyFrame, conf_type: str) -> LazyFrame:
    """
    Transform the games detail LazyFrame based on the configuration type.
    """
    try:
        applied_config = TEAM_CONFIG_MAP[conf_type]
    except KeyError:
        raise Exception("Choose a configuration between 'home' and 'away'")

    renaming_common = [
        pl.col(col_name).alias(col_alias)
        for col_name, col_alias in applied_config["renaming"].items()
    ]

    team_metrics = [
        pl.col(metric_name).cast(pl.Float32).alias(metric_alias)
        for metric_name, metric_alias in applied_config["team_metrics"].items()
    ]

    opponent_metrics = [
        pl.col(metric_name).cast(pl.Float32).alias(metric_alias)
        for metric_name, metric_alias in applied_config["opponent_metrics"].items()
    ]

    return games_detail.filter(pl.col("season_type") != "Pre Season").select(
        "game_id",
        "season_id",
        "season_type",
        pl.col("game_date").str.to_datetime(),
        pl.lit(f"{conf_type}").alias("game_location"),
        *renaming_common,
        *team_metrics,
        *opponent_metrics,
    )


def create_full_games(
    home_games: LazyFrame, away_games: LazyFrame, games_detail: LazyFrame
) -> LazyFrame:
    """
    Combine home and away games into a full games LazyFrame with season information.
    """

    game_season = (
        games_detail.with_columns(
            pl.col("game_date").str.to_datetime().dt.year().alias("year")
        )
        .group_by("season_id")
        .agg(pl.max("year").alias("season"))
    )

    return (
        pl.concat(items=[home_games, away_games], how="align")
        .join(game_season, how="left", on="season_id")
        .filter(pl.col("season") >= 2015)
    )


def compute_team_season_stats(full_games: LazyFrame) -> LazyFrame:
    """
    Compute the season statistics for teams based on game data.
    """
    team_stats_dimensions = ["season_id", "team", "team_name", "season"]

    return (
        full_games.with_columns(
            pl.when(pl.col("win_loss") == "W").then(1).otherwise(0).alias("wins"),
            pl.when(pl.col("win_loss") == "L").then(1).otherwise(0).alias("losses"),
        )
        .group_by(*team_stats_dimensions)
        .agg(
            pl.sum("wins"),
            pl.sum("losses"),
            pl.count("game_id").alias("total_games"),
            *[pl.mean(f"team_{metric}") for metric in TEAM_METRICS],
            *[pl.mean(f"opponent_{metric}") for metric in TEAM_METRICS],
        )
    )


@task(log_prints=True)
def get_team_season_stats() -> str:
    team_stats = nba_bucket.scan_parquet(bucket_conf.raw.games_detail)

    home_games = get_transformed_games(team_stats, "home")
    away_games = get_transformed_games(team_stats, "away")
    full_games = create_full_games(home_games, away_games, team_stats)

    team_season_stats = compute_team_season_stats(full_games)

    output_path = nba_bucket.sink_parquet(
        team_season_stats, "team_season_stats.parquet"
    )

    return output_path
