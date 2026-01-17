import polars as pl

from polars import LazyFrame


class TeamSeasonProcessor(object):
    def __init__(self, metrics: list, config_map: dict):
        self.metrics = metrics
        self.config_map = config_map

    def get_transformed_games(
        self, games_detail: LazyFrame, conf_type: str
    ) -> LazyFrame:
        """
        Transform the games detail LazyFrame based on the configuration type.
        """
        try:
            applied_config = self.config_map[conf_type]
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

    @staticmethod
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

    def compute_team_season_stats(self, full_games: LazyFrame) -> LazyFrame:
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
                *[pl.mean(f"team_{metric}") for metric in self.metrics],
                *[pl.mean(f"opponent_{metric}") for metric in self.metrics],
            )
        )

    def run(self, team_stats: LazyFrame) -> LazyFrame:
        home_games = self.get_transformed_games(team_stats, "home")
        away_games = self.get_transformed_games(team_stats, "away")
        full_games = self.create_full_games(home_games, away_games, team_stats)

        team_season_stats = self.compute_team_season_stats(full_games)

        return team_season_stats
