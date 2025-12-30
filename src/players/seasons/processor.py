import polars as pl

from polars import LazyFrame


class PlayerSeasonProcessor:
    def __init__(self, metrics: dict):
        self.metrics = metrics

    def filter_and_rename(self, game_stats: LazyFrame) -> LazyFrame:
        """
        Scan the player game statistics CSV file from S3.
        """

        is_after_2014 = pl.col("gameDate").str.to_datetime().dt.year() >= 2014

        return game_stats.filter(is_after_2014).rename(self.metrics)

    @staticmethod
    def compute_true_shooting(player_stats: LazyFrame) -> LazyFrame:
        ts_attempts = pl.col("fieldGoalsAttempted") + 0.44 * pl.col(
            "freeThrowsAttempted"
        )
        ts_percentage = pl.col("points") / (2 * ts_attempts)

        return player_stats.with_columns(
            ts_attempts.alias("trueShootingAttempts"),
            ts_percentage.alias("trueShootingPercentage"),
        )

    def compute_season_avg(
        self, players_stats: LazyFrame, scope_game_ids: LazyFrame
    ) -> LazyFrame:
        """
        Compute season stats of NBA players.
        """

        dimensions = ["season", "firstName", "lastName", "personId", "gameType"]
        number_of_games_played = pl.col("gameId").n_unique().alias("GP")

        average_metrics = [
            pl.mean(metric).round(1).alias(metric) for metric in self.metrics.values()
        ]

        return (
            players_stats.join(
                scope_game_ids, how="inner", left_on="gameId", right_on="game_id"
            )
            .group_by(*dimensions)
            .agg(number_of_games_played, *average_metrics)
        )

    def run(self, game_stats: LazyFrame, scope_game_ids: LazyFrame) -> LazyFrame:
        """
        Get the players' season statistics by aggregating game stats.
        """

        consolidated_stats = self.compute_true_shooting(game_stats)
        prepared_stats = self.filter_and_rename(consolidated_stats)

        season_stats = self.compute_season_avg(prepared_stats, scope_game_ids)

        return season_stats
