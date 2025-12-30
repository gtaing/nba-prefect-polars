import polars as pl

from polars.testing import assert_frame_equal
from src.players.seasons.processor import PlayerSeasonProcessor


class TestPlayerSeasonProcessor:
    def test_compute_true_shooting(self):
        input_game_stats = pl.LazyFrame(
            {
                "points": [10, 20],
                "fieldGoalsAttempted": [5, 8],
                "freeThrowsAttempted": [2, 4],
            }
        )

        result = PlayerSeasonProcessor.compute_true_shooting(input_game_stats).collect()

        # expected TSA = FGA + 0.44 * FTA
        expected_tsa = [5 + 0.44 * 2, 8 + 0.44 * 4]
        # expected TS% = points / (2 * TSA)
        expected_ts = [10 / (2 * expected_tsa[0]), 20 / (2 * expected_tsa[1])]

        expected = pl.DataFrame(
            {
                "points": [10, 20],
                "fieldGoalsAttempted": [5, 8],
                "freeThrowsAttempted": [2, 4],
                "trueShootingAttempts": expected_tsa,
                "trueShootingPercentage": expected_ts,
            }
        )

        assert_frame_equal(
            result,
            expected,
            check_dtypes=False,
            check_column_order=False,
        )

    def test_compute_season_avg(self):
        mock_metrics = {"points": "PTS", "rebounds": "REB"}

        players_stats = pl.LazyFrame(
            {
                "gameId": [1, 2],
                "season": [2023, 2023],
                "firstName": ["John", "John"],
                "lastName": ["Doe", "Doe"],
                "personId": [101, 101],
                "gameType": ["Regular", "Regular"],
                "PTS": [10, 20],
                "REB": [5, 7],
            }
        )

        scope_game_ids = pl.LazyFrame(
            {
                "game_id": [1, 2],
                "season_id": [1, 1],
                "game_date": ["2023-10-01", "2023-10-02"],
            }
        )

        processor = PlayerSeasonProcessor(metrics=mock_metrics)
        result = processor.compute_season_avg(players_stats, scope_game_ids).collect()

        expected = pl.DataFrame(
            {
                "season": [2023],
                "firstName": ["John"],
                "lastName": ["Doe"],
                "personId": [101],
                "gameType": ["Regular"],
                "GP": [2],
                "PTS": [15.0],
                "REB": [6.0],
            }
        )

        assert_frame_equal(
            result,
            expected,
            check_column_order=False,
            check_row_order=False,
            check_dtypes=False,
        )

    def test_run(self):
        mock_metrics = {
            "points": "PTS",
            "rebounds": "REB",
            "fieldGoalsAttempted": "FGA",
            "freeThrowsAttempted": "FTA",
            "trueShootingAttempts": "TSA",
            "trueShootingPercentage": "TS%",
        }

        game_stats = pl.LazyFrame(
            {
                "gameId": [1, 2, 3, 4],
                "gameDate": ["2023-10-01", "2023-01-01", "2023-10-21", "2023-12-01"],
                "season": [2023, 2023, 2023, 2023],
                "firstName": ["John", "John", "Jane", "Jane"],
                "lastName": ["Doe", "Doe", "Smith", "Smith"],
                "personId": [101, 101, 102, 102],
                "gameType": ["Regular", "Regular", "Regular", "Regular"],
                "points": [10, 20, 15, 25],
                "rebounds": [5, 7, 6, 8],
                "fieldGoalsAttempted": [10, 12, 8, 10],
                "freeThrowsAttempted": [2, 2, 2, 2],
            }
        )
        scope_game_ids = pl.LazyFrame(
            {
                "game_id": [1, 2, 3, 4],
                "season_id": [1, 1, 1, 1],
                "game_date": ["2023-10-01", "2023-01-01", "2023-10-21", "2023-12-01"],
            }
        )

        expected = pl.DataFrame(
            {
                "season": [2023, 2023],
                "firstName": ["Jane", "John"],
                "lastName": ["Smith", "Doe"],
                "personId": [102, 101],
                "gameType": ["Regular", "Regular"],
                "GP": [2, 2],
                "PTS": [20.0, 15.0],
                "REB": [7.0, 6.0],
                "FGA": [9.0, 11.0],
                "FTA": [2.0, 2.0],
                "TSA": [9.9, 11.9],
                "TS%": [1.0, 0.6],
            }
        )

        processor = PlayerSeasonProcessor(metrics=mock_metrics)
        result = processor.run(
            game_stats=game_stats, scope_game_ids=scope_game_ids
        ).collect()

        assert_frame_equal(
            result,
            expected,
            check_column_order=False,
            check_row_order=False,
            check_dtypes=False,
        )
