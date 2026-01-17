import pytest
import datetime as datetime
import polars as pl

from polars.testing import assert_frame_equal
from src.teams.seasons.processor import TeamSeasonProcessor


class TestTeamSeasonProcessor:
    @pytest.fixture(autouse=True)
    def setup_processor(self):
        mock_team_metrics = ["pts", "reb"]
        mock_team_config_map = {
            "home": {
                "renaming": {
                    "team_abbreviation_home": "team",
                    "team_name_home": "team_name",
                    "wl_home": "win_loss",
                },
                "team_metrics": {"pts_home": "team_pts"},
                "opponent_metrics": {"pts_away": "opponent_pts"},
            },
            "away": {
                "renaming": {
                    "team_abbreviation_home": "team",
                    "team_name_home": "team_name",
                    "wl_away": "win_loss",
                },
                "team_metrics": {"pts_away": "team_pts"},
                "opponent_metrics": {"pts_home": "opponent_pts"},
            },
        }

        self.processor = TeamSeasonProcessor(mock_team_metrics, mock_team_config_map)

    def test_get_transformed_games(self):
        games_details = pl.LazyFrame(
            {
                "game_id": [1, 2],
                "season_id": [2023, 2023],
                "season_type": ["Regular Season", "Pre Season"],
                "game_date": ["2023-01-01", "2023-01-02"],
                "team_abbreviation_home": ["TeamA", "TeamB"],
                "team_name_home": ["Team A", "Team B"],
                "wl_home": [1, 1],
                "wl_away": [0, 0],
                "pts_home": [100, 110],
                "pts_away": [90, 105],
            }
        )

        result = self.processor.get_transformed_games(games_details, "home").collect()

        expected = pl.DataFrame(
            {
                "game_id": [1],
                "season_id": [2023],
                "season_type": ["Regular Season"],
                "game_date": [datetime.datetime(2023, 1, 1)],
                "game_location": ["home"],
                "team": ["TeamA"],
                "team_name": ["Team A"],
                "win_loss": [1],
                "team_pts": [100],
                "opponent_pts": [90],
            }
        )

        assert_frame_equal(result, expected, check_dtypes=False)

    def test_create_full_games(self):
        home_games = pl.LazyFrame(
            {"game_id": [1], "season_id": [1], "location": "home"}
        )
        away_games = pl.LazyFrame(
            {"game_id": [2], "season_id": [2], "location": "away"}
        )
        games_detail = pl.LazyFrame(
            {"season_id": [1, 2], "game_date": ["2023-01-01", "2001-01-01"]}
        )

        expected = pl.DataFrame(
            {"game_id": [1], "season_id": [1], "location": ["home"], "season": [2023]}
        )

        result = self.processor.create_full_games(
            home_games=home_games, away_games=away_games, games_detail=games_detail
        )

        assert_frame_equal(result.collect(), expected, check_dtypes=False)

    def test_compute_team_season_stats(self):
        full_games = pl.LazyFrame(
            {
                "season_id": [2023, 2023, 2023],
                "team": ["A", "A", "B"],
                "team_name": ["Alpha", "Alpha", "Beta"],
                "season": [2023, 2023, 2023],
                "game_id": [1, 2, 3],
                "win_loss": ["W", "L", "W"],
                "team_pts": [100, 90, 110],
                "team_reb": [50, 45, 55],
                "opponent_pts": [95, 100, 100],
                "opponent_reb": [48, 47, 50],
            }
        )

        expected = pl.DataFrame(
            {
                "season_id": [2023, 2023],
                "team": ["A", "B"],
                "team_name": ["Alpha", "Beta"],
                "season": [2023, 2023],
                "wins": [1, 1],
                "losses": [1, 0],
                "total_games": [2, 1],
                "team_pts": [95.0, 110.0],
                "team_reb": [47.5, 55.0],
                "opponent_pts": [97.5, 100.0],
                "opponent_reb": [47.5, 50.0],
            }
        )

        result = self.processor.compute_team_season_stats(full_games).collect()

        assert_frame_equal(
            result,
            expected,
            check_dtypes=False,
            check_column_order=False,
            check_row_order=False,
        )

    def test_run(self):
        pass
