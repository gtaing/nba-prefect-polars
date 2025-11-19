import polars as pl

from polars.testing import assert_frame_equal
from src.players.season_stats import get_player_season_stats


def test_get_player_season_stats(monkeypatch):
    test_data = {
        "raw/playerstatistics.parquet": pl.LazyFrame(
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
            }
        ),
        "raw/games_detail.parquet": pl.LazyFrame(
            {
                "game_id": [1, 2, 3, 4],
                "season_id": [1, 1, 1, 1],
                "game_date": ["2023-10-01", "2023-01-01", "2023-10-21", "2023-12-01"],
            }
        ),
    }

    # Expected result
    expected = pl.DataFrame(
        {
            "season": [2023, 2023],
            "firstName": ["Jane", "John"],
            "lastName": ["Smith", "Doe"],
            "personId": [102, 101],
            "gameType": ["Regular", "Regular"],
            "GP": [2, 2],
            "PTS": [20.0, 15.0],  # mean of points
            "REB": [7.0, 6.0],  # mean of rebounds
        }
    )

    # Patch pl.scan_parquet to return our test data
    monkeypatch.setattr(
        "config.bucket.nba_bucket.scan_parquet",
        lambda filepath: test_data.get(filepath),
    )
    # Patch PLAYERS_METRICS
    monkeypatch.setattr(
        "src.players.season_stats.PLAYERS_METRICS",
        {"points": "PTS", "rebounds": "REB"},
    )

    # Run the function
    output_path = get_player_season_stats.__wrapped__()

    result = pl.read_parquet(output_path)

    assert_frame_equal(
        result,
        expected,
        check_column_order=False,
        check_row_order=False,
        check_dtypes=False,
    )