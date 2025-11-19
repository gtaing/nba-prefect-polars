TEAM_METRICS = [
    "pts",
    "fgm",
    "fga",
    "fg_pct",
    "fg3m",
    "fg3a",
    "fg3_pct",
    "ftm",
    "fta",
    "ft_pct",
    "oreb",
    "dreb",
    "reb",
    "ast",
]

TEAM_CONFIG_MAP = {
    "home": {
        "team_metrics": {f"{metric}_home": f"team_{metric}" for metric in TEAM_METRICS},
        "opponent_metrics": {
            f"{metric}_away": f"opponent_{metric}" for metric in TEAM_METRICS
        },
        "renaming": {
            "wl_home": "win_loss",
            "team_abbreviation_home": "team",
            "team_name_home": "team_name",
            "team_abbreviation_away": "opponent",
            "team_name_away": "opponent_name",
        },
    },
    "away": {
        "team_metrics": {f"{metric}_away": f"team_{metric}" for metric in TEAM_METRICS},
        "opponent_metrics": {
            f"{metric}_home": f"opponent_{metric}" for metric in TEAM_METRICS
        },
        "renaming": {
            "wl_away": "win_loss",
            "team_abbreviation_home": "opponent",
            "team_name_home": "opponent_name",
            "team_abbreviation_away": "team",
            "team_name_away": "team_name",
        },
    },
}