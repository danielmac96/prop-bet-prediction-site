"""
loaders/nextgen.py
==================
Source: nflreadpy.load_nextgen_stats()

Next Gen Stats — QB tracking metrics from AWS. One row per QB per week.
Only available for QBs (passing position group).

Key metrics:
  - Time to throw, completed / intended air yards, aggressiveness
  - CPAE (completion % above expectation)
  - Passer rating, air distance, sticks metrics

Excluded:
  - Non-QB positions (NGS currently only meaningful for passers)
"""

import pandas as pd
import nflreadpy as nfl

_KEEP_COLS = [
    "gsis_id", "season", "week", "season_type", "team",
    "player_display_name", "player_position",
    "avg_time_to_throw", "avg_completed_air_yards", "avg_intended_air_yards",
    "avg_air_yards_differential", "aggressiveness",
    "max_completed_air_distance", "avg_air_yards_to_sticks",
    "completion_percentage", "expected_completion_percentage",
    "completion_percentage_above_expectation",
    "avg_air_distance", "max_air_distance",
    "passer_rating", "attempts", "pass_yards", "pass_touchdowns", "interceptions",
]

_RENAME_IGNORE = {"gsis_id", "season", "week", "season_type", "team"}


def load() -> pd.DataFrame:
    """
    Load Next Gen Stats for all available seasons.

    Returns:
        DataFrame with one row per QB per week, columns prefixed with 'ng_'.
    """
    raw = nfl.load_nextgen_stats().to_pandas()
    raw.rename(columns={"player_gsis_id": "gsis_id", "team_abbr": "team"}, inplace=True)

    df = raw[[c for c in _KEEP_COLS if c in raw.columns]].copy()

    # Prefix non-key columns with ng_
    df.rename(columns={
        col: f"ng_{col}"
        for col in df.columns
        if col not in _RENAME_IGNORE and not col.startswith("ng_")
    }, inplace=True)

    return df
