"""
loaders/weekly_team_stats.py
=============================
Source: nflreadpy.load_team_stats(summary_level="week")

One row per team per game. Aggregated offensive + defensive + special teams.

Excluded:
  - Granular FG breakdown lists (fg_made_0_19, fg_made_list, etc.)
    → noisy and rarely used in team-level models
"""

import pandas as pd
import nflreadpy as nfl

# Pattern fragments that identify granular kicking breakdown columns to drop
_KICKING_PATTERNS = [
    "fg_made_0", "fg_made_2", "fg_made_3", "fg_made_4", "fg_made_5", "fg_made_6",
    "fg_missed_0", "fg_missed_2", "fg_missed_3", "fg_missed_4", "fg_missed_5", "fg_missed_6",
    "fg_made_list", "fg_missed_list", "fg_blocked_list",
    "fg_made_distance", "fg_missed_distance", "fg_blocked_distance",
]


def load(seasons: list[int]) -> pd.DataFrame:
    """
    Load weekly team-level stats.

    Args:
        seasons: List of NFL seasons.

    Returns:
        DataFrame with one row per team per game.
    """
    raw = nfl.load_team_stats(seasons, "week").to_pandas()

    df = raw.copy()

    drop_cols = [
        c for c in df.columns
        if any(pattern in c for pattern in _KICKING_PATTERNS)
    ]
    df.drop(columns=drop_cols, inplace=True)

    df["season"] = df["season"].astype(int)
    df["week"]   = df["week"].astype(int)

    return df
