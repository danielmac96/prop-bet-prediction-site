"""
loaders/snap_counts.py
=======================
Source: nflreadpy.load_snap_counts()

One row per player per game.
Includes offense / defense / special teams snap counts and percentages.

Excluded:
  - gsis_id (use pfr_player_id as key; join to gsis via fantasy_ids table)
"""

import pandas as pd
import nflreadpy as nfl

_KEEP_COLS = [
    "game_id", "season", "week", "game_type",
    "pfr_player_id", "player", "position", "team",
    "offense_snaps", "offense_pct",
    "defense_snaps", "defense_pct",
    "st_snaps", "st_pct",
]


def load(seasons: list[int] | None = None) -> pd.DataFrame:
    """
    Load snap count data.

    Args:
        seasons: Optional list of seasons. If None, loads all available.

    Returns:
        DataFrame with one row per player per game.
    """
    raw = nfl.load_snap_counts().to_pandas()
    if seasons:
        raw = raw[raw["season"].isin(seasons)]

    df = raw[[c for c in _KEEP_COLS if c in raw.columns]].copy()
    df["season"] = df["season"].astype(int)
    df["week"]   = df["week"].astype(int)

    df.rename(columns={"player": "snap_player_name", "position": "snap_position"}, inplace=True)

    return df
