"""
loaders/rosters.py
==================
Source: nflreadpy.load_rosters_weekly()

Weekly roster snapshots. One row per player per week.

Included:
  - Roster status / status description (active, IR, practice squad, etc.)
  - Position + depth chart position
  - Physical attributes (height/weight as of that season — can change year over year)

Excluded:
  - full_name (available in player_info)
  - birth_date (PII-adjacent)
  - jersey_number (cosmetic)
  - espn_id, sportradar_id (bridging handled by fantasy_ids table)
  - pfr_id (available in player_info and fantasy_ids)
  - years_exp, entry_year, rookie_year (available in player_info)
"""

import pandas as pd
import nflreadpy as nfl

_KEEP_COLS = [
    "season", "week", "game_type", "gsis_id", "team",
    "position",
    "status", "status_description_abbr",
]


def load(seasons: list[int]) -> pd.DataFrame:
    """
    Load weekly roster data.

    Args:
        seasons: List of NFL seasons.

    Returns:
        DataFrame with one row per player per week (deduped, keeping last entry).
    """
    raw = nfl.load_rosters_weekly(seasons=seasons).to_pandas()

    df = raw[[c for c in _KEEP_COLS if c in raw.columns]].copy()

    df["season"]  = df["season"].astype(int)
    df["week"]    = df["week"].astype(int)
    df["gsis_id"] = df["gsis_id"].astype(str).str.strip()

    # Drop malformed IDs
    df = df[df["gsis_id"].str.startswith("00-")].copy()

    df.rename(columns={
        "position":               "ros_position",
        "status":                 "ros_status",
        "status_description_abbr":"ros_status_desc",
    }, inplace=True)

    # Keep latest entry per player per week in case of mid-week moves
    df = (
        df.sort_values("week")
        .drop_duplicates(subset=["season", "week", "gsis_id"], keep="last")
    )

    return df
