"""
loaders/pfr_adv_stats.py
=========================
Source: nflreadpy.load_pfr_advstats()

Pro Football Reference advanced stats. One row per player per game.
Keyed on pfr_player_id — join to gsis_id via fantasy_ids table.

Included:
  - Drop rates (passing + receiving)
  - Bad throw rate
  - Pressure metrics: blitzed, hurried, hit, pressured rate
  - Defensive pressure contribution: blitzes, hurries, QB hits

Excluded:
  - pfr_player_name (join to player_info for names)
  - Redundant counting stats available in weekly_player_stats
"""

import pandas as pd
import nflreadpy as nfl

_KEEP_COLS = [
    "game_id", "season", "week", "game_type", "team", "opponent",
    "pfr_player_id", "pfr_player_name",
    "passing_drops", "passing_drop_pct",
    "receiving_drop", "receiving_drop_pct",
    "passing_bad_throws", "passing_bad_throw_pct",
    "times_sacked", "times_blitzed", "times_hurried", "times_hit",
    "times_pressured", "times_pressured_pct",
    "def_times_blitzed", "def_times_hurried", "def_times_hitqb",
]

_RENAME_IGNORE = {
    "game_id", "season", "week", "game_type",
    "team", "opponent", "pfr_player_id", "pfr_player_name",
}


def load() -> pd.DataFrame:
    """
    Load PFR advanced stats for all available seasons.

    Returns:
        DataFrame with one row per player per game, stat cols prefixed with 'pfr_'.
    """
    raw = nfl.load_pfr_advstats().to_pandas()

    df = raw[[c for c in _KEEP_COLS if c in raw.columns]].copy()
    df["season"] = df["season"].astype(int)
    df["week"]   = df["week"].astype(int)

    df.rename(columns={
        col: f"pfr_{col}"
        for col in df.columns
        if col not in _RENAME_IGNORE and not col.startswith("pfr_")
    }, inplace=True)

    return df
