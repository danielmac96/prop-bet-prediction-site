"""
loaders/weekly_player_stats.py
===============================
Source: nflreadpy.load_player_stats(summary_level="week")

One row per player per game. Covers QB/RB/WR/TE/DB/DL/LB.

Excluded (noise / kicker-specific / redundant with other feeds):
  - fantasy_points, fantasy_points_ppr  → computed in model layer
  - kicking cols (fg_*, pat_*, gwfg_*)  → kickers not in scope
  - special_teams_tds                   → rare, low signal
  - penalty/fumble recovery cols        → available in team feed
  - player_name, position               → use player_info feed instead
  - player_display_name, position_group, headshot_url → static; join to player_info
"""

import pandas as pd
import nflreadpy as nfl

_POSITION_GROUPS = {"DB", "DL", "LB", "QB", "RB", "TE", "WR"}

_DROP_COLS = [
    "player_name", "position",
    "player_display_name", "headshot_url", "position_group",  # static — join to player_info
    "passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions",
    "special_teams_tds",
    "def_sack_yards", "def_interception_yards", "def_tds", "def_fumbles", "def_safeties",
    "misc_yards",
    "fumble_recovery_own", "fumble_recovery_yards_own",
    "fumble_recovery_opp", "fumble_recovery_yards_opp", "fumble_recovery_tds",
    "penalties", "penalty_yards",
    "punt_returns", "punt_return_yards", "kickoff_returns", "kickoff_return_yards",
    "fg_made", "fg_att", "fg_missed", "fg_blocked", "fg_long", "fg_pct",
    "fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49",
    "fg_made_50_59", "fg_made_60_",
    "fg_missed_0_19", "fg_missed_20_29", "fg_missed_30_39", "fg_missed_40_49",
    "fg_missed_50_59", "fg_missed_60_",
    "fg_made_list", "fg_missed_list", "fg_blocked_list",
    "fg_made_distance", "fg_missed_distance", "fg_blocked_distance",
    "pat_made", "pat_att", "pat_missed", "pat_blocked", "pat_pct",
    "gwfg_made", "gwfg_att", "gwfg_missed", "gwfg_blocked", "gwfg_distance",
    "fantasy_points", "fantasy_points_ppr",
    "def_tackles_for_loss_yards", "def_fumbles_forced",
    "sack_yards_lost", "sack_fumbles", "sack_fumbles_lost",
]


def load(seasons: list[int]) -> pd.DataFrame:
    """
    Load weekly player stats for skill positions and defenders.

    Args:
        seasons: List of NFL seasons.

    Returns:
        DataFrame with one row per player per game.
    """
    raw = nfl.load_player_stats(seasons=seasons, summary_level="week").to_pandas()

    df = raw.copy()
    df.drop(columns=[c for c in _DROP_COLS if c in df.columns], inplace=True)
    df = df[df["position_group"].isin(_POSITION_GROUPS)].copy()

    df["season"] = df["season"].astype(int)
    df["week"]   = df["week"].astype(int)

    df.rename(columns={"player_id": "gsis_id"}, inplace=True)

    # Convenience: total TDs in one column for quick fantasy calc
    df["total_tds"] = (
        df["passing_tds"].fillna(0)
        + df["rushing_tds"].fillna(0)
        + df["receiving_tds"].fillna(0)
    )

    return df
