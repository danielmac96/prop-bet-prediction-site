"""
loaders/player_info.py
=======================
Source: nflreadpy.load_players()

Static player bio data. One row per player.
Strategy: snapshot — upsert on each run (handles new signings, retirements).

Included:
  - Physical attributes (height, weight)
  - Draft info (round, pick, rookie season)
  - Position mappings across systems (ngs, pff)
  - Cross-system IDs (pfr_id for joining to snap_counts / pfr_adv_stats)

Excluded:
  - birth_date (PII-adjacent, not used in models)
  - esb_id, nfl_id, espn_id, smart_id (bridging handled by fantasy_ids table)
  - pff_id, otc_id (rarely populated; cross-system bridging in fantasy_ids)
"""

import pandas as pd
import nflreadpy as nfl

_KEEP_COLS = [
    "gsis_id", "display_name", "position_group",
    "ngs_position", "height", "weight",
    "rookie_season", "draft_round", "draft_pick",
    "years_of_experience", "pff_position",
]


def load() -> pd.DataFrame:
    """
    Load static player information.

    Returns:
        DataFrame with one row per player (gsis_id).
    """
    raw = nfl.load_players().to_pandas()

    df = raw[[c for c in _KEEP_COLS if c in raw.columns]].copy()

    df.rename(columns={
        "display_name":   "pi_display_name",
        "position_group": "pi_position_group",
    }, inplace=True)

    return df
