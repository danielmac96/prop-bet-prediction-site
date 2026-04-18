"""
loaders/depth_chart.py
=======================
Source: nflreadpy.load_depth_charts()

Snapshot of the most recent depth chart date only.
One row per player (primary position).

Strategy: snapshot — full replace on each run (depth charts change weekly).

Excluded:
  - Historical depth chart entries (only latest date is actionable)
  - Secondary position slots (keep the lowest pos_rank per player)
"""

import pandas as pd
import nflreadpy as nfl

_KEEP_COLS = ["gsis_id", "team", "pos_abb", "pos_slot", "pos_rank", "pos_name", "pos_grp"]


def load(seasons:list) -> pd.DataFrame:
    """
    Load the latest depth chart snapshot.

    Returns:
        DataFrame with one row per player at their primary depth chart position.
    """
    raw = nfl.load_depth_charts(seasons=[2025]).to_pandas()
    raw["dt"] = pd.to_datetime(raw["dt"], errors="coerce")

    # Keep only the most recent date
    latest_dt = raw["dt"].max()
    df = raw[raw["dt"] == latest_dt].copy()

    # One row per player — take their highest-ranked (lowest pos_rank) slot
    df = (
        df.sort_values("pos_rank")
        .drop_duplicates(subset=["gsis_id"], keep="first")
        [[c for c in _KEEP_COLS if c in df.columns]]
        .copy()
    )

    df.rename(columns={
        "team":     "dc_team",
        "pos_abb":  "dc_pos_abb",
        "pos_slot": "dc_pos_slot",
        "pos_rank": "dc_pos_rank",
        "pos_name": "dc_pos_name",
        "pos_grp":  "dc_pos_grp",
    }, inplace=True)

    return df
