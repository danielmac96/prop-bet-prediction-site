"""
loaders/fantasy_ids.py
=======================
Source: nflreadpy.load_ff_playerids()

Cross-system ID bridge table. One row per pfr_player_id.
Use this to join pfr_player_id (snap_counts, pfr_adv_stats) → gsis_id.

Strategy: snapshot — re-upsert on each run to catch new player mappings.

Included:
  - pfr_id → gsis_id mapping (primary join key)
  - sleeper_id, yahoo_id, mfl_id (useful for fantasy data ingestion)

Excluded:
  - name, position, team, db_season (use player_info / rosters for current values)
  - Duplicate pfr_id rows (keep latest db_season entry)
"""

import pandas as pd
import nflreadpy as nfl

_KEEP_COLS = ["gsis_id", "pfr_id", "sportradar_id", "espn_id", "sleeper_id", "yahoo_id", "mfl_id"]


def load() -> pd.DataFrame:
    """
    Load cross-system player ID mappings.

    Returns:
        DataFrame with one row per pfr_player_id, deduplicated on latest season.
    """
    raw = nfl.load_ff_playerids().to_pandas()

    df = raw[[c for c in _KEEP_COLS if c in raw.columns]].copy()

    # Deduplicate: keep the most recent db_season entry per pfr_id
    if "db_season" in raw.columns:
        df = (
            raw.dropna(subset=["gsis_id", "pfr_id"])
            .sort_values("db_season", ascending=False)
            .drop_duplicates(subset=["pfr_id"])
            [[c for c in _KEEP_COLS if c in raw.columns]]
            .copy()
        )

    df.rename(columns={"pfr_id": "pfr_player_id"}, inplace=True)

    return df
