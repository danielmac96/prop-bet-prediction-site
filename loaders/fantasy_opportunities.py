"""
loaders/fantasy_opportunities.py
==================================
Source: nflreadpy.load_ff_opportunity()

Expected vs actual fantasy opportunity model outputs. One row per player per game.
All stat cols prefixed with 'opps_'.

This feed is the core of opportunity-based modeling:
  - Expected fantasy points (opps_*_fantasy_points_exp) from Vegas-calibrated models
  - Actual vs expected diffs (opps_*_diff) → identify over/underperformers
  - Useful for: target share regression, usage stability, matchup projections

Included:
  - Pass / rec / rush attempts, air yards, yards, TDs, first downs
  - Expected values and actual-vs-expected diffs for all key metrics
  - Two-point conversion opportunities (rare but included for completeness)

Excluded:
  - Team-level opportunity columns (use weekly_team_stats for team totals)
"""

import pandas as pd
import nflreadpy as nfl

_ID_COLS     = ["gsis_id", "game_id", "season", "week", "team", "full_name", "position"]
_RENAME_IGNORE = set(_ID_COLS)


def load(seasons: list[int] | None = None) -> pd.DataFrame:
    """
    Load fantasy opportunity data.

    Args:
        seasons: Optional list of seasons to filter. If None, loads all available.

    Returns:
        DataFrame with one row per player per game, stat cols prefixed with 'opps_'.
    """
    raw = nfl.load_ff_opportunity().to_pandas()
    raw.rename(columns={"player_id": "gsis_id", "posteam": "team"}, inplace=True)

    if seasons:
        raw = raw[raw["season"].isin(seasons)]

    # Build column list: ID cols + non-team stat cols
    stat_cols = [
        c for c in raw.columns
        if c not in _RENAME_IGNORE and not c.endswith("_team")
    ]
    df = raw[_ID_COLS + stat_cols].copy()

    # Prefix stat cols with opps_
    df.rename(columns={
        col: f"opps_{col}"
        for col in stat_cols
        if not col.startswith("opps_")
    }, inplace=True)

    return df
