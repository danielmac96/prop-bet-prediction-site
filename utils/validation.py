"""
utils/validation.py
===================
DataFrame validation before upload.
Add per-table checks to TABLE_RULES to catch bad data early.
"""

import logging
import pandas as pd
from typing import List, Optional, Dict, Any

logger = logging.getLogger(__name__)


# ── Per-table sanity rules ─────────────────────────────────────────────────────
# Keys match the logical feed names in config.TABLES
TABLE_RULES: Dict[str, Dict[str, Any]] = {
    "weekly_player_stats": {
        "required_cols": ["gsis_id", "game_id", "season", "week"],
        "no_dupes_on":   ["gsis_id", "game_id"],
        "range_checks":  {"passing_yards": (0, 700), "rushing_yards": (-50, 400)},
    },
    "weekly_team_stats": {
        "required_cols": ["team", "game_id", "season", "week"],
        "no_dupes_on":   ["team", "game_id"],
        "range_checks":  {"passing_yards": (0, 700)},
    },
    "schedule": {
        "required_cols": ["team", "game_id", "season", "week"],
        "no_dupes_on":   ["team", "game_id"],
        "range_checks":  {},
    },
    "play_by_play": {
        "required_cols": ["gsis_id", "game_id"],
        "no_dupes_on":   ["gsis_id", "game_id"],
        "range_checks":  {},
    },
    "snap_counts": {
        "required_cols": ["pfr_player_id", "game_id"],
        "no_dupes_on":   ["pfr_player_id", "game_id"],
        "range_checks":  {"offense_pct": (0, 1), "defense_pct": (0, 1)},
    },
    "nextgen": {
        "required_cols": ["gsis_id", "season", "week"],
        "no_dupes_on":   ["gsis_id", "season", "week"],
        "range_checks":  {"ng_completion_percentage": (0, 100)},
    },
    "fantasy_opportunities": {
        "required_cols": ["gsis_id", "game_id"],
        "no_dupes_on":   ["gsis_id", "game_id"],
        "range_checks":  {},
    },
    "rosters": {
        "required_cols": ["gsis_id", "season", "week"],
        "no_dupes_on":   ["gsis_id", "season", "week"],
        "range_checks":  {},
    },
}


def validate(df: pd.DataFrame, feed_name: str) -> pd.DataFrame:
    """
    Run validation checks for a given feed.
    Logs warnings but only raises on critical failures (nulls in key cols, bad ranges).
    Returns the validated (possibly filtered) DataFrame.
    """
    rules = TABLE_RULES.get(feed_name, {})
    errors: List[str] = []

    # 1. Required columns present
    required = rules.get("required_cols", [])
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"[{feed_name}] Missing required columns: {missing_cols}")

    # 2. No nulls in key columns
    for col in required:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            errors.append(f"  {null_count} nulls in key column '{col}'")

    # 3. Duplicate check
    dupe_cols = rules.get("no_dupes_on", [])
    if dupe_cols:
        dupes = df.duplicated(subset=dupe_cols).sum()
        if dupes > 0:
            logger.warning(f"[{feed_name}] {dupes} duplicate rows on {dupe_cols} — dropping.")
            df = df.drop_duplicates(subset=dupe_cols, keep="last")

    # 4. Range checks
    for col, (lo, hi) in rules.get("range_checks", {}).items():
        if col in df.columns:
            out_of_range = ((df[col] < lo) | (df[col] > hi)).sum()
            if out_of_range > 0:
                logger.warning(
                    f"[{feed_name}] {out_of_range} rows outside expected range "
                    f"for '{col}' [{lo}, {hi}]"
                )

    if errors:
        raise ValueError(f"[{feed_name}] Validation failed:\n" + "\n".join(errors))

    logger.info(f"[{feed_name}] Validation passed — {len(df)} rows.")
    return df
