"""
loaders/fantasy_rankings.py
============================
Source: nflreadpy.load_ff_rankings()

Expert consensus rankings (ECR) from FantasyPros.
Strategy: snapshot — deduplicate to most recent scrape per player/page_type.

page_type distinguishes ranking context:
  - "weekly"  → start/sit week-specific rankings  ← most useful for betting models
  - "ros"     → rest-of-season rankings
  - "draft"   → pre-season ADP

rank_ecr_type:
  - "std" / "half" / "ppr" → scoring format

Included:
  - ECR rank, std deviation, best/worst case ranks
  - Ownership rates across platforms (ESPN, Yahoo, avg)
  - Rank delta (week-over-week movement) — useful as a momentum signal

Excluded:
  - rank_id (FantasyPros internal, not useful for joins)
  - Stale scrape_date entries (only keep latest per mergename/pos/team/page_type)
"""

import pandas as pd
import nflreadpy as nfl

_KEEP_COLS = [
    "mergename", "pos", "team", "page_type", "ecr_type",
    "ecr", "sd", "best", "worst", "rank_delta", "bye",
    "player_owned_avg", "player_owned_espn", "player_owned_yahoo",
    "scrape_date",
]

_RENAME_IGNORE = {"mergename", "pos", "team", "page_type"}


def load() -> pd.DataFrame:
    """
    Load the most recent fantasy expert consensus rankings.

    Returns:
        DataFrame with one row per player/pos/team/page_type, stat cols prefixed with 'rank_'.
    """
    raw = nfl.load_ff_rankings(type="week").to_pandas()
    raw["scrape_date"] = pd.to_datetime(raw["scrape_date"], errors="coerce")

    # Keep only the latest scrape per unique player+context
    df = (
        raw.sort_values("scrape_date", ascending=False)
        .drop_duplicates(subset=["mergename", "pos", "team", "page_type"])
        [[c for c in _KEEP_COLS if c in raw.columns]]
        .copy()
    )

    df.rename(columns={
        col: f"rank_{col}"
        for col in df.columns
        if col not in _RENAME_IGNORE and not col.startswith("rank_")
    }, inplace=True)

    return df
