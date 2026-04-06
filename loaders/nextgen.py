"""
loaders/nextgen.py
==================
Source: nflreadpy.load_nextgen_stats()

Next Gen Stats — AWS tracking metrics. One row per player per week.
Covers passing (QB), rushing (all ball carriers), receiving (all targets).

Merge strategy: wide join on player+week keys so a dual-threat QB gets
one row with both ng_pass_* and ng_rush_* columns populated.

Key metrics by group:
  Passing  — time to throw, air yards, CPAE, aggressiveness, passer rating
  Rushing  — efficiency, avg time to LOS, RYOE, % attempts vs 8-box
  Receiving — cushion, separation, intended air yards share, YAC above exp
"""

import pandas as pd
import nflreadpy as nfl

# ── column configs per stat type ─────────────────────────────────────────────
# (raw_col, output_alias)  — None alias → keep raw name
_PASS_COLS = [
    ("player_gsis_id",                          "gsis_id"),
    ("season",                                  None),
    ("week",                                    None),
    ("season_type",                             None),
    ("team_abbr",                               "team"),
    ("player_display_name",                     None),
    ("player_position",                         None),
    ("avg_time_to_throw",                       None),
    ("avg_completed_air_yards",                 None),
    ("avg_intended_air_yards",                  None),
    ("avg_air_yards_differential",              None),
    ("aggressiveness",                          None),
    ("max_completed_air_distance",              None),
    ("avg_air_yards_to_sticks",                 None),
    ("completion_percentage",                   None),
    ("expected_completion_percentage",          None),
    ("completion_percentage_above_expectation", None),
    ("avg_air_distance",                        None),
    ("max_air_distance",                        None),
    ("passer_rating",                           None),
    ("attempts",                                None),
    ("pass_yards",                              None),
    ("pass_touchdowns",                         None),
    ("interceptions",                           None),
]

_RUSH_COLS = [
    ("player_gsis_id",                          "gsis_id"),
    ("season",                                  None),
    ("week",                                    None),
    ("season_type",                             None),
    ("team_abbr",                               "team"),
    ("player_display_name",                     None),
    ("player_position",                         None),
    ("efficiency",                              None),
    ("percent_attempts_gte_eight_defenders",    None),
    ("avg_time_to_los",                         None),
    ("rush_attempts",                           None),
    ("rush_yards",                              None),
    ("avg_rush_yards",                          None),
    ("rush_touchdowns",                         None),
    ("expected_rush_yards",                     None),
    ("rush_yards_over_expected",                None),
    ("rush_yards_over_expected_per_att",        None),
    ("rush_pct_over_expected",                  None),
]

_REC_COLS = [
    ("player_gsis_id",                          "gsis_id"),
    ("season",                                  None),
    ("week",                                    None),
    ("season_type",                             None),
    ("team_abbr",                               "team"),
    ("player_display_name",                     None),
    ("player_position",                         None),
    ("avg_cushion",                             None),
    ("avg_separation",                          None),
    ("avg_intended_air_yards",                  None),
    ("percent_share_of_intended_air_yards",     None),
    ("receptions",                              None),
    ("targets",                                 None),
    ("catch_percentage",                        None),
    ("yards",                                   "rec_yards"),   # disambiguate from rush/pass yards
    ("rec_touchdowns",                          None),
    ("avg_yac",                                 None),
    ("avg_expected_yac",                        None),
    ("avg_yac_above_expectation",               None),
]

# Keys shared across all three tables — not prefixed
_JOIN_KEYS = {"gsis_id", "season", "week", "season_type", "team",
              "player_display_name", "player_position"}

_STAT_CONFIGS = [
    ("passing",   _PASS_COLS, "ng_pass"),
    ("rushing",   _RUSH_COLS, "ng_rush"),
    ("receiving", _REC_COLS,  "ng_rec"),
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_one(seasons, stat_type: str, col_spec: list, prefix: str) -> pd.DataFrame:
    """
    Load a single NGS stat type, slice + rename columns, apply prefix to
    metric columns only (join keys stay bare for the merge).
    """
    raw = nfl.load_nextgen_stats(seasons, stat_type=stat_type).to_pandas()

    # build rename map from col_spec
    rename = {raw_col: alias for raw_col, alias in col_spec if alias}
    raw.rename(columns=rename, inplace=True)

    # resolve final col names after aliasing
    keep = []
    for raw_col, alias in col_spec:
        final = alias if alias else raw_col
        if final in raw.columns:
            keep.append(final)

    df = raw[keep].copy()

    # prefix metric cols
    df.rename(columns={
        col: f"{prefix}_{col}"
        for col in df.columns
        if col not in _JOIN_KEYS
    }, inplace=True)

    return df


# ── public API ────────────────────────────────────────────────────────────────

def load(seasons) -> pd.DataFrame:
    merge_keys = [
        "gsis_id", "season", "week", "season_type",
        "team", "player_display_name", "player_position"
    ]

    frames = [
        _load_one(seasons, stat_type, col_spec, prefix)
        for stat_type, col_spec, prefix in _STAT_CONFIGS
    ]

    df = frames[0]
    for right in frames[1:]:
        df = df.merge(right, on=merge_keys, how="outer")

    df.sort_values(["season", "week", "gsis_id"], ignore_index=True, inplace=True)
    return df