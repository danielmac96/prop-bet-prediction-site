"""
loaders/pfr_adv_stats.py
=========================
Source: nflreadpy.load_pfr_advstats()

pass  — drops, bad throws, pressure absorbed (QB-centric)
rush  — yards before/after contact, broken tackles
rec   — drops, broken tackles, int, passer rating when targeted
def   — coverage metrics, pass rush contributions, tackling
"""

import pandas as pd
import nflreadpy as nfl

_JOIN_KEYS = {
    "game_id", "pfr_game_id", "season", "week", "game_type",
    "team", "opponent", "pfr_player_id", "pfr_player_name",
}

_PASS_COLS = [
    ("game_id",                  None),
    ("pfr_game_id",              None),
    ("season",                   None),
    ("week",                     None),
    ("game_type",                None),
    ("team",                     None),
    ("opponent",                 None),
    ("pfr_player_name",          None),
    ("pfr_player_id",            None),
    ("passing_drops",            None),
    ("passing_drop_pct",         None),
    ("receiving_drop",           None),   # targets thrown into drops
    ("receiving_drop_pct",       None),
    ("passing_bad_throws",       None),
    ("passing_bad_throw_pct",    None),
    ("times_sacked",             None),
    ("times_blitzed",            None),
    ("times_hurried",            None),
    ("times_hit",                None),
    ("times_pressured",          None),
    ("times_pressured_pct",      None),
    ("def_times_blitzed",        None),
    ("def_times_hurried",        None),
    ("def_times_hitqb",          None),
]

_RUSH_COLS = [
    ("game_id",                           None),
    ("pfr_game_id",                       None),
    ("season",                            None),
    ("week",                              None),
    ("game_type",                         None),
    ("team",                              None),
    ("opponent",                          None),
    ("pfr_player_name",                   None),
    ("pfr_player_id",                     None),
    ("carries",                           None),
    ("rushing_yards_before_contact",      None),
    ("rushing_yards_before_contact_avg",  None),
    ("rushing_yards_after_contact",       None),
    ("rushing_yards_after_contact_avg",   None),
    ("rushing_broken_tackles",            None),
    ("receiving_broken_tackles",          None),
]

_REC_COLS = [
    ("game_id",                  None),
    ("pfr_game_id",              None),
    ("season",                   None),
    ("week",                     None),
    ("game_type",                None),
    ("team",                     None),
    ("opponent",                 None),
    ("pfr_player_name",          None),
    ("pfr_player_id",            None),
    ("rushing_broken_tackles",   None),
    ("receiving_broken_tackles", None),
    ("passing_drops",            None),
    ("passing_drop_pct",         None),
    ("receiving_drop",           None),
    ("receiving_drop_pct",       None),
    ("receiving_int",            None),
    ("receiving_rat",            None),   # passer rating when targeted
]

_DEF_COLS = [
    ("game_id",                    None),
    ("pfr_game_id",                None),
    ("season",                     None),
    ("week",                       None),
    ("game_type",                  None),
    ("team",                       None),
    ("opponent",                   None),
    ("pfr_player_name",            None),
    ("pfr_player_id",              None),
    ("def_ints",                   None),
    ("def_targets",                None),
    ("def_completions_allowed",    None),
    ("def_completion_pct",         None),
    ("def_yards_allowed",          None),
    ("def_yards_allowed_per_cmp",  None),
    ("def_yards_allowed_per_tgt",  None),
    ("def_receiving_td_allowed",   None),
    ("def_passer_rating_allowed",  None),
    ("def_adot",                   None),
    ("def_air_yards_completed",    None),
    ("def_yards_after_catch",      None),
    ("def_times_blitzed",          None),
    ("def_times_hurried",          None),
    ("def_times_hitqb",            None),
    ("def_sacks",                  None),
    ("def_pressures",              None),
    ("def_tackles_combined",       None),
    ("def_missed_tackles",         None),
    ("def_missed_tackle_pct",      None),
]

_STAT_CONFIGS = [
    ("pass", _PASS_COLS, "pfr_pass"),
    ("rush", _RUSH_COLS, "pfr_rush"),
    ("rec",  _REC_COLS,  "pfr_rec"),
    ("def",  _DEF_COLS,  "pfr_def"),
]


def _load_one(seasons, stat_type: str, col_spec: list, prefix: str) -> pd.DataFrame:
    raw = nfl.load_pfr_advstats(
        seasons=seasons,
        stat_type=stat_type,
        summary_level="week",
    ).to_pandas()

    rename = {raw_col: alias for raw_col, alias in col_spec if alias}
    raw.rename(columns=rename, inplace=True)

    keep = []
    for raw_col, alias in col_spec:
        final = alias if alias else raw_col
        if final in raw.columns:
            keep.append(final)

    df = raw[keep].copy()
    df["season"] = df["season"].astype(int)
    df["week"]   = df["week"].astype(int)

    df.rename(columns={
        col: f"{prefix}_{col}"
        for col in df.columns
        if col not in _JOIN_KEYS and not col.startswith("pfr_")
    }, inplace=True)

    return df


def load(seasons=None) -> pd.DataFrame:
    """
    Grain   : pfr_player_id × game_id × week
    Columns : join keys (bare) + pfr_pass_* / pfr_rush_* / pfr_rec_* / pfr_def_*
    """
    merge_keys = [
        "game_id", "pfr_game_id", "season", "week", "game_type",
        "team", "opponent", "pfr_player_id", "pfr_player_name",
    ]

    frames = [
        _load_one(seasons, stat_type, col_spec, prefix)
        for stat_type, col_spec, prefix in _STAT_CONFIGS
    ]

    df = frames[0]
    for right in frames[1:]:
        df = df.merge(right, on=merge_keys, how="outer")

    df.sort_values(["season", "week", "pfr_player_id"], ignore_index=True, inplace=True)
    return df