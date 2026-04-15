"""
loaders/pfr_adv_stats.py
=========================
Source: nflreadpy.load_pfr_advstats()

Advanced stats from Pro Football Reference. One row per pfr_player_id per game.
Wide join across four role-specific frames; most cells are NULL for any given player.

Column prefixes by frame:
  pfr_pass_* — QB accuracy, bad throws, pressure absorbed
  pfr_rush_* — yards before/after contact, broken tackles
  pfr_rec_*  — receiver drops, broken tackles, int, passer rating when targeted
  pfr_def_*  — coverage metrics, pass rush, tackling

Note: all cross-system joins go through fantasy_football_ids (pfr_id → gsis_id).
"""

import pandas as pd
import nflreadpy as nfl

_JOIN_KEYS = {
    "game_id", "pfr_game_id", "season", "week", "game_type",
    "team", "opponent", "pfr_player_id", "pfr_player_name",
}

_PASS_COLS = [
    ("game_id",               None),
    ("pfr_game_id",           None),
    ("season",                None),
    ("week",                  None),
    ("game_type",             None),
    ("team",                  None),
    ("opponent",              None),
    ("pfr_player_name",       None),
    ("pfr_player_id",         None),
    # QB accuracy / decision-making
    ("passing_drops",         "pfr_pass_drops"),
    ("passing_drop_pct",      "pfr_pass_drop_pct"),
    ("passing_bad_throws",    "pfr_pass_bad_throws"),
    ("passing_bad_throw_pct", "pfr_pass_bad_throw_pct"),
    # pressure absorbed by QB
    ("times_sacked",          "pfr_pass_times_sacked"),
    ("times_blitzed",         "pfr_pass_times_blitzed"),
    ("times_hurried",         "pfr_pass_times_hurried"),
    ("times_hit",             "pfr_pass_times_hit"),
    ("times_pressured",       "pfr_pass_times_pressured"),
    ("times_pressured_pct",   "pfr_pass_times_pressured_pct"),
    # defensive pressure generated (same play, defender perspective)
    ("def_times_blitzed",     "pfr_pass_def_blitzed"),
    ("def_times_hurried",     "pfr_pass_def_hurried"),
    ("def_times_hitqb",       "pfr_pass_def_hitqb"),
]

_RUSH_COLS = [
    ("game_id",                             None),
    ("pfr_game_id",                         None),
    ("season",                              None),
    ("week",                                None),
    ("game_type",                           None),
    ("team",                                None),
    ("opponent",                            None),
    ("pfr_player_name",                     None),
    ("pfr_player_id",                       None),
    ("carries",                             "pfr_rush_carries"),
    ("rushing_yards_before_contact",        "pfr_rush_ybc"),
    ("rushing_yards_before_contact_avg",    "pfr_rush_ybc_avg"),
    ("rushing_yards_after_contact",         "pfr_rush_yac"),
    ("rushing_yards_after_contact_avg",     "pfr_rush_yac_avg"),
    ("rushing_broken_tackles",              "pfr_rush_broken_tackles"),
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
    ("receiving_broken_tackles", "pfr_rec_broken_tackles"),
    ("receiving_drop",           "pfr_rec_drops"),
    ("receiving_drop_pct",       "pfr_rec_drop_pct"),
    ("receiving_int",            "pfr_rec_ints"),
    ("receiving_rat",            "pfr_rec_passer_rating"),
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
    # coverage
    ("def_ints",                   "pfr_def_ints"),
    ("def_targets",                "pfr_def_targets"),
    ("def_completions_allowed",    "pfr_def_completions_allowed"),
    ("def_completion_pct",         "pfr_def_completion_pct"),
    ("def_yards_allowed",          "pfr_def_yards_allowed"),
    ("def_yards_allowed_per_cmp",  "pfr_def_yards_per_cmp"),
    ("def_yards_allowed_per_tgt",  "pfr_def_yards_per_tgt"),
    ("def_receiving_td_allowed",   "pfr_def_tds_allowed"),
    ("def_passer_rating_allowed",  "pfr_def_passer_rating"),
    ("def_adot",                   "pfr_def_adot"),
    ("def_air_yards_completed",    "pfr_def_air_yards_completed"),
    ("def_yards_after_catch",      "pfr_def_yac"),
    # pass rush
    ("def_times_blitzed",          "pfr_def_times_blitzed"),
    ("def_times_hurried",          "pfr_def_times_hurried"),
    ("def_times_hitqb",            "pfr_def_times_hitqb"),
    ("def_sacks",                  "pfr_def_sacks"),
    ("def_pressures",              "pfr_def_pressures"),
    # tackling
    ("def_tackles_combined",       "pfr_def_tackles_combined"),
    ("def_missed_tackles",         "pfr_def_missed_tackles"),
    ("def_missed_tackle_pct",      "pfr_def_missed_tackle_pct"),
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

    # Auto-prefix any remaining metric cols that weren't given explicit pfr_* aliases
    df.rename(columns={
        col: f"{prefix}_{col}"
        for col in df.columns
        if col not in _JOIN_KEYS and not col.startswith("pfr_")
    }, inplace=True)

    return df


def load(seasons=None) -> pd.DataFrame:
    """
    Grain   : pfr_player_id × game_id
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
