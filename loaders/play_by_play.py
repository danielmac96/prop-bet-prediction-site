"""
loaders/play_by_play.py
========================
Source: nflreadpy.load_pbp()

Aggregates raw play-by-play into one row per player per game across
three roles: passer, rusher, receiver. Roles are outer-joined so
a player appearing in multiple roles (e.g. QB who also rushes) gets
a single merged row.

Key metrics included:
  - EPA, CPOE, xPass, air yards, YAC, red zone usage
  - Scrambles, no-huddle/shotgun snap counts

Excluded:
  - Individual play-level records (too large for Supabase, use raw PBP for model features)
  - Defensive play tracking (use def stats from weekly_player_stats)
  - xyac_median/xyac_success (low signal at game level)
"""

import pandas as pd
import nflreadpy as nfl

# ── Column selection per role ──────────────────────────────────────────────────
_PASSER_COLS = [
    "game_id", "season", "week", "passer_player_id", "posteam",
    "qb_dropback", "qb_scramble",
    "pass_attempt", "complete_pass", "incomplete_pass", "sack",
    "passing_yards", "air_yards", "yards_after_catch",
    "pass_touchdown", "interception",
    "epa", "qb_epa", "air_epa", "yac_epa", "cp", "cpoe",
    "xyac_epa", "xpass", "pass_oe",
    "first_down_pass", "no_huddle", "shotgun", "yardline_100",
]

_RUSHER_COLS = [
    "game_id", "season", "week", "rusher_player_id", "posteam",
    "rush_attempt", "rushing_yards", "rush_touchdown",
    "epa", "xyac_epa",
    "first_down_rush", "tackled_for_loss",
    "yardline_100",
]

_RECEIVER_COLS = [
    "game_id", "season", "week", "receiver_player_id", "posteam",
    "pass_attempt", "complete_pass", "receiving_yards", "pass_touchdown",
    "air_yards", "yards_after_catch",
    "epa", "air_epa", "yac_epa",
    "first_down_pass", "yardline_100",
    "pass_length", "xyac_mean_yardage",
]


def load(seasons: list[int]) -> pd.DataFrame:
    """
    Load and aggregate play-by-play into player-game level rows.

    Args:
        seasons: List of NFL seasons.

    Returns:
        DataFrame with one row per player per game (passer+rusher+receiver merged).
    """
    raw = nfl.load_pbp(seasons=seasons).to_pandas()
    raw["season"] = raw["season"].astype(int)
    raw["week"]   = raw["week"].astype(int)

    passer   = _aggregate_passers(raw)
    rusher   = _aggregate_rushers(raw)
    receiver = _aggregate_receivers(raw)

    merged = (
        passer
        .merge(rusher,   on=["game_id", "gsis_id", "team"], how="outer")
        .merge(receiver, on=["game_id", "gsis_id", "team"], how="outer")
    )

    return merged


# ── Private aggregation helpers ────────────────────────────────────────────────

def _aggregate_passers(pbp: pd.DataFrame) -> pd.DataFrame:
    df = (
        pbp[pbp["pass_attempt"] == 1]
        .dropna(subset=["passer_player_id"])
        [[c for c in _PASSER_COLS if c in pbp.columns]]
        .copy()
    )
    df["in_redzone"] = (df["yardline_100"] <= 20).astype(int)

    agg = df.groupby(["game_id", "passer_player_id", "posteam"]).agg(
        pbp_dropbacks         = ("qb_dropback",     "sum"),
        pbp_scrambles         = ("qb_scramble",      "sum"),
        pbp_pass_attempts     = ("pass_attempt",     "sum"),
        pbp_completions       = ("complete_pass",    "sum"),
        pbp_incompletions     = ("incomplete_pass",  "sum"),
        pbp_sacks             = ("sack",             "sum"),
        pbp_pass_yards        = ("passing_yards",    "sum"),
        pbp_air_yards_thrown  = ("air_yards",        "sum"),
        pbp_yac               = ("yards_after_catch","sum"),
        pbp_pass_tds          = ("pass_touchdown",   "sum"),
        pbp_ints              = ("interception",     "sum"),
        pbp_pass_epa_total    = ("epa",              "sum"),
        pbp_qb_epa_total      = ("qb_epa",           "sum"),
        pbp_air_epa           = ("air_epa",          "sum"),
        pbp_yac_epa           = ("yac_epa",          "sum"),
        pbp_cpoe_mean         = ("cpoe",             "mean"),
        pbp_cp_mean           = ("cp",               "mean"),
        pbp_xyac_epa          = ("xyac_epa",         "sum"),
        pbp_xpass_mean        = ("xpass",            "mean"),
        pbp_pass_oe_mean      = ("pass_oe",          "mean"),
        pbp_first_down_pass   = ("first_down_pass",  "sum"),
        pbp_rz_dropbacks      = ("in_redzone",       "sum"),
        pbp_nohuddle_snaps    = ("no_huddle",        "sum"),
        pbp_shotgun_snaps     = ("shotgun",          "sum"),
    ).reset_index()

    return agg.rename(columns={"passer_player_id": "gsis_id", "posteam": "team"})


def _aggregate_rushers(pbp: pd.DataFrame) -> pd.DataFrame:
    df = (
        pbp[pbp["rush_attempt"] == 1]
        .dropna(subset=["rusher_player_id"])
        [[c for c in _RUSHER_COLS if c in pbp.columns]]
        .copy()
    )
    df["in_redzone"]     = (df["yardline_100"] <= 20).astype(int)
    df["goal_line_rush"] = (df["yardline_100"] <= 5).astype(int)

    agg = df.groupby(["game_id", "rusher_player_id", "posteam"]).agg(
        pbp_carries           = ("rush_attempt",      "sum"),
        pbp_rush_yards        = ("rushing_yards",     "sum"),
        pbp_rush_tds          = ("rush_touchdown",    "sum"),
        pbp_rush_epa_total    = ("epa",               "sum"),
        pbp_xyac_rush_epa     = ("xyac_epa",          "sum"),
        pbp_rush_first_downs  = ("first_down_rush",   "sum"),
        pbp_tfl               = ("tackled_for_loss",  "sum"),
        pbp_rz_carries        = ("in_redzone",        "sum"),
        pbp_goal_line_carries = ("goal_line_rush",    "sum"),
    ).reset_index()

    return agg.rename(columns={"rusher_player_id": "gsis_id", "posteam": "team"})


def _aggregate_receivers(pbp: pd.DataFrame) -> pd.DataFrame:
    df = (
        pbp[pbp["pass_attempt"] == 1]
        .dropna(subset=["receiver_player_id"])
        [[c for c in _RECEIVER_COLS if c in pbp.columns]]
        .copy()
    )
    df["in_redzone"]  = (df["yardline_100"] <= 20).astype(int)
    df["deep_target"] = (
        (df.get("pass_length") == "deep") | (df["air_yards"] >= 15)
    ).fillna(False).astype(int)

    agg = df.groupby(["game_id", "receiver_player_id", "posteam"]).agg(
        pbp_targets          = ("pass_attempt",      "sum"),
        pbp_receptions       = ("complete_pass",     "sum"),
        pbp_rec_yards        = ("receiving_yards",   "sum"),
        pbp_rec_tds          = ("pass_touchdown",    "sum"),
        pbp_rec_air_yards    = ("air_yards",         "sum"),
        pbp_rec_yac          = ("yards_after_catch", "sum"),
        pbp_rec_epa_total    = ("epa",               "sum"),
        pbp_rec_air_epa      = ("air_epa",           "sum"),
        pbp_rec_yac_epa      = ("yac_epa",           "sum"),
        pbp_rec_first_downs  = ("first_down_pass",   "sum"),
        pbp_rz_targets       = ("in_redzone",        "sum"),
        pbp_deep_targets     = ("deep_target",       "sum"),
        pbp_xyac_rec_mean    = ("xyac_mean_yardage", "mean"),
    ).reset_index()

    return agg.rename(columns={"receiver_player_id": "gsis_id", "posteam": "team"})
