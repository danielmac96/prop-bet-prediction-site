"""
loaders/formations.py
======================
Source: nflreadpy.load_participation()

Aggregates offensive formation usage per player per game.
Each player on the field for an offensive snap gets a row;
we track what formation they lined up in and pressure metrics.

Included:
  - Shotgun / under center / empty backfield pct
  - Pressure rate, time to throw, defenders in box

Excluded:
  - Individual route data (complex, sparse — reserved for route-tree models)
  - Defensive participation (focus is offensive formations)
  - Raw play_id level detail (use raw participation for advanced models)
"""

import pandas as pd
import nflreadpy as nfl


def load(seasons: list[int]) -> pd.DataFrame:
    """
    Load and aggregate offensive formation data per player per game.

    Args:
        seasons: List of NFL seasons.

    Returns:
        DataFrame with one row per player per game.
    """
    raw = nfl.load_participation(seasons=seasons).to_pandas()
    raw.rename(columns={"nflverse_game_id": "game_id"}, inplace=True)

    # ── Filter to offensive snaps with known participants ────────────────────
    off = raw[[
        "game_id", "play_id", "possession_team", "offense_players",
        "offense_formation", "was_pressure", "time_to_throw", "defenders_in_box",
    ]].dropna(subset=["offense_players"]).copy()

    # ── Explode player list: one row per player per play ─────────────────────
    off = off.assign(gsis_id=off["offense_players"].str.split(";")).explode("gsis_id")
    off["gsis_id"] = off["gsis_id"].str.strip()
    off = off[off["gsis_id"].str.startswith("00-")]   # drop malformed IDs

    # ── Binary formation flags ───────────────────────────────────────────────
    off["is_shotgun"]       = (off["offense_formation"] == "SHOTGUN").astype(float)
    off["is_under_center"]  = (off["offense_formation"] == "UNDER CENTER").astype(float)
    off["is_empty_back"]    = (off["offense_formation"] == "EMPTY").astype(float)
    off["was_pressure_num"] = pd.to_numeric(off["was_pressure"], errors="coerce")
    off["time_to_throw"]    = pd.to_numeric(off["time_to_throw"], errors="coerce")
    off["defenders_in_box"] = pd.to_numeric(off["defenders_in_box"], errors="coerce")

    agg = off.groupby(["game_id", "gsis_id", "possession_team"]).agg(
        form_off_snaps         = ("play_id",          "count"),
        form_shotgun_pct       = ("is_shotgun",        "mean"),
        form_under_center_pct  = ("is_under_center",   "mean"),
        form_empty_back_pct    = ("is_empty_back",     "mean"),
        form_pressure_rate     = ("was_pressure_num",  "mean"),
        form_avg_time_to_throw = ("time_to_throw",     "mean"),
        form_avg_defenders_box = ("defenders_in_box",  "mean"),
    ).reset_index()

    agg.rename(columns={"possession_team": "team"}, inplace=True)

    return agg
