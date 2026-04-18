"""
loaders/schedule.py
===================
Source: nflreadpy.load_schedules()

Produces one row per team per game (home + away split).
Includes game context: spread, moneyline, weather, stadium, referee.

Excluded: raw away/home score columns (rolled into team_score / opp_score).
"""

import pandas as pd
import nflreadpy as nfl


# Columns kept from the raw game-level schedule
_GAME_CONTEXT_COLS = [
    "game_id",
    "season", "game_type", "week", "gameday", "gametime",
    "location", "result", "total",
    "spread_line", "total_line",
    "away_moneyline", "home_moneyline",
    "div_game", "roof", "surface", "temp", "wind",
    "stadium",
]


def load(seasons: list[int]) -> pd.DataFrame:
    """
    Load and reshape schedule data into team-level rows.

    Args:
        seasons: List of NFL seasons (e.g. [2024, 2025]).

    Returns:
        DataFrame with one row per team per game.
    """
    raw = nfl.load_schedules(seasons=seasons).to_pandas()
    raw["gameday"] = pd.to_datetime(raw["gameday"], errors="coerce")

    game_ctx = raw[[c for c in _GAME_CONTEXT_COLS if c in raw.columns]].copy()

    # ── Reshape: one row per team ────────────────────────────────────────────
    away = _build_team_side(raw, side="away")
    home = _build_team_side(raw, side="home")

    team_rows = pd.concat([home, away], ignore_index=True)

    sched = team_rows.merge(game_ctx, on="game_id", how="left")

    # Rename spread_line collision (exists in both sides and game_ctx)
    sched = sched.loc[:, ~sched.columns.duplicated()]

    return sched


def _build_team_side(raw: pd.DataFrame, side: str) -> pd.DataFrame:
    opp = "home" if side == "away" else "away"
    df = raw[[
        "game_id",
        f"{side}_team", f"{opp}_team",
        f"{side}_score", f"{opp}_score",
        f"{side}_rest", f"{side}_coach",
        "spread_line",
        f"{side}_moneyline",
    ]].copy()

    df.columns = [
        "game_id", "team", "opponent_team",
        "team_score", "opp_score",
        "rest_days", "team_coach",
        "spread_from_team", "team_moneyline",
    ]
    df["is_home"] = int(side == "home")
    return df
