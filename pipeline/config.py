"""
config.py
=========
Single source of truth for all pipeline settings.
Edit SEASONS / CURRENT_WEEK here before any run.
"""

# ── Seasons ────────────────────────────────────────────────────────────────────
HISTORICAL_SEASONS = [] #list(range(2025, 2025))   # backfill range
CURRENT_SEASON     = 2025
CURRENT_WEEK       = 1   # update each week

# ── Table registry ─────────────────────────────────────────────────────────────
# Maps logical name → (supabase_table, conflict_columns, reload_strategy)
# reload_strategy: "weekly" = append new rows | "snapshot" = full replace
TABLES = {
    "schedule": {
        "table":    "sched_final",
        "conflict": ["team", "game_id"],
        "strategy": "weekly",
    },
    "weekly_player_stats": {
        "table":    "weekly_player_data",
        "conflict": ["gsis_id", "game_id"],
        "strategy": "weekly",
    },
    "weekly_team_stats": {
        "table":    "weekly_team_data",
        "conflict": ["team", "game_id"],
        "strategy": "weekly",
    },
    "play_by_play": {
        "table":    "play_by_play",
        "conflict": ["gsis_id", "game_id"],
        "strategy": "weekly",
    },
    "formations": {
        "table":    "play_by_play_formations",
        "conflict": ["gsis_id", "game_id"],
        "strategy": "weekly",
    },
    "snap_counts": {
        "table":    "snap_count",
        "conflict": ["pfr_player_id", "game_id"],
        "strategy": "weekly",
    },
    "nextgen": {
        "table":    "nextgen",
        "conflict": ["gsis_id", "season", "week"],
        "strategy": "weekly",
    },
    "pfr_adv_stats": {
        "table":    "pro_football_ref_adv_stats",
        "conflict": ["pfr_player_id", "game_id"],
        "strategy": "weekly",
    },
    "fantasy_opportunities": {
        "table":    "fantasy_football_opportunities",
        "conflict": ["gsis_id", "game_id"],
        "strategy": "weekly",
    },
    "rosters": {
        "table":    "rosters",
        "conflict": ["gsis_id", "season", "week"],
        "strategy": "weekly",
    },
    # ── Static / snapshot tables (re-pulled fresh each run) ──────────────────
    "player_info": {
        "table":    "player_info",
        "conflict": ["gsis_id"],
        "strategy": "snapshot",
    },
    "depth_chart": {
        "table":    "depth_chart",
        "conflict": ["gsis_id"],
        "strategy": "snapshot",
    },
    "fantasy_ids": {
        "table":    "fantasy_football_ids",
        "conflict": ["pfr_player_id"],
        "strategy": "snapshot",
    },
    "fantasy_rankings": {
        "table":    "fantasy_football_rankings",
        "conflict": ["mergename", "pos", "team", "page_type"],
        "strategy": "snapshot",
    },
}

# ── Upload settings ─────────────────────────────────────────────────────────────
BATCH_SIZE = 500   # rows per Supabase upsert call
MAX_RETRIES = 3    # retry attempts on failed batches
