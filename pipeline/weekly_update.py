"""
pipeline/weekly_update.py
==========================
Weekly cron job. Pulls the just-completed week's data and upserts.
Runs every Tuesday during the NFL season (games end Sunday/Monday).

Usage:
    # Auto-detect week from config:
    python -m pipeline.weekly_update

    # Override week (e.g. to re-run a specific week):
    python -m pipeline.weekly_update --season 2025 --week 4

    # Single feed re-run:
    python -m pipeline.weekly_update --feeds snap_counts pfr_adv_stats

Schedule suggestion (crontab):
    0 10 * * 2 cd /path/to/nfl-pipeline && python -m pipeline.weekly_update >> logs/weekly.log 2>&1

What runs each week:
  - Weekly stat feeds  → filter to current season/week only
  - Snapshot feeds     → always re-upserted (depth chart changes weekly)
  - play_by_play       → heaviest feed; runs last
"""

import argparse
import logging
import sys

from config import CURRENT_SEASON, CURRENT_WEEK, TABLES
from pipeline.initial_load import run_feed   # reuse the same feed router
from utils.upload import upsert
from utils.validation import validate

import loaders.schedule               as schedule_loader
import loaders.weekly_player_stats    as weekly_player_loader
import loaders.weekly_team_stats      as weekly_team_loader
import loaders.play_by_play           as pbp_loader
import loaders.formations             as formations_loader
import loaders.snap_counts            as snap_loader
import loaders.depth_chart            as depth_chart_loader
import loaders.player_info            as player_info_loader
import loaders.rosters                as rosters_loader
import loaders.nextgen                as nextgen_loader
import loaders.pfr_adv_stats          as pfr_loader
import loaders.fantasy_ids            as fantasy_ids_loader
import loaders.fantasy_opportunities  as opps_loader
import loaders.fantasy_rankings       as rankings_loader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ── Feeds to run each week and their filter type ──────────────────────────────
# "season"   → pass [current_season]; loader handles its own week filtering
# "snapshot" → no season arg; always full refresh
_WEEKLY_FEEDS = [
    ("player_info",           "snapshot"),
    ("fantasy_ids",           "snapshot"),
    ("depth_chart",           "snapshot"),
    ("fantasy_rankings",      "snapshot"),
    ("rosters",               "season"),
    ("schedule",              "season"),
    ("weekly_player_stats",   "season"),
    ("weekly_team_stats",     "season"),
    ("snap_counts",           "season"),
    ("pfr_adv_stats",         "snapshot"),  # load_pfr_advstats has no season filter
    ("nextgen",               "snapshot"),  # same
    ("fantasy_opportunities", "season"),
    ("play_by_play",          "season"),    # heaviest — run last
    ("formations",            "season"),
]


def _load_for_week(feed_name: str, season: int, week: int):
    """
    Load a single week of data.
    Most loaders accept a seasons list; we post-filter to the target week.
    """
    match feed_name:
        case "schedule":
            df = schedule_loader.load([season])
            return df[df["week"] == week] if "week" in df.columns else df

        case "weekly_player_stats":
            df = weekly_player_loader.load([season])
            return df[df["week"] == week]

        case "weekly_team_stats":
            df = weekly_team_loader.load([season])
            return df[df["week"] == week]

        case "play_by_play":
            df = pbp_loader.load([season])
            # PBP is keyed on game_id, not week — filter via schedule join not needed
            # since game_id encodes the week; return all season data and rely on upsert
            return df

        case "formations":
            df = formations_loader.load([season])
            return df

        case "snap_counts":
            df = snap_loader.load([season])
            return df[df["week"] == week] if "week" in df.columns else df

        case "rosters":
            df = rosters_loader.load([season])
            return df[df["week"] == week]

        case "fantasy_opportunities":
            df = opps_loader.load([season])
            return df[df["week"] == week] if "week" in df.columns else df

        # ── Snapshot feeds ───────────────────────────────────────────────────
        case "player_info":      return player_info_loader.load()
        case "depth_chart":      return depth_chart_loader.load()
        case "nextgen":          return nextgen_loader.load()
        case "pfr_adv_stats":    return pfr_loader.load()
        case "fantasy_ids":      return fantasy_ids_loader.load()
        case "fantasy_rankings": return rankings_loader.load()

        case _:
            raise ValueError(f"Unknown feed: '{feed_name}'")


def run_weekly(season: int, week: int, feeds: list[str] | None = None) -> None:
    logger.info(f"━━━ Weekly Update | Season {season} | Week {week} ━━━")

    feeds_to_run = feeds if feeds else [f for f, _ in _WEEKLY_FEEDS]
    feed_type_map = dict(_WEEKLY_FEEDS)

    failed = []
    for feed_name in feeds_to_run:
        cfg      = TABLES[feed_name]
        table    = cfg["table"]
        conflict = cfg["conflict"]

        logger.info(f"[{feed_name}] Loading...")
        try:
            df = _load_for_week(feed_name, season, week)
            df = validate(df, feed_name)
            upsert(df, table, conflict)
        except Exception as e:
            logger.error(f"[{feed_name}] FAILED: {e}")
            failed.append(feed_name)

    if failed:
        logger.error(f"Weekly update completed with failures: {failed}")
        sys.exit(1)
    else:
        logger.info(f"✓ Week {week} update complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="NFL weekly data update.")
    parser.add_argument("--season", type=int, default=CURRENT_SEASON)
    parser.add_argument("--week",   type=int, default=CURRENT_WEEK)
    parser.add_argument(
        "--feeds", nargs="*", default=None,
        help="Subset of feeds to run. Default: all weekly feeds."
    )
    args = parser.parse_args()

    run_weekly(season=args.season, week=args.week, feeds=args.feeds)


if __name__ == "__main__":
    main()
