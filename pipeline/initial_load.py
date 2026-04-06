"""
pipeline/initial_load.py
=========================
One-time historical backfill. Run once to populate Supabase with
all seasons defined in config.HISTORICAL_SEASONS.

Usage:
    python -m pipeline.initial_load

    # Or a single feed only:
    python -m pipeline.initial_load --feeds schedule weekly_player_stats

Feeds that are season-agnostic (player_info, depth_chart, fantasy_ids,
fantasy_rankings, nextgen, pfr_adv_stats, snap_counts) are loaded in
full regardless of season filter.

Estimated runtime: 20–45 min depending on season range and Supabase tier.
"""

import argparse
import logging
import sys

from pipeline.config import HISTORICAL_SEASONS, CURRENT_SEASON, TABLES
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

ALL_SEASONS = HISTORICAL_SEASONS + [CURRENT_SEASON]


def run_feed(feed_name: str, seasons: list[int]) -> None:
    """Load, validate, and upsert a single feed."""
    cfg = TABLES[feed_name]
    table    = cfg["table"]
    conflict = cfg["conflict"]

    logger.info(f"━━━ [{feed_name}] Starting ━━━")

    try:
        df = _load(feed_name, seasons)
        df = validate(df, feed_name)
        upsert(df, table, conflict)
    except Exception as e:
        logger.error(f"[{feed_name}] FAILED: {e}")
        raise


def _load(feed_name: str, seasons: list[int]):
    """Route to the correct loader function."""
    match feed_name:
        # ── Weekly / seasonal feeds ───────────────────────────────────────────
        case "schedule":
            return schedule_loader.load(seasons)
        case "weekly_player_stats":
            return weekly_player_loader.load(seasons)
        case "weekly_team_stats":
            return weekly_team_loader.load(seasons)
        case "play_by_play":
            return pbp_loader.load(seasons)
        case "formations":
            return formations_loader.load(seasons)
        case "snap_counts":
            return snap_loader.load(seasons)
        case "rosters":
            return rosters_loader.load(seasons)
        case "fantasy_opportunities":
            return opps_loader.load(seasons)
        case "nextgen":
            return nextgen_loader.load(seasons)

        # ── Snapshot feeds (season param ignored) ────────────────────────────
        case "player_info":
            return player_info_loader.load()
        case "depth_chart":
            return depth_chart_loader.load()
        case "pfr_adv_stats":
            return pfr_loader.load()
        case "fantasy_ids":
            return fantasy_ids_loader.load()
        case "fantasy_rankings":
            return rankings_loader.load()

        case _:
            raise ValueError(f"Unknown feed: '{feed_name}'")


# ── Feed execution order ───────────────────────────────────────────────────────
# Static reference tables first, then weekly stat feeds.
# play_by_play and formations are large — run last.
_DEFAULT_ORDER = [
    "player_info",
    "fantasy_ids",
    # "depth_chart",
    "rosters",
    "schedule",
    "weekly_player_stats",
    "weekly_team_stats",
    "snap_counts",
    "pfr_adv_stats",
    "nextgen",
    "fantasy_opportunities",
    "fantasy_rankings",
    "play_by_play",
    "formations",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="NFL data initial historical backfill.")
    parser.add_argument(
        "--feeds", nargs="*", default=None,
        help="Feed names to run (default: all). Example: --feeds schedule weekly_player_stats"
    )
    args = parser.parse_args()

    feeds_to_run = args.feeds if args.feeds else _DEFAULT_ORDER
    invalid = [f for f in feeds_to_run if f not in TABLES]
    if invalid:
        logger.error(f"Unknown feeds: {invalid}. Valid options: {list(TABLES.keys())}")
        sys.exit(1)

    logger.info(f"Starting initial load | Seasons: {ALL_SEASONS} | Feeds: {feeds_to_run}")

    failed = []
    for feed in feeds_to_run:
        try:
            run_feed(feed, ALL_SEASONS)
        except Exception:
            failed.append(feed)
            logger.error(f"[{feed}] Skipping after failure — continuing with remaining feeds.")

    if failed:
        logger.error(f"Initial load completed with failures: {failed}")
        sys.exit(1)
    else:
        logger.info("✓ Initial load complete — all feeds succeeded.")


if __name__ == "__main__":
    main()
