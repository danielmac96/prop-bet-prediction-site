# CLAUDE.md — prop-bet-prediction-site

Developer reference for the NFL data pipeline. Covers what each data script does, how the datasets relate, and how to debug before pushing to Supabase.

---

## What This Is

A Python ETL pipeline. It pulls NFL data via `nflreadpy`, transforms it in `loaders/`, validates it, and upserts to Supabase (PostgreSQL). No web server, no API — pure data pipeline with two entry points:

- `python -m pipeline.initial_load` — one-time historical backfill (2016 → present, ~30–45 min)
- `python -m pipeline.weekly_update` — weekly cron job (Tuesdays after games)

---

## Quick Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in SUPABASE_URL and SUPABASE_KEY
```

**Before pushing to Supabase**, sample and inspect the data locally:

```bash
# Pull 25 rows from every loader → schema_samples/
python sample_schemas.py --season 2025

# Full 1000-row dump + terminal inspection report
python inspect_tables.py --generate --season 2025

# Inspect specific tables only
python inspect_tables.py --generate --season 2025 --tables weekly_player_data snap_count
```

---

## Data Scripts — What Each One Does

Datasets are split into two strategies:

- **Weekly** — append/upsert each run (new rows per game)
- **Snapshot** — full replace each run (latest state wins)

---

### Foundation Tables *(snapshot)*

These are static lookup tables. Run first. Everything else joins through them.

---

#### `loaders/player_info.py` → `player_info`
**One row per player (`gsis_id`)**

Bio data: height, weight, college, draft round/pick, position mappings across systems (NGS, PFF). The `pfr_id` column here bridges to `snap_count` and `pfr_adv_stats` if you don't want to go through `fantasy_ids`.

**Joins to:** every table via `gsis_id`
**Model use:** position group filtering, physical attributes as baseline features

---

#### `loaders/fantasy_ids.py` → `fantasy_football_ids`
**One row per `pfr_player_id`**

The critical ID bridge table. `snap_count` and `pfr_adv_stats` use `pfr_player_id` as their key (sourced from Pro Football Reference), not `gsis_id`. This table maps `pfr_player_id → gsis_id` so you can join those feeds to everything else.

```sql
-- Always join snap_count through here
SELECT s.*, i.gsis_id
FROM snap_count s
JOIN fantasy_football_ids i ON s.pfr_player_id = i.pfr_player_id;
```

**Joins to:** `snap_count`, `pro_football_ref_adv_stats`
**Model use:** not a model feature — purely a join key resolver

---

#### `loaders/depth_chart.py` → `depth_chart`
**One row per player — latest depth chart date only**

Current depth chart position and ranking. Re-fetched every run (charts change weekly). Only the most recent date is kept; historical depth chart data is not stored.

**Joins to:** `player_info` via `gsis_id`
**Model use:** position rank as a proxy for role security; useful for filtering out backups

---

### Game Context *(weekly)*

---

#### `loaders/schedule.py` → `sched_final`
**One row per team per game (`team + game_id`)**

Reshaped from a game-level record into two team-level rows (home + away). Contains game context that isn't in the player stat feeds: spread, moneyline, rest days, coach, weather (temp/wind), roof type, surface, referee, and stadium.

**Joins to:** all weekly tables via `game_id`; filtered to team with `team` column
**Model use:** spread and moneyline encode market expectations; rest days and weather affect scoring environments; home/away splits for travel fatigue

---

### Core Stat Tables *(weekly)*

---

#### `loaders/weekly_player_stats.py` → `weekly_player_data`
**One row per player per game (`gsis_id + game_id`)**

The primary output table. Standard box score stats for QB/RB/WR/TE/DB/DL/LB. Kickers excluded. Adds a computed `total_tds` column (passing + rushing + receiving TDs combined). Fantasy points columns are intentionally dropped — compute those in the model layer.

**Joins to:** `sched_final` via `game_id + team`, all enrichment tables via `gsis_id + game_id`
**Model use:** contains all prediction targets — `passing_yards`, `rushing_yards`, `receiving_yards`, `total_tds`, `receptions`

---

#### `loaders/weekly_team_stats.py` → `weekly_team_data`
**One row per team per game (`team + game_id`)**

Aggregated offensive and defensive team-level stats. Granular FG distance breakdowns are dropped (noise). Use this for game script context: pass rate, total yards, pace.

**Joins to:** `weekly_player_data` via `game_id + team`; `sched_final` via same
**Model use:** team pass rate and total offensive efficiency are strong predictors for individual player targets and carries

---

#### `loaders/rosters.py` → `rosters`
**One row per player per week (`gsis_id + season + week`)**

Weekly roster status: active, IR, practice squad, etc. Deduped to keep the latest entry per player per week (captures mid-week transactions). Useful for filtering out injured or inactive players before prediction.

**Joins to:** `weekly_player_data` via `gsis_id + season + week`
**Model use:** status filtering is essential — never predict for players on IR or inactive

---

### Play-Level Enrichment *(weekly)*

These feeds add efficiency and usage context that box scores don't capture.

---

#### `loaders/snap_counts.py` → `snap_count`
**One row per player per game (`pfr_player_id + game_id`)**

Offense, defense, and special teams snap counts and percentages from Pro Football Reference. Uses `pfr_player_id` not `gsis_id` — always join through `fantasy_football_ids`.

**Joins to:** `fantasy_football_ids` via `pfr_player_id` → then to everything else via `gsis_id`
**Model use:** `offense_pct` is the single strongest usage signal for skill position players — more reliable than target share alone

---

#### `loaders/play_by_play.py` → `play_by_play`
**One row per player per game (`gsis_id + game_id`)**

Raw PBP data aggregated to player-game level across three roles: passer, rusher, receiver. A QB who also rushes gets one merged row with both passing and rushing columns populated. Columns prefixed `pbp_`.

Key metrics: EPA totals, CPOE, xPass, air yards thrown vs caught, YAC, red zone dropbacks/carries/targets, shotgun snaps, sacks.

**Joins to:** `weekly_player_data` via `gsis_id + game_id`
**Model use:** EPA and CPOE are better efficiency signals than raw yards; red zone usage predicts TD variance; shotgun snaps cross-check with `formations`

---

#### `loaders/formations.py` → `play_by_play_formations`
**One row per player per game (`gsis_id + game_id`)**

Offensive formation usage per player per game, aggregated from participation data. Every offensive player on the field gets counted. Columns prefixed `form_`.

Key metrics: `form_shotgun_pct`, `form_under_center_pct`, `form_empty_back_pct`, `form_pressure_rate`, `form_avg_time_to_throw`, `form_avg_defenders_box`.

**Joins to:** `weekly_player_data` via `gsis_id + game_id`; consistent with `play_by_play` shotgun snap counts
**Model use:** shotgun rate predicts passing volume and WR/TE opportunity; defenders in box predicts run/pass split; pressure rate affects QB accuracy

---

### Advanced Metrics *(weekly)*

Higher-signal features for the prediction model.

---

#### `loaders/nextgen.py` → `nextgen`
**One row per player per week (`gsis_id + season + week`)**

AWS Next Gen Stats tracking data. Three stat types (passing, rushing, receiving) merged into one wide row per player per week. Columns prefixed `ng_pass_`, `ng_rush_`, `ng_rec_`.

Key metrics by role:
- **QB:** time to throw, completion % above expectation (CPAE), aggressiveness, intended air yards
- **RB:** rush yards over expected (RYOE), efficiency, time to line of scrimmage
- **WR/TE:** cushion, separation, intended air yards share, YAC above expectation

Note: grain is week-level (not game-level) — joins on `gsis_id + season + week`, not `game_id`.

**Joins to:** `weekly_player_data` via `gsis_id + season + week`
**Model use:** CPAE and separation are strong independent predictors; RYOE separates scheme from player; intended air yards share shows target quality

---

#### `loaders/pfr_adv_stats.py` → `pro_football_ref_adv_stats`
**One row per player per game (`pfr_player_id + game_id`)**

Pro Football Reference advanced stats across four groups (pass, rush, rec, def), merged into one wide row. Columns prefixed `pfr_pass_`, `pfr_rush_`, `pfr_rec_`, `pfr_def_`. Uses `pfr_player_id` — join through `fantasy_football_ids` to reach `gsis_id`.

Key metrics:
- **QB:** pressure rate, bad throw %, drop %, times blitzed/hurried/hit
- **RB:** yards before/after contact, broken tackles
- **WR/TE:** drops, passer rating when targeted, broken tackles after catch
- **DEF:** coverage metrics, missed tackle %, pressure contributions

**Joins to:** `fantasy_football_ids` via `pfr_player_id`; then `weekly_player_data` via `gsis_id + game_id`
**Model use:** pressure rate is essential for QB projections; passer rating when targeted identifies favorable matchups; broken tackles explain YAC variance

---

#### `loaders/fantasy_opportunities.py` → `fantasy_football_opportunities`
**One row per player per game (`gsis_id + game_id`)**

Vegas-calibrated opportunity model — expected vs actual fantasy points for every player every game. All stat columns prefixed `opps_`. The `opps_*_fantasy_points_exp` columns are the model's pre-game opportunity estimate; `opps_*_diff` shows how much a player over/underperformed.

**Joins to:** `weekly_player_data` via `gsis_id + game_id`
**Model use:** the most directly actionable pre-game feature — expected points encode matchup quality, game script, and usage all at once; tracking diff over time identifies systematic over/underperformers

---

### Market Signals *(snapshot)*

---

#### `loaders/fantasy_rankings.py` → `fantasy_football_rankings`
**One row per player per position per team per page_type**

FantasyPros expert consensus rankings (ECR). The `page_type` column distinguishes contexts:
- `"weekly"` — start/sit rankings for the current week ← use this for betting models
- `"ros"` — rest-of-season outlook
- `"draft"` — pre-season ADP

Columns prefixed `rank_`. Includes ECR rank, standard deviation (consensus spread), best/worst case, week-over-week delta, and platform ownership rates.

No `gsis_id` — joins via `mergename + pos + team` (fuzzy). Use only for market signal features, not as a primary join key.

**Joins to:** loosely to `weekly_player_data` via name/team matching
**Model use:** rank delta (week-over-week movement) as momentum signal; ownership rates signal public bias; ECR consensus as a baseline to bet against or with

---

## How Tables Fit Together

```
player_info ──────────────────────────────────────┐
fantasy_football_ids (pfr_player_id → gsis_id) ───┤
depth_chart ───────────────────────────────────────┤
                                                    │ gsis_id
rosters ──────────── gsis_id + season + week       │
                                                    ▼
sched_final ──────── team + game_id ──────► weekly_player_data  (core target table)
weekly_team_data ─── team + game_id ──────►        │
                                                    │ gsis_id + game_id
play_by_play ────────────────────────────────────── ┤
play_by_play_formations ─────────────────────────── ┤
fantasy_football_opportunities ──────────────────── ┘

snap_count ─── pfr_player_id + game_id ── (bridge via fantasy_football_ids)
pro_football_ref_adv_stats ── pfr_player_id + game_id ── (bridge via fantasy_football_ids)

nextgen ────── gsis_id + season + week  (no game_id — week-level grain)
```

---

## Debugging Before Supabase

**Step 1 — Sample the data locally**
```bash
python sample_schemas.py --season 2025 --rows 25
# Writes schema_samples/<table>.csv + schema_overview.csv + column_overlap.csv
```

**Step 2 — Inspect for data quality issues**
```bash
python inspect_tables.py --generate --season 2025
# Checks: duplicate conflict keys, null%, dtype, numeric ranges
```

**Step 3 — Run a specific feed end-to-end**
```bash
python -m pipeline.weekly_update --season 2025 --week 4 --feeds snap_counts
```

**Common issues:**

| Problem | Cause | Fix |
|---|---|---|
| Upsert fails with column error | Loader added a column not in `schema.sql` | `ALTER TABLE` in Supabase or update `schema.sql` |
| Duplicate key violation | Conflict columns don't uniquely identify rows | Check `dedup_cols` in `utils/validation.py` |
| `snap_count` won't join | Uses `pfr_player_id`, not `gsis_id` | Bridge through `fantasy_football_ids` |
| `nextgen` won't join on `game_id` | Its grain is week-level, not game-level | Join on `gsis_id + season + week` instead |
| Loader returns empty DataFrame | nflreadpy season filter too narrow | Check `CURRENT_SEASON` and `CURRENT_WEEK` in `pipeline/config.py` |
| EnvironmentError on import | `.env` missing or incomplete | Verify `SUPABASE_URL` and `SUPABASE_KEY` are set |

---

## Configuration

Edit `pipeline/config.py` before each run:

```python
CURRENT_SEASON = 2025
CURRENT_WEEK   = 4     # bump every Tuesday after games complete

HISTORICAL_SEASONS = []  # set to list(range(2016, 2025)) for backfill
```

**Upload defaults:** `BATCH_SIZE = 500` rows per upsert, `MAX_RETRIES = 3` with exponential backoff.

---

## Adding a New Feed

1. Create `loaders/my_feed.py` — return a clean `pd.DataFrame` from `load(seasons)` or `load()`
2. Register it in `pipeline/config.py` under `TABLES` with table name, conflict columns, and strategy
3. Add a dispatch case in both `pipeline/initial_load.py` and `pipeline/weekly_update.py`
4. Add validation rules in `utils/validation.py`
5. Add the table + UNIQUE constraint to `sql/schema.sql`

If the Supabase table doesn't have a column that the loader returns, the upsert will fail. Always run `sample_schemas.py` and check `schema_overview.csv` before the first upload.
