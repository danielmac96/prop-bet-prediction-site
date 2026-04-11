# CLAUDE.md — prop-bet-prediction-site

AI assistant guide for the NFL data pipeline. Read this before making any changes.

---

## Project Overview

Production-grade NFL data pipeline that fetches weekly player/team statistics and loads them into Supabase (PostgreSQL). The data feeds a sportsbook prop-bet prediction model.

**Core flow:**
1. Pull raw NFL data via `nflreadpy` library
2. Transform/clean in loader modules (`loaders/`)
3. Validate per-table rules (`utils/validation.py`)
4. Upsert to Supabase in batches (`utils/upload.py`)

There is **no web server, no API, no frontend**. This is a pure data pipeline with two entry points: a one-time historical backfill and a weekly cron job.

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.10+ |
| Database | Supabase (PostgreSQL) |
| Data source | `nflreadpy` (NFL data API client) |
| Data manipulation | `pandas`, `numpy`, `pyarrow` |
| DB client | `supabase` Python SDK |
| Config management | `python-dotenv` |

---

## Repository Structure

```
prop-bet-prediction-site/
├── .env                          # SUPABASE_URL + SUPABASE_KEY (never commit)
├── requirements.txt              # pip dependencies
├── run_initial_load.py           # thin wrapper calling pipeline.initial_load.main()
├── inspect_tables.py             # dev utility: generate CSV samples + inspect
│
├── db/
│   └── client.py                 # Supabase singleton (reads env on import)
│
├── loaders/                      # One file per data feed — pure transforms only
│   ├── schedule.py
│   ├── weekly_player_stats.py
│   ├── weekly_team_stats.py
│   ├── play_by_play.py
│   ├── formations.py
│   ├── snap_counts.py
│   ├── depth_chart.py
│   ├── player_info.py
│   ├── rosters.py
│   ├── nextgen.py
│   ├── pfr_adv_stats.py
│   ├── fantasy_ids.py
│   ├── fantasy_opportunities.py
│   └── fantasy_rankings.py
│
├── pipeline/
│   ├── config.py                 # CURRENT_SEASON, CURRENT_WEEK, TABLES registry
│   ├── initial_load.py           # One-time historical backfill (all seasons)
│   └── weekly_update.py          # Weekly cron job (Tuesdays)
│
├── sql/
│   └── schema.sql                # CREATE TABLE + indexes — run once in Supabase
│
└── utils/
    ├── upload.py                 # Generic upsert: NaN→None, batching, retry
    └── validation.py             # Per-table sanity checks (required cols, dupes, ranges)
```

---

## Environment Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env with real credentials
```

**Required `.env` variables:**

```
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_KEY=<supabase-publishable-key>
```

`db/client.py` raises `EnvironmentError` at import time if either variable is missing.

---

## Running the Pipeline

### Initial backfill (run once)

Pulls 2016 → present. Takes ~30–45 minutes.

```bash
# All feeds
python -m pipeline.initial_load

# Specific feeds only
python -m pipeline.initial_load --feeds schedule weekly_player_stats play_by_play
```

### Weekly update (every Tuesday)

```bash
# Use CURRENT_SEASON + CURRENT_WEEK from pipeline/config.py
python -m pipeline.weekly_update

# Override season/week
python -m pipeline.weekly_update --season 2025 --week 4

# Re-run specific feeds
python -m pipeline.weekly_update --feeds snap_counts pfr_adv_stats
```

### Crontab (10am ET every Tuesday)

```
0 10 * * 2 cd /path/to/nfl-pipeline && /path/to/.venv/bin/python -m pipeline.weekly_update >> logs/weekly.log 2>&1
```

### Dev utilities

```bash
# Generate CSV samples for a season (writes to samples/)
python inspect_tables.py --generate --season 2025

# Inspect existing CSV samples (columns, nulls, dtypes, dupes, numeric summary)
python inspect_tables.py --rows 10
```

---

## Configuration (`pipeline/config.py`)

**Update these before each season or weekly run:**

```python
CURRENT_SEASON = 2025
CURRENT_WEEK   = 4    # ← bump every Tuesday after games complete
```

**`TABLES` registry** maps logical feed names to Supabase config:

```python
TABLES = {
    "schedule": {
        "table":    "sched_final",      # Supabase table name
        "conflict": ["team", "game_id"],# Upsert conflict key columns
        "strategy": "weekly",           # "weekly" or "snapshot"
    },
    ...
}
```

- `"weekly"` strategy: append/upsert new rows each run
- `"snapshot"` strategy: full replace each run (player_info, depth_chart, fantasy_ids, fantasy_rankings)

**Upload settings:**
- `BATCH_SIZE = 500` — rows per Supabase upsert call
- `MAX_RETRIES = 3` — retry attempts on failed batches

---

## Architecture: Data Flow

```
nflreadpy API
      │
      ▼
loaders/<feed>.py         # fetch + transform → returns pd.DataFrame
      │
      ▼
utils/validation.py       # per-table QA: required cols, no dupes, range checks
      │
      ▼
utils/upload.py           # NaN→None, batch 500 rows, upsert with retry
      │
      ▼
Supabase (PostgreSQL)
```

### Loader interface contract

Every loader exposes a single public function:

- **Weekly feeds**: `load(seasons: list[int]) -> pd.DataFrame`
- **Snapshot feeds**: `load() -> pd.DataFrame` (no season argument)

Loaders do **only** transformation — no DB calls, no side effects. They must return a clean `pd.DataFrame` ready for validation and upsert.

---

## Loader Conventions

1. **One file per feed** in `loaders/`. Do not combine unrelated feeds.
2. **No DB calls inside loaders.** All Supabase interaction goes through `utils/upload.py`.
3. **Column naming**: use `snake_case`. Prefix columns by source when needed (e.g., `pbp_`, `ng_`, `opps_`, `pfr_`).
4. **Drop unused columns early.** Prefer explicit `keep_cols` lists or drop irrelevant columns near the top of the transform.
5. **Deduplication**: when a feed may return duplicate rows, deduplicate explicitly (keep last) before returning.
6. **NaN handling**: `utils/upload.py` converts all `NaN` to `None` automatically — loaders don't need to do this.
7. **Type coercion**: don't force dtypes unnecessarily; let pandas infer. IDs (gsis_id, game_id) must remain `TEXT`/`str` — never cast to int.

---

## Adding a New Feed

Follow these four steps in order:

1. **Create `loaders/my_feed.py`** with a `load(seasons)` or `load()` function returning a `pd.DataFrame`.

2. **Register in `pipeline/config.py`**:
   ```python
   "my_feed": {
       "table":    "my_supabase_table",
       "conflict": ["primary_key_col"],
       "strategy": "weekly",  # or "snapshot"
   },
   ```

3. **Add a `case` in both `pipeline/initial_load.py` and `pipeline/weekly_update.py`**:
   ```python
   elif name == "my_feed":
       df = my_feed.load(seasons)
   ```

4. **Add validation rules in `utils/validation.py`** — at minimum specify required columns and the no-null key columns.

5. **Add the Supabase table in `sql/schema.sql`** with the appropriate composite UNIQUE constraint matching your conflict columns.

---

## Database Schema Conventions

- **All ID columns** (gsis_id, game_id, pfr_player_id) are `TEXT` — never cast to integer.
- **Float columns** use `DOUBLE PRECISION` to match pandas `float64`.
- **Every table** has a composite `UNIQUE` constraint on the same columns listed in `TABLES[feed]["conflict"]`.
- **No foreign key constraints** in the DB — referential integrity is enforced at the pipeline layer.
- **Indexes** are defined on common join/filter columns: gsis_id, game_id, pfr_player_id, team+season.
- Schema is initialized once by pasting `sql/schema.sql` into the Supabase SQL editor.

---

## Key Table Reference

| Supabase Table | Conflict Key | Strategy | Feed Name |
|---|---|---|---|
| `player_info` | `gsis_id` | snapshot | `player_info` |
| `depth_chart` | `gsis_id` | snapshot | `depth_chart` |
| `fantasy_football_ids` | `pfr_player_id` | snapshot | `fantasy_ids` |
| `fantasy_football_rankings` | `mergename, pos, team, page_type` | snapshot | `fantasy_rankings` |
| `rosters` | `gsis_id, season, week` | weekly | `rosters` |
| `sched_final` | `team, game_id` | weekly | `schedule` |
| `weekly_player_data` | `gsis_id, game_id` | weekly | `weekly_player_stats` |
| `weekly_team_data` | `team, game_id` | weekly | `weekly_team_stats` |
| `snap_count` | `pfr_player_id, game_id` | weekly | `snap_counts` |
| `nextgen` | `gsis_id, season, week` | weekly | `nextgen` |
| `pro_football_ref_adv_stats` | `pfr_player_id, game_id` | weekly | `pfr_adv_stats` |
| `fantasy_football_opportunities` | `gsis_id, game_id` | weekly | `fantasy_opportunities` |
| `play_by_play` | `gsis_id, game_id` | weekly | `play_by_play` |
| `play_by_play_formations` | `gsis_id, game_id` | weekly | `formations` |

---

## Key Joins

`snap_count` uses `pfr_player_id` as its primary key. Bridge to `gsis_id` via `fantasy_football_ids`:

```sql
SELECT s.*, i.gsis_id
FROM snap_count s
JOIN fantasy_football_ids i ON s.pfr_player_id = i.pfr_player_id;
```

Full player game profile:

```sql
SELECT p.*, w.*, sc.offense_pct, f.form_shotgun_pct
FROM weekly_player_data p
JOIN sched_final sc_g ON p.game_id = sc_g.game_id AND p.team = sc_g.team
JOIN weekly_team_data w ON p.game_id = w.game_id AND p.team = w.team
JOIN snap_count sc ON p.game_id = sc.game_id
JOIN fantasy_football_ids fi ON sc.pfr_player_id = fi.pfr_player_id AND fi.gsis_id = p.gsis_id
JOIN play_by_play_formations f ON p.game_id = f.game_id AND p.gsis_id = f.gsis_id;
```

---

## Upsert / Upload Behavior (`utils/upload.py`)

- All `NaN` values are converted to `None` (PostgreSQL `NULL`) before upload.
- Rows are sent in batches of `BATCH_SIZE` (500).
- On failure, retries up to `MAX_RETRIES` (3) with exponential backoff (`2^attempt` seconds).
- Uses Supabase `upsert(..., on_conflict=<conflict_cols>)` — existing rows are updated, new rows are inserted.

---

## Validation Rules (`utils/validation.py`)

Per-table rules in `TABLE_RULES` dict. Each entry can specify:
- `required_cols`: columns that must exist in the DataFrame
- `no_null_cols`: columns where nulls trigger a critical error
- `dedup_cols`: deduplicate on these columns (keep last)
- `range_checks`: `{col: (min, max)}` for sanity bounds

Warnings are logged. Critical failures raise an exception and halt the feed.

---

## Modeling Context (for prediction features)

**Target variables** (all in `weekly_player_data`):
- `passing_yards`, `rushing_yards`, `receiving_yards`
- `total_tds` (passing + rushing + receiving TDs, added as computed column in loader)
- `receptions`

**Key predictive features:**
- `opps_*_fantasy_points_exp` — opportunity model (expected fantasy points)
- `offense_pct` — snap share from `snap_count`
- `form_shotgun_pct` — formation usage from `play_by_play_formations`
- `pfr_times_pressured_pct` — pressure rate from `pro_football_ref_adv_stats`
- `ng_completion_percentage_above_expectation` — AWS tracking from `nextgen`

**Recommended modeling approach:**
- Ridge regression or XGBoost on rolling 4-week windows
- Poisson regression for TD counts
- Bet sizing: Kelly Criterion with 0.25 fractional Kelly cap

---

## Git Workflow

- **Active development branch**: `claude/add-claude-documentation-BhgB1`
- Push with: `git push -u origin <branch-name>`
- Commit messages should be concise and describe the "why" of changes.

---

## Common Gotchas

1. **`CURRENT_WEEK` must be updated manually** in `pipeline/config.py` each Tuesday before the cron job runs. The pipeline does not auto-detect the current week.

2. **`HISTORICAL_SEASONS` is currently empty** (`[]`). To run a historical backfill, populate it (e.g., `list(range(2016, 2025))`).

3. **`snap_count` joins via `pfr_player_id`**, not `gsis_id`. Always bridge through `fantasy_football_ids` when joining snap data to other tables.

4. **nflreadpy may return extra/fewer columns** than expected between package updates. Loaders should be robust to this — use explicit column selection rather than relying on positional access.

5. **Supabase upsert requires exact column match.** If you add columns to a loader, you must also add them to `sql/schema.sql` and re-run the schema (or `ALTER TABLE`). The upsert will fail if the DataFrame contains columns that don't exist in the DB table.

6. **The `.env` file contains real credentials** and must never be committed. It is correctly gitignored via `.gitignore`.

7. **`fantasy_football_rankings` page_type matters.** The value `"weekly"` is the most useful for prop betting (weekly expert consensus). `"ros"` and `"draft"` are rest-of-season and draft-day rankings.

8. **No test suite exists.** Use `inspect_tables.py` for manual data quality checks before and after pipeline changes.
