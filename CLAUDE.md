# CLAUDE.md — Signal Engine

## Project overview
Statistical signal finder for football, tennis, and NBA. Fetches live Winamax odds via The Odds API, runs sport-specific predictive models, and surfaces signals where EV > threshold. Results are stored in Supabase and displayed in a plain-HTML/ES-module SPA.

## Key entry points
- `main.py` — pipeline orchestrator; CLI: `python main.py [--fetch] [--dry-run] [--debug]`
- `serve.py` — local dev server (suppresses Chrome DevTools 404 noise)
- `js/app.js` — frontend entry point

## Architecture
> **Before making any change, verify it respects layer boundaries.** Each layer must remain abstract from the others — no cross-layer imports or logic leakage. If a change would couple two layers that are currently independent, redesign the approach rather than breaking the abstraction.

```
main.py → pipeline/ → extractors/ + models/ → db/ + notifications/
```
- `pipeline/fetch.py` — fetches odds + historical data, writes to SQLite
- `pipeline/fetchers.py` — `LeagueFetcher` Protocol + `FetchResult` dataclass + sport-specific fetch strategies (`FootballFetcher`, `TennisFetcher`, `NBAFetcher`) + `FETCHERS` registry
- `pipeline/evaluate.py` — builds features, runs models, enriches with injury context (football + NBA)
- `pipeline/settlement.py` — resolves past signals (ESPN primary, tennis-data.co.uk fallback)
- `models/features.py` — Dixon-Coles + H2H + fatigue (football)
- `models/evaluator.py` — Poisson score matrix + EV (football)
- `models/tennis_model.py` — surface-adjusted Elo + EV
- `models/nba_model.py` — Gaussian efficiency model + EV
- `models/sport_evaluators.py` — `SportEvaluator` Protocol + sport-specific evaluation strategies (`FootballEvaluator`, `TennisEvaluator`, `NBAEvaluator`) + `EVALUATORS` registry
- `db/schema.py` — SQLAlchemy models (matches, odds, fixtures, bet_history)
- `db/supabase.py` — remote persistence, settlement, pruning
- `config.py` — `LeagueConfig` dataclass + `Config` dataclass + `load_config()`
- `constants.py` — shared numeric constants (thresholds, live-window durations)

## Frontend
Plain ES modules, no bundler. Files in `js/`: `app.js`, `ui.js`, `api.js`, `state.js`, `config.js`. Served directly from `index.html`. Supabase is the data source at runtime.

## Data files
- `data/team_name_map.json` — Winamax → canonical name mapping (football + NBA); edit manually to fix name mismatches. Always update `_meta.last_updated` to today's date when editing this file.
- `data/football_crest_map.json`, `tennis_crest_map.json`, `nba_crest_map.json` — logo/flag URLs (tennis map is auto-updated each run)
- `data/signals.db` — SQLite; never commit this

## Conventions
- All model thresholds/constants live in `constants.py` or as `Config` fields with `.env` overrides — don't hardcode values in model files
- EV formula is uniform across sports: `EV = model_prob × decimal_odds − 1`
- Probability ratio cap (`max_prob_ratio`) filters hallucinated high-EV signals — each sport has its own cap
- Only the highest-EV outcome per market group (1X2, O/U, moneyline, totals, spreads) is surfaced
- Tennis leagues are discovered dynamically each run; football/NBA leagues are statically defined in `config.py`
- NBA off-season (July–September): ratings computation is skipped automatically

## Environment variables
Required: `THE_ODDS_API_KEY`, `SUPABASE_URL`, `SUPABASE_ANON_KEY`
Optional: `ENABLED_LEAGUES`, `EV_THRESHOLD`, `ROLLING_WINDOW`, and other model params

## Database migrations

Migrations live in `supabase/migrations/` and are managed via the Supabase CLI.

- The project is already linked (`project-ref: uteiydpfxybtjzmdvsgc`, Signal Arena, West Europe London).
- Filename format: `YYYYMMDDHHmmss_description.sql` (required by the CLI).

**Create a new migration:**
```bash
supabase migration new describe_your_change
# Edit the generated file, then:
supabase db push
```

**If a migration was already applied manually (SQL editor):**
```bash
supabase migration repair --status applied <timestamp>
```

**Check sync status:**
```bash
supabase migration list
```

## CI
GitHub Actions (`.github/workflows/daily_update.yml`) runs `python main.py --fetch` several times a day. Only commits the three crest map JSONs when they change. Does not commit `signals.db` or `index.html`.

## Adding a new sport
The pipeline uses a polymorphic strategy pattern. To add an other sport (e.g. NFL):
1. Create `extractors/espn_nfl_client.py` implementing the `SportsExtractor` Protocol from `extractors/base.py`
2. Add a `NFLEvaluator` class to `models/sport_evaluators.py` and register it: `EVALUATORS["american_football"] = NFLEvaluator()`
3. Add a `NFLFetcher` class to `pipeline/fetchers.py` and register it: `FETCHERS["american_football"] = NFLFetcher()`
4. Add the league config to `LEAGUES` in `config.py` with `sport_type="american_football"`

`run_league_pipeline` in `pipeline/__init__.py` requires no changes.

## After editing a file
- Check for stale references: any imports, call sites, or type signatures that reference removed/renamed symbols
- Check for dead code: exported functions with no call sites, parameters no longer used, variables only written but never read
- Remove dead code rather than leaving it commented out or orphaned

## What to avoid
- Don't import between `config.py` and `constants.py` circularly (only `constants.py → config.py` is allowed via the `_NBA_WINDOW` import)
- Don't add league keys to `LEAGUES` without also updating `data/team_name_map.json`
- Don't mock the Supabase or SQLite layers in tests — use real connections or skip
- Don't bundle or transpile the JS frontend; it uses native ES modules via CDN imports
- Don't mention API quota in README.md file

## Vocabulary
Act as a strict UX copywriter for Signal Arena — a data-driven sports analytics platform. Our product detects statistical signals in sports markets and surfaces +EV opportunities identified by predictive models. We treat sports analysis as a quantitative discipline, not gambling.

Strict Vocabulary Rules to Enforce:
- NEVER USE: Picks, locks, sure things, bet of the day, winnings, tipster, gamble, guaranteed, jackpot, value bets, bet, betting.
- ALWAYS USE: Signals, +EV, edge, model output, variance, turnover, P&L, ROI, staking, probabilities, historical yield, market inefficiency.
- TONE: Objective, analytical, transparent, and responsible. Never use hype or FOMO. Speak to the user like a data scientist presenting model findings.
