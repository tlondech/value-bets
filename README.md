# Signal Arena — Football, Tennis & Basketball

A statistical signal engine that identifies +EV opportunities across professional football, tennis, and NBA basketball. It fetches live odds from The Odds API (Winamax lines), models outcomes using sport-specific predictive models, and surfaces signals where the bookmaker's implied probability is lower than the model's estimate.

## How It Works

### Football pipeline

For each upcoming match across supported leagues:

1. **Fetches live odds** from The Odds API (Winamax lines)
2. **Loads historical results** from football-data.co.uk (domestic leagues) or football-data.org (Champions League)
3. **Builds team ratings** using a Dixon-Coles MLE model (with rolling-window fallback), blended with head-to-head stats
4. **Computes expected goals** (λ) per team, adjusted for fatigue, rest days, and UCL second-leg aggregate dynamics
5. **Builds a score probability matrix** via Poisson distribution with Dixon-Coles low-score correction
6. **Calculates Expected Value** (EV = true_prob × decimal_odds − 1) for each outcome; caps the model/implied probability ratio to filter out hallucinated high-EV signals; surfaces only the highest-EV signal per market group (1X2, O/U)
7. **Fetches team news** (optional) — for signals with EV ≥ 20% within 24h of kickoff, pulls injury/suspension context from NewsAPI using rule-based sentence extraction

### Tennis pipeline

For each active ATP/WTA tournament (discovered automatically each run):

1. **Discovers active tournaments** from The Odds API `/v4/sports` endpoint — no hardcoding required
2. **Fetches live odds** from The Odds API (Winamax lines)
3. **Downloads historical match data** from Jeff Sackmann's tennis repositories (`tennis_atp` / `tennis_wta`) on GitHub — last 5 seasons
4. **Computes surface-adjusted Elo ratings** per player: overall Elo + surface-specific Elo (Hard / Clay / Grass), blended 60/40
5. **Infers court surface** from tournament name (keyword matching)
6. **Calculates Expected Value** against Winamax odds using the same EV formula as football

Elo ratings are computed once per run and shared across all tournaments for the same tour (ATP or WTA).

**Settlement:** Completed match results are sourced from the ESPN public API. tennis-data.co.uk CSVs are used as a fallback for tournaments not yet reflected in ESPN.

### NBA basketball pipeline

1. **Fetches live odds** from The Odds API (Winamax lines), including spreads/handicap market
2. **Downloads team game logs** from the ESPN public API (no API key required) — current season
3. **Computes team efficiency ratings** per team: rolling average of points scored (attack) and points allowed (defense), with separate home/away splits; stores each team's most recent game date for fatigue detection
4. **Predicts expected scores** using the Gaussian efficiency model:
   ```
   home_expected = home_attack_home + league_avg − away_defense + HOME_ADV (3.5 pts)
   away_expected = away_attack_away + league_avg − home_defense
   ```
5. **Applies back-to-back fatigue adjustment**: teams with ≤1 day of rest since their last game have their expected score reduced by 2.5 pts; flagged with ⏱ in the report
6. **Calculates win, O/U, and spread cover probabilities** from a Normal distribution over the point differential and total
7. **Calculates Expected Value** using the same formula as other sports; surfaces the highest-EV signal per market group (moneyline, totals, spreads)

Team ratings are computed once per run. Games currently in progress (within a 3.5-hour live window to account for overtime) are skipped.

### Supported Markets
- **Football:** 1X2 (Home Win / Draw / Away Win), Over/Under goals (line auto-selected per event)
- **Tennis:** Match winner (Player 1 Win / Player 2 Win)
- **Basketball:** Moneyline (Home Win / Away Win), Over/Under points, Spread/Handicap

### Supported Leagues & Tournaments

**Football — static configuration:**

| Key | Competition |
|-----|-------------|
| `epl` | Premier League (England) |
| `ligue1` | Ligue 1 (France) |
| `laliga` | La Liga (Spain) |
| `bundesliga` | Bundesliga (Germany) |
| `seriea` | Serie A (Italy) |
| `ucl` | UEFA Champions League |
| `worldcup` | FIFA World Cup |

**Tennis — discovered dynamically each run:**

All active ATP and WTA tournaments at run time (ATP 250 / 500 / 1000, Grand Slams, WTA equivalents). New events appear automatically without any configuration change.

**Basketball:**

| Key | Competition |
|-----|-------------|
| `nba` | NBA |

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure API keys

```bash
cp .env.example .env
```

Edit `.env` and fill in your keys:

```env
THE_ODDS_API_KEY=your_key_here         # required — https://the-odds-api.com (500 free req/month)
SUPABASE_URL=your_supabase_url         # required — https://supabase.com
SUPABASE_ANON_KEY=your_anon_key        # required — Supabase project anon key
NEWS_API_KEY=your_key_here             # optional — https://newsapi.org (100 req/day free tier)
```

### 3. Run

```bash
python main.py
```

The report opens automatically in your browser.

---

## Automated Daily Updates

A GitHub Actions workflow (`.github/workflows/daily_update.yml`) runs `python main.py --fetch` five times a day at **10:07, 13:07, 16:07, 19:07, and 22:07 UTC** (every 3 hours). It writes signals directly to Supabase; the frontend reads from Supabase at load time, so no file is committed on each run.

The only files the workflow ever commits are the three crest map JSONs (`data/football_crest_map.json`, `data/tennis_crest_map.json`, `data/nba_crest_map.json`) — and only when they actually change (new teams or players detected).

The workflow can also be triggered manually via `workflow_dispatch`.

---

## Usage

```bash
# Normal run (uses cached DB data; auto-fetches if a match is scheduled today)
python main.py

# Always fetch fresh data from external APIs (use in CI / scheduled runs)
python main.py --fetch

# Check Odds API coverage per league/tournament without writing to DB or running the model
python main.py --dry-run

# Enable debug-level logging
python main.py --debug
```

---

## Configuration

All settings can be overridden via `.env`:

| Variable | Default | Description |
|----------|---------|-------------|
| `THE_ODDS_API_KEY` | — | The Odds API key (required) |
| `SUPABASE_URL` | — | Supabase project URL (required) |
| `SUPABASE_ANON_KEY` | — | Supabase project anon key (required) |
| `NEWS_API_KEY` | `""` | NewsAPI key (optional — enables team news for signals with EV ≥ 20% within 24h of kickoff) |
| `ENABLED_LEAGUES` | all | Comma-separated league keys, e.g. `epl,laliga,nba` (tennis is always auto-discovered) |
| `EV_THRESHOLD` | `0.05` | Minimum EV to surface a signal (5%) |
| `ROLLING_WINDOW` | `5` | Number of recent matches for rolling stats (football); doubled for NBA |
| `POISSON_MAX_GOALS` | `8` | Score matrix size (0–N goals) (football) |
| `ODDS_TOTALS_BOOKMAKERS` | `""` | Fallback bookmaker for O/U when Winamax has no totals line, e.g. `pinnacle` |
| `NBA_MIN_GAMES` | `10` | Minimum games a team must have played to generate signals |
| `NBA_HOME_ADVANTAGE` | `3.5` | Home court advantage in points |
| `NBA_SPREAD_STD` | `15.5` | Std dev of point differential (Normal distribution) |
| `NBA_TOTAL_STD` | `19.0` | Std dev of total points (Normal distribution) |

---

## Project Structure

```
.
├── main.py                          # Pipeline entry point
├── config.py                        # Configuration and league definitions
├── constants.py                     # Shared constants (EV thresholds, live window durations)
├── requirements.txt
├── .env.example
├── index.html                       # SPA shell — HTML structure only, no inline JS
│
├── js/                              # ES module frontend (no bundler required)
│   ├── app.js                       # Entry point: init(), refreshData(), event wiring, IIFEs
│   ├── ui.js                        # All rendering, filter chips, drawer/tab/pill logic
│   ├── api.js                       # fetchSignals(), fetchHistoryPage() — Supabase queries
│   ├── state.js                     # Centralised mutable state object (single source of truth)
│   └── config.js                    # Supabase client (ESM CDN build)
│
├── .github/workflows/
│   └── daily_update.yml             # Runs every 3 hours (10:00–22:00 UTC), auto-commits crest maps
│
├── extractors/
│   ├── base.py                      # Universal MatchData schema + SportsExtractor protocol
│   ├── espn_client.py               # Shared ESPN HTTP transport (base class for sport clients)
│   ├── espn_basketball_client.py    # ESPN NBA match results (completed games → MatchData)
│   ├── espn_soccer_client.py        # ESPN football fixtures (settlement)
│   ├── espn_tennis_client.py        # ESPN tennis match results (settlement)
│   ├── basketball_data_client.py    # ESPN game logs for NBA ratings (with CSV cache)
│   ├── tennis_sackmann_client.py    # Jeff Sackmann ATP/WTA historical data for Elo ratings
│   ├── tennisdatauk_client.py       # tennis-data.co.uk CSVs (tennis settlement fallback)
│   ├── odds.py                      # The Odds API client (1X2, O/U, spreads, tennis discovery)
│   ├── stats.py                     # Stats processing utilities
│   └── team_news.py                 # NewsAPI client — injury/suspension context for high-EV signals
│
├── models/
│   ├── features.py                  # Feature engineering (Dixon-Coles, H2H, fatigue)
│   ├── evaluator.py                 # Poisson probability + EV calculation (football)
│   ├── tennis_model.py              # Surface-adjusted Elo ratings + EV calculation (tennis)
│   ├── nba_model.py                 # Gaussian efficiency model + EV calculation (basketball)
│   └── sport_evaluators.py          # SportEvaluator protocol + per-sport evaluation strategies + EVALUATORS registry
│
├── pipeline/
│   ├── __init__.py                  # Thin dispatch layer: routes each league to FETCHERS + EVALUATORS
│   ├── fetch.py                     # Data fetching and SQLite persistence
│   ├── fetchers.py                  # LeagueFetcher protocol + FetchResult + per-sport fetch strategies + FETCHERS registry
│   ├── evaluate.py                  # Feature building, match evaluation, news enrichment (football)
│   ├── helpers.py                   # Shared helpers (is_live, build_leg2_map)
│   └── settlement.py                # Dual-source football settlement (football-data.org + .co.uk)
│
├── db/
│   ├── schema.py                    # SQLAlchemy models (matches, odds, fixtures, signal_history)
│   ├── queries.py                   # SQLite read/write helpers
│   └── supabase.py                  # Supabase client — remote persistence, settlement, pruning
│
├── notifications/
│   └── reporter.py                  # HTML + JSON report generation
│
├── logs/
│   └── run.log                      # Rotating log output
│
├── serve.py                         # Local dev server (suppresses Chrome DevTools 404 noise)
│
└── data/
    ├── team_name_map.json           # Name mapping (Winamax → canonical) for football and NBA
    ├── football_crest_map.json      # Football team crest URLs
    ├── tennis_crest_map.json        # Tennis player flag URLs (auto-updated each run)
    ├── nba_crest_map.json           # NBA team logo URLs (NBA CDN)
    └── signals.db                   # SQLite database
```

---

## Statistical Models

### Football — Dixon-Coles + Poisson

The primary rating system fits a Maximum Likelihood Estimation model over historical fixtures to derive each team's attack and defense strength. A ρ (rho) parameter corrects for the known over/under-frequency of low-score results (0-0, 1-0, 0-1, 1-1).

When fewer than 10 fixtures are available, the model falls back to rolling-window averages.

**Adjustments:**
- **Head-to-head blending:** H2H stats receive 30% weight when ≥3 historical meetings exist
- **Fatigue:** Teams with <4 days since their last match concede 8% more goals
- **UCL second legs:** Trailing teams receive an attack boost proportional to their goal deficit; leading teams receive a slight defensive orientation

**Signal filtering:**
- **Probability ratio cap:** Signals are dropped when `model_prob / implied_prob > 1.3` (1.4 for UCL)
- **Market-group deduplication:** Only the single highest-EV signal per market group (1X2, O/U) is surfaced

### Tennis — Surface-Adjusted Elo

Elo ratings are computed from Jeff Sackmann's historical match data (last 5 seasons) with two pools per player: overall Elo and surface-specific Elo (Hard, Clay, Grass). Win probability uses a 60/40 blend of surface vs. overall Elo.

K-factors are weighted by tournament level:

| Level | K |
|-------|---|
| Grand Slam | 32 |
| Masters 1000 | 28 |
| ATP 500 / WTA | 24 |
| ATP 250 | 20 |

Court surface is inferred from the tournament name (keyword matching). New tournaments with unknown names default to Hard court.

### Basketball — Gaussian Efficiency Model

Team ratings use a rolling window of recent game logs (2× the football rolling window). Scoring is modelled as a Normal distribution over the point differential and total, which is appropriate for basketball's high-scoring, continuous score distribution.

```
spread_mu = home_expected − away_expected
total_mu  = home_expected + away_expected

P(home win)    = norm.sf(0, spread_mu, spread_std)
P(over line)   = norm.sf(line, total_mu, total_std)
P(home covers) = norm.sf(−spread_home_point, spread_mu, spread_std)
```

**Adjustments:**
- **Home court advantage:** +3.5 pts added to the home team's expected score
- **Back-to-back fatigue:** Teams with ≤1 day of rest since their last game have their expected score reduced by 2.5 pts; flagged with ⏱ on the card

NBA games can run 3h+ (including overtime), so the live detection window is 3.5 hours (vs. 2.5h for football) to ensure in-progress games are never evaluated.

### Expected Value (all sports)

```
EV = (model_probability × decimal_odds) − 1
```

A positive EV indicates the model estimates a higher probability than the bookmaker's implied odds. Signals are only surfaced when EV > threshold (default 5%).

### Team News Enrichment (football only)
When `NEWS_API_KEY` is set, the pipeline fetches recent articles from NewsAPI for both teams in high-EV football signals (EV ≥ 20%) scheduled within the next 24 hours. A rule-based extractor surfaces the top injury and suspension sentences per team — no LLM required.

---


## Output

### HTML Report (`index.html`)
Interactive SPA dashboard — frontend logic lives in `js/` as plain ES modules (no bundler).

**Content:**
- Signals grouped by date/league, with odds, true probability, and EV
- Team form, standings position, rest days (football)
- Team form and logos (basketball)
- Player flag icons (tennis)
- UCL aggregate context for second legs
- Team news (injury/suspension context) for high-EV football signals near kickoff (requires `NEWS_API_KEY`)
- Signal history with won/lost outcomes, stats grid (record, win rate, P&L/ROI), infinite scroll
- Removable active-filter chips

**Mobile layout (< 768 px):**
- Slim top bar — burger menu (left), team search (centre), legend `?` (right)
- Left-side burger drawer with league, signal-type, and date filter pills; sticky Reset button
- Sticky bottom tab bar (Signals ⚡ · History 🕐 · Sport picker) that hides on scroll-down and reappears on scroll-up
- Sport popover from the bottom bar replaces the sport pill row
- Pull-to-refresh gesture

**Desktop layout (≥ 768 px):** title header, inline tab bar, sport/search/filter action bar, right-side filter drawer — unchanged.

### Database (`data/signals.db`)
SQLite database with four tables:
- `matches` — upcoming match metadata
- `odds` — bookmaker odds (h2h and totals)
- `fixtures` — finished match results with xG
- `signal_history` — all detected signals and their resolved outcomes

On every run, unsettled future signals that are no longer in the detected set are automatically pruned from both the local SQLite database and Supabase.

---

## External Data Sources

| Source | Usage | Cost |
|--------|-------|------|
| [The Odds API](https://the-odds-api.com) | Live odds (Winamax), tennis tournament discovery | 500 req/month free |
| [ESPN public API](https://site.api.espn.com) | Football/tennis/basketball settlement; NBA game logs | Free (no key) |
| [Jeff Sackmann / tennis_atp](https://github.com/JeffSackmann/tennis_atp) | ATP historical match data for Elo ratings | Free (GitHub) |
| [Jeff Sackmann / tennis_wta](https://github.com/JeffSackmann/tennis_wta) | WTA historical match data for Elo ratings | Free (GitHub) |
| [tennis-data.co.uk](http://www.tennis-data.co.uk) | Tennis settlement fallback (CSV) | Free |
| [NewsAPI](https://newsapi.org) | Team news and injury context (optional) | 100 req/day free tier |
