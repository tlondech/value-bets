# Value Bet Finder — Football & Tennis

A statistical betting recommendation engine that identifies value bets across professional football and tennis. It fetches live odds from The Odds API (Winamax lines), models outcomes using sport-specific predictive models, and surfaces bets where the bookmaker's implied probability is lower than the model's estimate.

## How It Works

### Football pipeline

For each upcoming match across supported leagues:

1. **Fetches live odds** from The Odds API (Winamax lines)
2. **Loads historical results** from football-data.co.uk (domestic leagues) or football-data.org (Champions League)
3. **Builds team ratings** using a Dixon-Coles MLE model (with rolling-window fallback), blended with head-to-head stats
4. **Computes expected goals** (λ) per team, adjusted for fatigue, rest days, and UCL second-leg aggregate dynamics
5. **Builds a score probability matrix** via Poisson distribution with Dixon-Coles low-score correction
6. **Calculates Expected Value** (EV = true_prob × decimal_odds − 1) for each outcome; caps the model/implied probability ratio to filter out hallucinated high-EV bets; surfaces only the best bet per market group (1X2, O/U 2.5)
7. **Fetches team news** (optional) — for bets with EV ≥ 20% within 24h of kickoff, pulls injury/suspension context from NewsAPI using rule-based sentence extraction

### Tennis pipeline

For each active ATP/WTA tournament (discovered automatically each run):

1. **Discovers active tournaments** from The Odds API `/v4/sports` endpoint — no hardcoding required
2. **Fetches live odds** from The Odds API (Winamax lines)
3. **Downloads historical match data** from Jeff Sackmann's tennis repositories (`tennis_atp` / `tennis_wta`) on GitHub — last 5 seasons
4. **Computes surface-adjusted Elo ratings** per player: overall Elo + surface-specific Elo (Hard / Clay / Grass), blended 60/40
5. **Infers court surface** from tournament name (keyword matching)
6. **Calculates Expected Value** against Winamax odds using the same EV formula as football

Elo ratings are computed once per run and shared across all tournaments for the same tour (ATP or WTA).

### Supported Markets
- **Football:** 1X2 (Home Win / Draw / Away Win), Over/Under 2.5 goals
- **Tennis:** Match winner (Player 1 Win / Player 2 Win)

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
FOOTBALL_DATA_ORG_API_KEY=your_key     # required for Champions League — https://www.football-data.org (free tier)
SUPABASE_URL=your_supabase_url         # required — https://supabase.com
SUPABASE_ANON_KEY=your_anon_key        # required — Supabase project anon key
NEWS_API_KEY=your_key_here             # optional — https://newsapi.org (100 req/day free tier)
```

### 3. Run

```bash
python main.py
```

The report opens automatically in your browser. Results are also saved to `data/latest_report.json`.

---

## Automated Daily Updates

A GitHub Actions workflow (`.github/workflows/daily_update.yml`) runs every 6 hours and commits the updated `index.html` directly to the repository. This lets you host the report as a static GitHub Pages site with no manual intervention.

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
| `FOOTBALL_DATA_ORG_API_KEY` | `""` | football-data.org key (required for UCL) |
| `SUPABASE_URL` | — | Supabase project URL (required) |
| `SUPABASE_ANON_KEY` | — | Supabase project anon key (required) |
| `NEWS_API_KEY` | `""` | NewsAPI key (optional — enables team news for EV ≥ 20% bets within 24h of kickoff) |
| `ENABLED_LEAGUES` | all | Comma-separated football league keys, e.g. `epl,laliga` (tennis is always auto-discovered) |
| `EV_THRESHOLD` | `0.05` | Minimum EV to surface a bet (5%) |
| `ROLLING_WINDOW` | `5` | Number of recent matches for rolling stats (football) |
| `POISSON_MAX_GOALS` | `8` | Score matrix size (0–N goals) (football) |
| `ODDS_TOTALS_BOOKMAKERS` | `""` | Fallback bookmaker for O/U 2.5 when Winamax has no line, e.g. `pinnacle` |

---

## Project Structure

```
.
├── main.py                          # Pipeline entry point
├── config.py                        # Configuration and league definitions
├── constants.py                     # Shared constants (EV thresholds, news fetch config)
├── requirements.txt
├── .env.example
├── index.html                       # Generated report (committed by CI for GitHub Pages)
│
├── .github/workflows/
│   └── daily_update.yml             # Runs every 6 hours, auto-commits index.html
│
├── extractors/
│   ├── odds.py                      # The Odds API client (1X2, O/U, tennis discovery)
│   ├── tennis_data_client.py        # Jeff Sackmann ATP/WTA historical data client
│   ├── footballdata_client.py       # football-data.co.uk CSV client (domestic leagues)
│   ├── footballdataorg_client.py    # football-data.org API client (UCL)
│   ├── soccerdata_client.py         # Alternative data source
│   ├── stats.py                     # Stats processing utilities
│   └── team_news.py                 # NewsAPI client — injury/suspension context for high-EV bets
│
├── models/
│   ├── features.py                  # Feature engineering (Dixon-Coles, H2H, fatigue)
│   ├── evaluator.py                 # Poisson probability + EV calculation (football)
│   └── tennis_model.py              # Surface-adjusted Elo ratings + EV calculation (tennis)
│
├── pipeline/
│   ├── __init__.py                  # Per-league orchestration (routes football vs tennis)
│   ├── fetch.py                     # Data fetching and SQLite persistence
│   ├── evaluate.py                  # Feature building, match evaluation, news enrichment
│   ├── helpers.py                   # Shared helpers (is_live, build_leg2_map)
│   └── settlement.py                # Dual-source settlement (football-data.org + .co.uk)
│
├── db/
│   ├── schema.py                    # SQLAlchemy models (matches, odds, fixtures, bet_history)
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
    ├── team_name_map.json           # Name mapping (Winamax → canonical) for football
    ├── crest_map.json               # Team crest URLs
    ├── bets.db                      # SQLite database
    └── latest_report.json           # Most recent report output
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

**Bet filtering:**
- **Probability ratio cap:** Bets are dropped when `model_prob / implied_prob > 1.3` (1.4 for UCL)
- **Market-group deduplication:** Only the single highest-EV outcome per market group (1X2, O/U 2.5) is surfaced

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

### Expected Value (both sports)

```
EV = (model_probability × decimal_odds) − 1
```

A positive EV indicates the model estimates a higher probability than the bookmaker's implied odds. Bets are only surfaced when EV > threshold (default 5%).

### Team News Enrichment (football only)
When `NEWS_API_KEY` is set, the pipeline fetches recent articles from NewsAPI for both teams in high-EV football matches (EV ≥ 20%) scheduled within the next 24 hours. A rule-based extractor surfaces the top injury and suspension sentences per team — no LLM required.

---

## Odds API Quota

Each run consumes one API request per active league/tournament:

| Call | Cost |
|------|------|
| `/v4/sports` (tennis discovery) | Free |
| `/v4/sports/{sport}/odds/` per football league | 1 request |
| `/v4/sports/{sport}/odds/` per active tennis tournament | 1 request |
| Jeff Sackmann CSV fetches | Free (GitHub) |

Typical cost: **7 football + 2–4 tennis = 9–11 requests per run**.

---

## Output

### HTML Report (`index.html`)
Interactive dashboard showing:
- Today's value bets grouped by league/tournament, with odds, true probability, and EV
- Team form, standings position, rest days (football)
- UCL aggregate context for second legs
- Team news (injury/suspension context) for high-EV football bets near kickoff (requires `NEWS_API_KEY`)
- Bet history with settled outcomes (won/lost)
- Filter drawer to narrow bets by league, market, or EV range

### JSON Report (`data/latest_report.json`)
Machine-readable version of the same data.

### Database (`data/bets.db`)
SQLite database with four tables:
- `matches` — upcoming match metadata
- `odds` — bookmaker odds (h2h and totals)
- `fixtures` — finished match results with xG
- `bet_history` — all recommended bets and their resolved outcomes

On every run, unsettled future bets that are no longer in the recommended set are automatically pruned from both the local SQLite database and Supabase.

---

## External Data Sources

| Source | Usage | Cost |
|--------|-------|------|
| [The Odds API](https://the-odds-api.com) | Live odds (Winamax), tennis tournament discovery | 500 req/month free |
| [football-data.co.uk](https://football-data.co.uk) | Historical results for domestic leagues | Free |
| [football-data.org](https://www.football-data.org) | Champions League fixtures and results | Free tier available |
| [Jeff Sackmann / tennis_atp](https://github.com/JeffSackmann/tennis_atp) | ATP historical match data for Elo | Free (GitHub) |
| [Jeff Sackmann / tennis_wta](https://github.com/JeffSackmann/tennis_wta) | WTA historical match data for Elo | Free (GitHub) |
| [NewsAPI](https://newsapi.org) | Team news and injury context (optional) | 100 req/day free tier |
