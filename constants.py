# ---------------------------------------------------------------------------
# Team news enrichment
# ---------------------------------------------------------------------------

# Minimum EV for a bet to trigger team news fetching
EV_NEWS_THRESHOLD = 0.20

# Hours before kickoff within which team news is fetched
TEAM_NEWS_CUTOFF_HOURS = 24

# Fallback look-back window (days) when rest_days is unavailable for a team
NEWS_DAYS_BACK_DEFAULT = 5

# Number of articles fetched per team from NewsAPI (pre-relevance-filter)
NEWS_FETCH_SIZE = 7

# Maximum number of injury/suspension sentences surfaced per team
TOP_NEWS_SENTENCES = 2

# Timeout for NewsAPI requests (seconds)
NEWSAPI_TIMEOUT = 10


# ---------------------------------------------------------------------------
# Fatigue & rest-day adjustments
# ---------------------------------------------------------------------------

# Days since last match below which a team is considered fatigued
FATIGUE_THRESHOLD_DAYS = 4

# Goals-conceded multiplier applied to the opponent of a fatigued team (~8% more)
FATIGUE_FACTOR = 1.08


# ---------------------------------------------------------------------------
# Head-to-head blending
# ---------------------------------------------------------------------------

# Weight given to H2H stats when blending with form-based stats
H2H_BLEND_WEIGHT = 0.30

# Minimum H2H fixtures required to apply the blend
H2H_MIN_FIXTURES = 2


# ---------------------------------------------------------------------------
# Rolling-window team stats
# ---------------------------------------------------------------------------

# Minimum home (and away) matches required to compute team attack/defense stats
MIN_TEAM_FIXTURES = 3

# Decay factor for exponentially-weighted mean (most recent = 1.0)
EXP_DECAY_WEIGHT = 0.8

# Fallback league averages when no fixtures exist yet
DEFAULT_AVG_HOME_GOALS = 1.5
DEFAULT_AVG_AWAY_GOALS = 1.1


# ---------------------------------------------------------------------------
# Dixon-Coles MLE model
# ---------------------------------------------------------------------------

# Time-decay parameter ξ — controls how fast older fixtures lose weight
DIXON_COLES_XI = 0.0065

# Minimum fixtures required to fit the Dixon-Coles model (falls back to rolling-window)
DIXON_COLES_MIN_FIXTURES = 10

# Initial values for the global parameters in the optimizer
DIXON_COLES_INIT_GAMMA = 0.3   # home advantage
DIXON_COLES_INIT_RHO = -0.1   # low-score correction

# L-BFGS-B bounds: (attack/defense params, home advantage γ, low-score correction ρ)
DIXON_COLES_ATTACK_DEFENSE_BOUND = 3.0   # symmetric: each α/β ∈ [-3, 3]
DIXON_COLES_GAMMA_BOUNDS = (0.0, 2.0)
DIXON_COLES_RHO_BOUNDS = (-1.0, 1.0)

# Optimizer stopping criteria
DIXON_COLES_MAX_ITER = 500
DIXON_COLES_FTOL = 1e-9

# Numerical floor applied to the Dixon-Coles τ correction to prevent zero/negative probability
DIXON_COLES_RHO_FLOOR = 1e-10


# ---------------------------------------------------------------------------
# Poisson lambda clamping
# ---------------------------------------------------------------------------

# Expected-goals values are clamped to this range to prevent degenerate inputs
LAMBDA_MIN = 0.1
LAMBDA_MAX = 6.0


# ---------------------------------------------------------------------------
# EV evaluation & bet filtering
# ---------------------------------------------------------------------------

# Minimum EV required to flag a bet as value
EV_THRESHOLD = 0.05

# Probability-ratio cap for UCL matches (looser than the default to allow for
# higher variance in knockout football)
UCL_PROB_RATIO_CAP = 1.4

# Hours window used to classify a match as "currently live" (kicked off but unfinished)
LIVE_MATCH_WINDOW_HOURS = 2.5


# ---------------------------------------------------------------------------
# API timeouts & rate limits
# ---------------------------------------------------------------------------

# The Odds API
ODDS_API_TIMEOUT = 15            # seconds
ODDS_API_QUOTA_CRITICAL = 10    # abort if fewer than this many requests remain

# football-data.co.uk
FOOTBALLDATA_COUK_TIMEOUT = 30  # seconds

# football-data.org (free tier: 10 req/min → 1 req per 6 s; 6.5 s adds safety margin)
FOOTBALLDATA_ORG_MIN_INTERVAL = 6.5  # seconds between requests
FOOTBALLDATA_ORG_TIMEOUT = 30        # seconds


# ---------------------------------------------------------------------------
# Odds parsing
# ---------------------------------------------------------------------------

# Maximum absolute difference from 2.5 when matching the Over/Under goals line
TOTALS_LINE_TOLERANCE = 0.01


# ---------------------------------------------------------------------------
# Database & persistence
# ---------------------------------------------------------------------------

# Number of prior seasons (including current) loaded for H2H fixture lookups
H2H_LOOKBACK_SEASONS = 3

# Tolerance when matching a fixture to a bet by date (used in SQLite settlement)
FIXTURE_DATE_TOLERANCE_DAYS = 1

# Same tolerance expressed in seconds (used in Supabase settlement)
FIXTURE_DATE_TOLERANCE_SECONDS = 86400


# ---------------------------------------------------------------------------
# Local dev
# ---------------------------------------------------------------------------

LOCAL_REPORT_URL = "http://localhost:8000"
