# ---------------------------------------------------------------------------
# Injury enrichment
# ---------------------------------------------------------------------------

# Minimum EV for a signal to trigger injury context fetching
EV_NEWS_THRESHOLD = 0.20

# Hours before kickoff within which injury context is fetched
TEAM_NEWS_CUTOFF_HOURS = 24


# ---------------------------------------------------------------------------
# Fatigue & rest-day adjustments
# ---------------------------------------------------------------------------

# Days since last match below which a team is considered fatigued
FATIGUE_THRESHOLD_DAYS = 4

# Goals-conceded multiplier applied to the opponent of a fatigued team (~8% more)
FATIGUE_FACTOR = 1.08

# NBA back-to-back: teams with exactly 1 day of rest (played the night before)
# score ~2–3 points fewer; applied as a direct deduction from expected points.
NBA_BACK_TO_BACK_DAYS = 1
NBA_BACK_TO_BACK_PENALTY = 2.5


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
DIXON_COLES_MAX_ITER = 1000
DIXON_COLES_FTOL = 1e-7

# Numerical floor applied to the Dixon-Coles τ correction to prevent zero/negative probability
DIXON_COLES_RHO_FLOOR = 1e-10

# L2 regularization strength applied to attack/defense parameters.
# Shrinks sparse-data teams (cups, qualifiers) toward the mean, preventing
# degenerate solutions when n_teams > n_fixtures.
DIXON_COLES_L2_REG = 0.01


# ---------------------------------------------------------------------------
# Poisson lambda clamping
# ---------------------------------------------------------------------------

# Expected-goals values are clamped to this range to prevent degenerate inputs
LAMBDA_MIN = 0.1
LAMBDA_MAX = 6.0


# ---------------------------------------------------------------------------
# EV evaluation & signal filtering
# ---------------------------------------------------------------------------

# Minimum EV required to flag a signal as positive
EV_THRESHOLD = 0.05

# Probability-ratio cap for UCL matches (looser than the default to allow for
# higher variance in knockout football)
UCL_PROB_RATIO_CAP = 1.4



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

# ESPN public API (no key required)
ESPN_API_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports"


# ---------------------------------------------------------------------------
# Database & persistence
# ---------------------------------------------------------------------------

# Number of prior seasons (including current) loaded for H2H fixture lookups
H2H_LOOKBACK_SEASONS = 3

# Tolerance when matching a fixture to a signal by date (used in SQLite settlement)
FIXTURE_DATE_TOLERANCE_DAYS = 1

# Same tolerance expressed in seconds (used in Supabase settlement)
FIXTURE_DATE_TOLERANCE_SECONDS = 86400


# ---------------------------------------------------------------------------
# Local dev
# ---------------------------------------------------------------------------

LOCAL_REPORT_URL = "http://localhost:8000"
