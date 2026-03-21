// ── Centralised application state ─────────────────────────────
// All module files import this object and mutate its properties
// directly (e.g. `state.activeSport = "basketball"`).
// This keeps a single source of truth without a bundler.

export const state = {
  // ── UI ──────────────────────────────────────────────────────
  mainTab:          "signals",  // "signals" | "history" | "analytics"
  burgerDrawerOpen: false,
  teaserMode:       false,

  // ── Data ────────────────────────────────────────────────────
  signalsData: [],
  histData: [],
  pendingData: [],
  analyticsData: [],

  // ── Analytics filters ───────────────────────────────────────
  analyticsActiveDateRange: "all",   // "all" | "30d" | "3m" | "6m"

  // ── Filters ─────────────────────────────────────────────────
  activeSport:       "all",        // "all" | "football" | "basketball" | "tennis"
  activeLeague:      "all",
  activeSignalType:  "all",
  teamSearch:        "",
  activeDateSignals: "today",      // "all" | "today" | "tomorrow" | "week"
  activeDateHist:    "all",        // "all" | "7d" | "30d" | "3m"

  // ── History table ───────────────────────────────────────────
  histSortCol:      "kickoff",
  histSortDir:      "desc",
  histStatusFilter: "settled",

  // ── History pagination ───────────────────────────────────────
  HISTORY_PAGE_SIZE: 50,
  historyPage:       0,
  historyTotal:      0,
  historyLoaded:     [],
  historyFetching:   false,
  historyObserver:   null,
};
