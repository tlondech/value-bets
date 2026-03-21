import { sb } from "./config.js";
import { state } from "./state.js";

// ── Fetch upcoming (unsettled) signals ──────────────────────────
export async function fetchSignals() {
  if (state.teaserMode) {
    const { data, error } = await sb.rpc("get_teaser_signals");
    if (error) throw error;
    return data ?? [];
  }
  const { data, error } = await sb
    .from("signal_history")
    .select("*")
    .eq("settled", false)
    .gt("kickoff", new Date().toISOString())
    .order("kickoff", { ascending: true });
  if (error) throw error;
  return data;
}

// ── Fetch past unsettled (pending) signals with active filters ──
export async function fetchPendingSignals() {
  let q = sb
    .from("signal_history")
    .select("*")
    .eq("settled", false)
    .lt("kickoff", new Date().toISOString())
    .order("kickoff", { ascending: false });

  if (state.activeSport      !== "all") q = q.eq("sport",      state.activeSport);
  if (state.activeLeague     !== "all") q = q.eq("league_key", state.activeLeague);
  if (state.activeSignalType !== "all") {
    if (state.activeSignalType === "1x2")       q = q.in("outcome", ["home_win", "draw", "away_win"]);
    else if (state.activeSignalType === "moneyline") q = q.in("outcome", ["home_win", "away_win"]);
    else if (state.activeSignalType.endsWith("_"))   q = q.like("outcome", `${state.activeSignalType}%`);
    else                                             q = q.eq("outcome", state.activeSignalType);
  }
  if (state.teamSearch)                 q = q.or(`home_team.ilike.%${state.teamSearch}%,away_team.ilike.%${state.teamSearch}%`);
  if (state.activeDateHist   !== "all") {
    const days  = state.activeDateHist === "7d" ? 7 : state.activeDateHist === "30d" ? 30 : 90;
    const since = new Date(Date.now() - days * 864e5).toISOString();
    q = q.gte("kickoff", since);
  }

  const { data, error } = await q;
  if (error) throw error;
  return data ?? [];
}

// ── Fetch all settled history for analytics (capped at 365 days) ─
export async function fetchAllHistory() {
  const floor = new Date(Date.now() - 365 * 864e5);

  let q = sb
    .from("signal_history")
    .select("*")
    .eq("settled", true)
    .order("kickoff", { ascending: true });

  if (state.activeSport !== "all") q = q.eq("sport", state.activeSport);

  let since = floor;
  if (state.analyticsActiveDateRange !== "all") {
    const days        = state.analyticsActiveDateRange === "30d" ? 30
                      : state.analyticsActiveDateRange === "3m"  ? 90
                      : 180; // "6m"
    const filterFloor = new Date(Date.now() - days * 864e5);
    if (filterFloor > floor) since = filterFloor;
  }
  q = q.gte("kickoff", since.toISOString());

  const { data, error } = await q;
  if (error) throw error;
  return data ?? [];
}

// ── Fetch a page of settled history with active filters ────────
export async function fetchHistoryPage(page = 0) {
  const from = page * state.HISTORY_PAGE_SIZE;
  const to   = from + state.HISTORY_PAGE_SIZE - 1;

  let q = sb
    .from("signal_history")
    .select("*", { count: "exact" })
    .eq("settled", true)
    .order("kickoff", { ascending: false })
    .range(from, to);

  if (state.activeSport      !== "all") q = q.eq("sport",      state.activeSport);

  if (state.activeLeague     !== "all") q = q.eq("league_key", state.activeLeague);
  if (state.activeSignalType !== "all") {
    if (state.activeSignalType === "1x2")       q = q.in("outcome", ["home_win", "draw", "away_win"]);
    else if (state.activeSignalType === "moneyline") q = q.in("outcome", ["home_win", "away_win"]);
    else if (state.activeSignalType.endsWith("_"))   q = q.like("outcome", `${state.activeSignalType}%`);
    else                                             q = q.eq("outcome", state.activeSignalType);
  }
  if (state.teamSearch)                 q = q.or(`home_team.ilike.%${state.teamSearch}%,away_team.ilike.%${state.teamSearch}%`);

  if (state.activeDateHist !== "all") {
    const days  = state.activeDateHist === "7d" ? 7 : state.activeDateHist === "30d" ? 30 : 90;
    const since = new Date(Date.now() - days * 864e5).toISOString();
    q = q.gte("kickoff", since);
  }

  const { data, error, count } = await q;
  if (error) throw error;
  return { data, count };
}

// ── Fetch showcase signals for the auth screen (anon-accessible) ─
export async function fetchShowcaseSignals() {
  try {
    const { data, error } = await sb.rpc("get_showcase_signals");
    if (error) return null;
    return data ?? null;
  } catch {
    return null;
  }
}
