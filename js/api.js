import { sb } from "./config.js";
import { state } from "./state.js";

// ── Fetch upcoming (unsettled) signals ──────────────────────────
export async function fetchSignals() {
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

  q = q.eq("sport", state.activeSport);
  if (state.activeLeague     !== "all") q = q.eq("league_key", state.activeLeague);
  if (state.activeSignalType !== "all") q = q.eq("outcome",    state.activeSignalType);
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

  q = q.eq("sport", state.activeSport);

  if (state.activeLeague     !== "all") q = q.eq("league_key", state.activeLeague);
  if (state.activeSignalType !== "all") q = q.eq("outcome",    state.activeSignalType);
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
