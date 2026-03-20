import { state } from "./state.js";
import { fetchHistoryPage, fetchPendingSignals } from "./api.js";

// ── Info tooltip helper ────────────────────────────────────────
function infoIcon(text) {
  return `<span class="info-icon inline-flex items-center justify-center ml-1 w-3.5 h-3.5 rounded-full border border-gray-300 dark:border-gray-600 text-gray-400 dark:text-gray-500 hover:border-indigo-400 hover:text-indigo-500 transition-colors text-[9px] font-bold leading-none cursor-default flex-shrink-0" title="${esc(text)}">i</span>`;
}

// ── Formatting helpers ─────────────────────────────────────────
export function esc(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
export function fmtDate(iso) {
  return new Date(iso).toLocaleDateString(undefined, { weekday: "long", day: "numeric", month: "long" });
}
export function fmtTime(iso) {
  return new Date(iso).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone });
}
export function relativeDate(date) {
  const d   = new Date(date);
  const now = new Date();
  const tz  = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const time = d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit", hour12: true, timeZone: tz });
  const startOfToday     = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const startOfYesterday = new Date(startOfToday - 864e5);
  const startOfWeek      = new Date(startOfToday - 6 * 864e5);
  if (d >= startOfToday)     return `Today at ${time}`;
  if (d >= startOfYesterday) return `Yesterday at ${time}`;
  if (d >= startOfWeek)      return `${d.toLocaleDateString(undefined, { weekday: "long" })} at ${time}`;
  return `${d.toLocaleDateString(undefined, { day: "numeric", month: "short" })} at ${time}`;
}
function ordinal(n) {
  if (!n) return "";
  const s = ["th", "st", "nd", "rd"], v = n % 100;
  return n + (s[(v - 20) % 10] || s[v] || s[0]);
}
function evClass(ev) {
  if (ev >= 0.20) return "ev-danger";
  if (ev >= 0.10) return "ev-warning";
  return "ev-success";
}
function evLabel(ev) {
  return `<span class="${evClass(ev)}">+${(ev * 100).toFixed(1)}%</span>`;
}
function stakeFor(odds) {
  if (odds < 2) return 20;
  if (odds < 3) return 10;
  return 5;
}

// ── Display constants ──────────────────────────────────────────
export const LEAGUE_SHORT_NAMES = {
  // England
  epl:          "EPL",
  facup:        "FA Cup",
  eflcup:       "EFL Cup",
  // France
  ligue1:       "Ligue 1",
  ligue2:       "Ligue 2",
  coupedefrance:"Coupe de France",
  // Spain
  laliga:       "La Liga",
  copadelrey:   "Copa del Rey",
  // Germany
  bundesliga:   "Bundesliga",
  dfbpokal:     "DFB-Pokal",
  // Italy
  seriea:       "Serie A",
  coppaditalia: "Coppa Italia",
  // UEFA
  ucl:          "UCL",
  uel:          "UEL",
  uecl:         "UECL",
  uefanations:  "Nations League",
  euroqual:     "Euro Qual.",
  // FIFA
  worldcup:     "World Cup",
  wcqualeurope: "WC Qual. EU",
  // Basketball
  nba:          "NBA",
};
const LEAGUE_TOTAL_MATCHDAYS = {
  epl:        38,
  laliga:     38,
  bundesliga: 34,
  seriea:     38,
  ligue1:     34,
  ligue2:     38,
};
const LEAGUE_COLORS = {
  // England
  epl:          "bg-purple-100  text-purple-800  dark:bg-purple-900/40  dark:text-purple-300",
  facup:        "bg-fuchsia-100 text-fuchsia-800 dark:bg-fuchsia-900/40 dark:text-fuchsia-300",
  eflcup:       "bg-pink-100    text-pink-800    dark:bg-pink-900/40    dark:text-pink-300",
  // France
  ligue1:       "bg-green-100   text-green-800   dark:bg-green-900/40   dark:text-green-300",
  ligue2:       "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300",
  coupedefrance:"bg-lime-100    text-lime-800    dark:bg-lime-900/40    dark:text-lime-300",
  // Spain
  laliga:       "bg-orange-100  text-orange-800  dark:bg-orange-900/40  dark:text-orange-300",
  copadelrey:   "bg-amber-100   text-amber-800   dark:bg-amber-900/40   dark:text-amber-300",
  // Germany
  bundesliga:   "bg-red-100     text-red-800     dark:bg-red-900/40     dark:text-red-300",
  dfbpokal:     "bg-rose-100    text-rose-800    dark:bg-rose-900/40    dark:text-rose-300",
  // Italy
  seriea:       "bg-blue-100    text-blue-800    dark:bg-blue-900/40    dark:text-blue-300",
  coppaditalia: "bg-sky-100     text-sky-800     dark:bg-sky-900/40     dark:text-sky-300",
  // UEFA
  ucl:          "bg-indigo-100  text-indigo-800  dark:bg-indigo-900/40  dark:text-indigo-300",
  uel:          "bg-orange-100  text-orange-800  dark:bg-orange-900/40  dark:text-orange-300",
  uecl:         "bg-teal-100    text-teal-800    dark:bg-teal-900/40    dark:text-teal-300",
  uefanations:  "bg-slate-100   text-slate-800   dark:bg-slate-800/60   dark:text-slate-300",
  euroqual:     "bg-cyan-100    text-cyan-800    dark:bg-cyan-900/40    dark:text-cyan-300",
  // FIFA
  worldcup:     "bg-yellow-100  text-yellow-800  dark:bg-yellow-900/40  dark:text-yellow-300",
  wcqualeurope: "bg-amber-100   text-amber-800   dark:bg-amber-900/40   dark:text-amber-300",
  // Basketball
  nba:          "bg-red-100     text-red-800     dark:bg-red-950/60     dark:text-red-300",
};
function leaguePillCls(key, isActive) {
  const base = LEAGUE_COLORS[key]
    || (key.startsWith("tennis_atp_") ? "bg-teal-100 text-teal-800 dark:bg-teal-900/40 dark:text-teal-300"
      : key.startsWith("tennis_wta_") ? "bg-violet-100 text-violet-800 dark:bg-violet-900/40 dark:text-violet-300"
      : "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300");
  return isActive ? `${base} ring-2 ring-current ring-offset-1` : `${base} opacity-70 hover:opacity-100`;
}
export const SPORTS = [
  { key: "football",   label: "Football" },
  { key: "basketball", label: "Basketball" },
  { key: "tennis",     label: "Tennis" },
];
export const SPORT_EMOJI = { football: "⚽️", basketball: "🏀", tennis: "🎾" };
export const SIGNAL_TYPES = {
  football: [
    { key: "all",      label: "All Types" },
    { key: "home_win", label: "Home Win" },
    { key: "draw",     label: "Draw" },
    { key: "away_win", label: "Away Win" },
    { key: "over_",    label: "Over" },
    { key: "under_",   label: "Under" },
  ],
  basketball: [
    { key: "all",          label: "All Types" },
    { key: "home_win",     label: "Home Win" },
    { key: "away_win",     label: "Away Win" },
    { key: "over_",        label: "Over" },
    { key: "under_",       label: "Under" },
    { key: "spread_home_", label: "Spread (Home)" },
    { key: "spread_away_", label: "Spread (Away)" },
  ],
};
const DATE_RANGES_HIST = [
  { key: "all", label: "All time" },
  { key: "7d",  label: "Last 7 days" },
  { key: "30d", label: "Last 30 days" },
  { key: "3m",  label: "Last 3 months" },
];
const HIST_COLS = [
  { key: "kickoff",       label: "Date",   sortable: true, render: r => { const d = new Date(r.kickoff); const tz = Intl.DateTimeFormat().resolvedOptions().timeZone; const date = d.toLocaleDateString(undefined, { day: "numeric", month: "short", timeZone: tz }); const time = d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", timeZone: tz }); return `<span class="whitespace-nowrap leading-tight">${esc(date)}<br><span class="text-gray-400 dark:text-gray-500 text-xs">${esc(time)}</span></span>`; } },
  { key: "league_name",   label: "League", render: r => `<span class="whitespace-nowrap">${leagueBadge(r.league_key, LEAGUE_SHORT_NAMES[r.league_key] || r.league_name)}</span>` },
  { key: "home_team",     label: "Match",  render: r => `<span class="whitespace-nowrap">${esc(r.home_team)} <span class="text-gray-400 mx-0.5">v</span> ${esc(r.away_team)}</span>` },
  { key: "outcome_label", label: "Selection",   labelHtml: `Selection${infoIcon("Highest-EV outcome identified by the model")}`, render: r => `<span class="whitespace-nowrap px-2 py-0.5 rounded-full text-xs font-medium ${signalBadgeCls(r.result, true)}">${esc(r.outcome_label)}</span>` },
  { key: "_score",        label: "Score",  render: r => {
    if (r.actual_home_score == null) return "—";
    const isTennis = (r.league_key || "").startsWith("tennis_");
    const txt = `${r.actual_home_score}–${r.actual_away_score}${isTennis ? " sets" : ""}`;
    if (isTennis && r.score_detail) return `<span class="border-b border-dashed border-gray-400 dark:border-gray-500 cursor-default whitespace-nowrap" title="${esc(r.score_detail)}">${txt}</span>`;
    return txt;
  }, sortKey: "actual_home_score", align: "center" },
  { key: "odds",          label: "Odds",   labelHtml: `Odds${infoIcon("Decimal odds at time of signal detection")}`,  render: r => `<span class="font-mono">${Number(r.odds).toFixed(2)}</span>`, align: "right" },
  { key: "true_prob",     label: "Prob%",  labelHtml: `Prob%${infoIcon("Model's estimated win probability")}`, render: r => `${(r.true_prob * 100).toFixed(1)}%`, align: "right" },
  { key: "ev",            label: "EV%",    labelHtml: `EV%${infoIcon("Expected value — edge over the bookmaker")}`,  render: r => evLabel(r.ev), align: "right" },
];

// ── Badge / chip helpers ───────────────────────────────────────
function leagueBadge(key, name) {
  let cls = LEAGUE_COLORS[key];
  if (!cls) {
    if      (key.startsWith("tennis_atp_")) cls = "bg-teal-100 text-teal-800 dark:bg-teal-900/40 dark:text-teal-300";
    else if (key.startsWith("tennis_wta_")) cls = "bg-violet-100 text-violet-800 dark:bg-violet-900/40 dark:text-violet-300";
    else cls = "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300";
  }
  return `<span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap ${cls}">${esc(name)}</span>`;
}

function tennisCircuitChip(leagueKey) {
  const isATP = leagueKey.startsWith("tennis_atp_");
  const label = isATP ? "ATP" : "WTA";
  const cls   = isATP ? "bg-teal-100 text-teal-800 dark:bg-teal-900/40 dark:text-teal-300"
                      : "bg-violet-100 text-violet-800 dark:bg-violet-900/40 dark:text-violet-300";
  return `<span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap ${cls}">${label}</span>`;
}
function tennisTournamentChip(name, surface, round) {
  const cleanName = name.replace(/^(ATP|WTA)\s+/i, "").trim();
  const parts = [cleanName, round, surface].filter(Boolean);
  const text = parts.join(" · ");
  const cls  = surface === "Clay"  ? "bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300"
             : surface === "Grass" ? "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300"
             :                       "bg-sky-100 text-sky-800 dark:bg-sky-900/40 dark:text-sky-300";
  return `<span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap ${cls}">${esc(text)}</span>`;
}
function formBubbles(form) {
  if (!Array.isArray(form) || form.length === 0) return "";
  return form.map(r => {
    const cls = r === "W" ? "bg-green-500" : r === "D" ? "bg-gray-400 dark:bg-gray-500" : "bg-red-500";
    return `<span class="${cls} inline-block w-2.5 h-2.5 rounded-full" title="${r}"></span>`;
  }).join("");
}
function signalBadgeCls(result, colored) {
  if (!colored) return "bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300";
  if (result === "hit")  return "bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300";
  if (result === "miss") return "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300";
  return "bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300";
}


// ── Data grouping ──────────────────────────────────────────────
export function groupIntoMatches(rows) {
  const map = new Map();
  for (const row of rows) {
    const key = `${row.kickoff}|${row.home_team}|${row.away_team}`;
    if (!map.has(key)) {
      map.set(key, {
        kickoff:        row.kickoff,
        league_key:     row.league_key,
        league_name:    row.league_name,
        home_team:      row.home_team,
        away_team:      row.away_team,
        stage:          row.stage,
        home_rank:      row.home_rank,
        away_rank:      row.away_rank,
        home_form:      row.home_form,
        away_form:      row.away_form,
        home_crest:     row.home_crest,
        away_crest:     row.away_crest,
        home_rest_days: row.home_rest_days,
        away_rest_days: row.away_rest_days,
        h2h_used:       row.h2h_used,
        sport:          row.sport || "football",
        surface:        row.surface || null,
        handicap_line:  row.handicap_line ?? null,
        is_second_leg:  row.is_second_leg,
        agg_home:       row.agg_home,
        agg_away:       row.agg_away,
        leg1_result:    row.leg1_result,
        team_news:      row.team_news || null,
        bookmaker_link: row.bookmaker_link || null,
        signals: [],
      });
    }
    map.get(key).signals.push({
      outcome:       row.outcome,
      outcome_label: row.outcome_label,
      odds:          row.odds,
      true_prob:     row.true_prob,
      ev:            row.ev,
    });
  }
  return Array.from(map.values());
}

function groupHistoryIntoMatches(rows) {
  const map = new Map();
  for (const row of rows) {
    const key = `${row.kickoff}|${row.home_team}|${row.away_team}`;
    if (!map.has(key)) {
      map.set(key, {
        kickoff:           row.kickoff,
        league_key:        row.league_key,
        league_name:       row.league_name,
        home_team:         row.home_team,
        away_team:         row.away_team,
        stage:             row.stage,
        home_rank:         row.home_rank,
        away_rank:         row.away_rank,
        home_form:         row.home_form,
        away_form:         row.away_form,
        home_crest:        row.home_crest,
        away_crest:        row.away_crest,
        home_rest_days:    row.home_rest_days,
        away_rest_days:    row.away_rest_days,
        h2h_used:          row.h2h_used,
        sport:             row.sport || "football",
        handicap_line:     row.handicap_line ?? null,
        is_second_leg:     row.is_second_leg,
        agg_home:          row.agg_home,
        agg_away:          row.agg_away,
        actual_home_score: row.actual_home_score,
        actual_away_score: row.actual_away_score,
        signals: [],
      });
    }
    map.get(key).signals.push({
      outcome:       row.outcome,
      outcome_label: row.outcome_label,
      odds:          row.odds,
      true_prob:     row.true_prob,
      ev:            row.ev,
      result:        row.result,
    });
  }
  return Array.from(map.values());
}

// ── Team news context panel ────────────────────────────────────
function teamNewsPanel(m) {
  const hasHighEV = m.signals.some(s => s.ev >= 0.20);
  if (!hasHighEV || !m.team_news) return "";
  const tn = m.team_news;
  return `
    <details class="border-t border-gray-100 dark:border-gray-800 news-details group">
      <summary class="px-4 py-2.5 text-xs font-medium text-gray-500 dark:text-gray-400 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800/50 hover:text-indigo-600 dark:hover:text-indigo-400 select-none flex items-center gap-1.5 transition-colors">
        <span class="news-arrow transition-transform duration-150">▶</span> Context (team news)
      </summary>
      <div class="px-4 pb-3 pt-1 space-y-2 text-xs">
        <div class="flex gap-1.5">
          <span class="font-semibold text-gray-700 dark:text-gray-300 shrink-0">${esc(m.home_team)}:</span>
          <span class="text-gray-500 dark:text-gray-400">${esc(tn.home_summary || "No notable absences reported.")}</span>
        </div>
        <div class="flex gap-1.5">
          <span class="font-semibold text-gray-700 dark:text-gray-300 shrink-0">${esc(m.away_team)}:</span>
          <span class="text-gray-500 dark:text-gray-400">${esc(tn.away_summary || "No notable absences reported.")}</span>
        </div>
      </div>
    </details>`;
}

// ── Render a single match card ─────────────────────────────────
export function renderCard(m, opts = {}) {
  const time = fmtTime(m.kickoff);
  const crestH = (url, name) => url
    ? `<img src="${esc(url)}" alt="${esc(name)}" class="w-7 h-7 object-contain flex-shrink-0" onerror="this.style.display='none'">`
    : `<span class="w-7 h-7 flex-shrink-0"></span>`;
  const rankStr = n => n ? `<span class="text-xs font-medium text-gray-400 dark:text-gray-500">${ordinal(n)}</span>` : "";

  const isTennis     = m.sport === "tennis";
  const isBasketball = m.sport === "basketball";
  const restThreshold = isBasketball ? 2 : 4;
  const restWarn = days => (days != null && days < restThreshold)
    ? `<span class="whitespace-nowrap shrink-0 inline-flex items-center gap-0.5 text-xs font-medium text-amber-500" title="${isBasketball ? "Back-to-back" : "Short rest"}: only ${days} day${days === 1 ? "" : "s"} since last match">⏱ ${days}d</span>`
    : "";

  let badgeText;
  if (isTennis) {
    badgeText = esc(m.league_name);
  } else {
    const shortLeagueName = LEAGUE_SHORT_NAMES[m.league_key] || m.league_name;
    let shortStage = m.stage || "";
    const totalMD = LEAGUE_TOTAL_MATCHDAYS[m.league_key];
    if (totalMD && /^Matchday \d+$/.test(shortStage)) shortStage += ` / ${totalMD}`;
    shortStage = shortStage
      .replace("Matchday", "MD")
      .replace("Round of 16", "R16")
      .replace("Quarter-finals", "QF")
      .replace("Semi-finals", "SF");
    badgeText = shortStage ? `${shortLeagueName} • ${shortStage}` : shortLeagueName;
  }

  const showResult = !!opts.showResult;
  let score = "";
  if (showResult && m.actual_home_score != null) {
    score = isTennis
      ? `<span class="text-sm font-bold tabular-nums">${m.actual_home_score}–${m.actual_away_score} sets</span>`
      : `<span class="text-sm font-bold tabular-nums">${m.actual_home_score}–${m.actual_away_score}</span>`;
  }

  const signalLabel = b => {
    if (isBasketball && m.handicap_line != null) {
      const sign = m.handicap_line > 0 ? "+" : "";
      return `${esc(b.outcome_label)} (${sign}${m.handicap_line})`;
    }
    return esc(b.outcome_label);
  };

  // Card-level result for history tinting
  const cardResult = showResult
    ? (m.signals.some(b => b.result === "hit") ? "hit"
      : m.signals.some(b => b.result === "miss") ? "miss"
      : null)
    : null;

  const signalsRows = m.signals.map(b => `
    <tr class="border-t border-gray-100 dark:border-gray-700/50">
      <td class="py-1.5 pr-2">
        <div class="max-w-full"><span class="inline-block max-w-full truncate px-2 py-0.5 rounded-full text-xs font-medium align-middle bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300">${signalLabel(b)}</span></div>
      </td>
      <td class="py-1.5 pr-2 text-right font-mono text-sm">${Number(b.odds).toFixed(2)}</td>
      <td class="py-1.5 pr-2 text-right text-sm text-gray-500 dark:text-gray-400">${(b.true_prob * 100).toFixed(1)}%</td>
      <td class="py-1.5 text-right text-sm font-semibold">${evLabel(b.ev)}</td>
    </tr>`).join("");

  const bookmakerHref = !showResult
    ? (m.bookmaker_link || `https://www.winamax.fr/paris-sportifs/search?query=${encodeURIComponent(m.home_team + " " + m.away_team)}`)
    : null;
  const headerTag      = bookmakerHref ? "a" : "div";
  const headerAttr     = bookmakerHref ? `href="${esc(bookmakerHref)}" target="_blank" rel="noopener noreferrer"` : "";
  const headerHoverCls = bookmakerHref ? "hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors cursor-pointer group/header" : "";

  const cardCls = cardResult === "hit"
    ? "bg-green-500/10 rounded-xl border border-gray-200 dark:border-gray-800 border-l-4 border-l-green-500 overflow-hidden"
    : cardResult === "miss"
    ? "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden opacity-50 grayscale"
    : "bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden";

  return `
  <div class="${cardCls}">
    <${headerTag} ${headerAttr} class="flex items-start justify-between px-4 py-2.5 bg-gray-50 dark:bg-gray-800/60 border-b border-gray-200 dark:border-gray-700 ${headerHoverCls}">
      <div class="flex flex-wrap items-center gap-2 mr-3">
        <span class="text-base leading-none">${SPORT_EMOJI[m.sport] || "🏆"}</span>
        ${isTennis ? `${tennisCircuitChip(m.league_key)}${tennisTournamentChip(m.league_name, m.surface, m.stage)}` : leagueBadge(m.league_key, badgeText)}
        ${m.is_second_leg ? `<span class="text-[11px] bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300 px-2 py-0.5 rounded font-semibold whitespace-nowrap flex items-center gap-1">2nd Leg ${m.agg_home != null ? '<span class="opacity-70 font-normal">| Agg ' + m.agg_home + "–" + m.agg_away + "</span>" : ""}</span>` : ""}
        ${m.h2h_used ? `<span class="text-xs bg-pink-100 text-pink-700 dark:bg-pink-900/40 dark:text-pink-300 px-1.5 rounded">H2H</span>` : ""}
      </div>
      <div class="flex items-center gap-2 shrink-0">
        ${score}
        <span class="text-sm font-semibold tabular-nums text-gray-500 dark:text-gray-400">${esc(time)}</span>
        ${bookmakerHref ? `<svg class="w-3.5 h-3.5 text-gray-400 dark:text-gray-400 group-hover/header:text-indigo-500 dark:group-hover/header:text-indigo-400 transition-colors shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"/></svg>` : ""}
      </div>
    </${headerTag}>
    <div class="px-4 py-3 space-y-2">
      <div class="flex items-center justify-between gap-2">
        <div class="flex items-center gap-1.5 min-w-0">
          ${crestH(m.home_crest, m.home_team)}
          <span class="font-semibold truncate">${esc(m.home_team)}</span>
          ${restWarn(m.home_rest_days)}
        </div>
        <div class="flex items-center gap-2.5 shrink-0">
          ${rankStr(m.home_rank)}
          ${isTennis ? "" : `<div class="flex gap-1">${formBubbles(m.home_form)}</div>`}
        </div>
      </div>
      <div class="flex items-center justify-between gap-2">
        <div class="flex items-center gap-1.5 min-w-0">
          ${crestH(m.away_crest, m.away_team)}
          <span class="font-semibold truncate">${esc(m.away_team)}</span>
          ${restWarn(m.away_rest_days)}
        </div>
        <div class="flex items-center gap-2.5 shrink-0">
          ${rankStr(m.away_rank)}
          ${isTennis ? "" : `<div class="flex gap-1">${formBubbles(m.away_form)}</div>`}
        </div>
      </div>
    </div>
    <div class="px-4 pb-3 mt-2 border-t border-gray-100 dark:border-gray-800 pt-3">
      <table class="w-full text-sm table-fixed">
        <thead>
          <tr class="text-xs text-gray-400 uppercase">
            <th class="w-[34%] pb-1 pr-2 text-left font-medium">Signal<span class="hidden md:inline-flex">${infoIcon("The outcome with the highest model edge")}</span></th>
            <th class="w-[20%] pb-1 pr-2 text-right font-medium">Odds<span class="hidden md:inline-flex">${infoIcon("Decimal odds offered by the bookmaker")}</span></th>
            <th class="w-[21%] pb-1 pr-2 text-right font-medium">Prob<span class="hidden md:inline-flex">${infoIcon("Model's estimated probability of this outcome")}</span></th>
            <th class="w-[25%] pb-1 text-right font-medium">EV<span class="hidden md:inline-flex">${infoIcon("Expected value — gain per €1 staked if the model is right. Green = good value, yellow/red = high edge, verify odds first")}</span></th>
          </tr>
        </thead>
        <tbody>${signalsRows}</tbody>
      </table>
    </div>
    ${teamNewsPanel(m)}
  </div>`;
}

// ── Sport pills (now rendered inside the burger drawer by renderBurgerDrawerPills) ──
export function renderSportPills() {
  // Sport pills live in #sport-pills-drawer; renderBurgerDrawerPills() handles rendering.
  // This stub is kept so call-sites in renderSignalsPanel() don't break.
}

// ── League pills ───────────────────────────────────────────────
export function renderLeaguePills(matches) {
  const counts = {};
  for (const m of matches) counts[m.league_key] = (counts[m.league_key] || { name: m.league_name, n: 0 });
  for (const m of matches) counts[m.league_key].n++;

  const allActive   = "bg-indigo-600 text-white";
  const allInactive = "border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 hover:border-indigo-400 hover:text-indigo-600 dark:hover:text-indigo-400";

  let html = `<button class="league-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${state.activeLeague === "all" ? allActive : allInactive}" data-league="all">
    All <span class="ml-1 opacity-70">(${matches.length})</span>
  </button>`;
  for (const [key, { name, n }] of Object.entries(counts)) {
    html += `<button class="league-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${leaguePillCls(key, state.activeLeague === key)}" data-league="${esc(key)}">
      ${esc(LEAGUE_SHORT_NAMES[key] || name)} <span class="ml-1 opacity-70">(${n})</span>
    </button>`;
  }
  document.getElementById("league-pills").innerHTML = html;
  document.querySelectorAll(".league-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      state.activeLeague = btn.dataset.league;
      updateFilterUI();
      renderSignalsPanel();
      resetHistoryPagination();
    });
  });
}

// ── Date range pills / select ──────────────────────────────────
export function renderDatePills() {
  // Sync the standalone date <select> (desktop action bar) with current state
  const sel = document.getElementById("date-select");
  if (sel && sel.value !== state.activeDateSignals) sel.value = state.activeDateSignals;

  // History date pills (live in the burger drawer)
  const active   = "bg-indigo-600 text-white";
  const inactive = "border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 hover:border-indigo-400 hover:text-indigo-600 dark:hover:text-indigo-400";
  document.getElementById("date-hist-pills").innerHTML = DATE_RANGES_HIST.map(t =>
    `<button class="date-hist-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${state.activeDateHist === t.key ? active : inactive}" data-range="${t.key}">${t.label}</button>`
  ).join("");
  document.querySelectorAll(".date-hist-pill").forEach(btn => {
    btn.addEventListener("click", () => { state.activeDateHist = btn.dataset.range; updateFilterUI(); resetHistoryPagination(); });
  });
}

// ── Signal-type pills ─────────────────────────────────────────
export function renderSignalTypePills() {
  const container = document.getElementById("signal-type-pills");
  const section   = document.getElementById("signal-type-section");
  const types     = SIGNAL_TYPES[state.activeSport];
  if (!types) {
    section.classList.add("hidden");
    state.activeSignalType = "all";
    return;
  }
  section.classList.remove("hidden");
  const active   = "bg-indigo-600 text-white";
  const inactive = "border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 hover:border-indigo-400 hover:text-indigo-600 dark:hover:text-indigo-400";
  container.innerHTML = types.map(t =>
    `<button class="signal-type-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${state.activeSignalType === t.key ? active : inactive}" data-type="${t.key}">${t.label}</button>`
  ).join("");
  document.querySelectorAll(".signal-type-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      state.activeSignalType = btn.dataset.type;
      updateFilterUI();
      renderSignalsPanel();
      resetHistoryPagination();
    });
  });
}

// ── Burger drawer pills (context-aware per tab) ────────────────
export function renderBurgerDrawerPills() {
  const isAnalytics = state.mainTab === "analytics";

  // Show/hide sections based on active tab
  document.getElementById("drawer-signals-history-section")?.classList.toggle("hidden", isAnalytics);
  document.getElementById("drawer-analytics-section")?.classList.toggle("hidden", !isAnalytics);

  // Sport pills — different state and behavior per context
  const sportEl = document.getElementById("sport-pills-drawer");
  if (sportEl) {
    const pillBase = "flex-none px-3 py-1 rounded-full text-sm font-medium transition-colors";
    const activeCls   = `${pillBase} bg-indigo-600 text-white`;
    const inactiveCls = `${pillBase} border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 hover:border-indigo-400 hover:text-indigo-600 dark:hover:text-indigo-400`;

    if (isAnalytics) {
      const opts = [{ key: "all", label: "All Sports" }, ...SPORTS];
      sportEl.innerHTML = opts.map(s => {
        const cls = state.activeSport === s.key ? activeCls : inactiveCls;
        return `<button class="analytics-sport-btn ${cls}" data-sport="${s.key}">${s.label}</button>`;
      }).join("");
      // Analytics sport pill clicks handled by event delegation in analytics.js
    } else {
      const sportOpts = [{ key: "all", label: "All Sports" }, ...SPORTS];
      sportEl.innerHTML = sportOpts.map(s => {
        const cls = state.activeSport === s.key ? activeCls : inactiveCls;
        return `<button class="sport-pill ${cls}" data-sport="${s.key}">${s.label}</button>`;
      }).join("");
      sportEl.querySelectorAll(".sport-pill").forEach(btn => {
        btn.addEventListener("click", () => {
          state.activeSport      = btn.dataset.sport;
          state.activeLeague     = "all";
          state.activeSignalType = "all";
          state.teamSearch       = "";
          const ts  = document.getElementById("team-search");
          const tsm = document.getElementById("team-search-mobile");
          if (ts)  ts.value  = "";
          if (tsm) tsm.value = "";
          updateFilterUI();
          renderSignalsPanel();
          resetHistoryPagination();
          renderBurgerDrawerPills();
        });
      });
    }
  }

  // Analytics date range pills
  if (isAnalytics) {
    const dateEl = document.getElementById("analytics-date-pills-drawer");
    if (dateEl) {
      const pillBase = "flex-none px-3 py-1 rounded-full text-sm font-medium transition-colors";
      const activeCls   = `${pillBase} bg-indigo-600 text-white`;
      const inactiveCls = `${pillBase} border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 hover:border-indigo-400 hover:text-indigo-600 dark:hover:text-indigo-400`;
      const ranges = [
        { key: "all", label: "All time" },
        { key: "30d", label: "30d" },
        { key: "3m",  label: "3m" },
        { key: "6m",  label: "6m" },
      ];
      dateEl.innerHTML = ranges.map(r => {
        const cls = state.analyticsActiveDateRange === r.key ? activeCls : inactiveCls;
        return `<button class="analytics-date-btn ${cls}" data-range="${r.key}">${r.label}</button>`;
      }).join("");
      // Analytics date pill clicks handled by event delegation in analytics.js
    }
  }
}

// ── Filter UI (badge + chips) ──────────────────────────────────
export function updateFilterUI() {
  const active = [];

  if (state.mainTab === "analytics") {
    // Analytics tab: sport (shared) + date range
    if (state.activeSport !== "all") {
      const s = SPORTS.find(x => x.key === state.activeSport);
      active.push({ key: "sport", label: s ? s.label : state.activeSport });
    }
    if (state.analyticsActiveDateRange !== "all") {
      active.push({ key: "analyticsdate", label: state.analyticsActiveDateRange });
    }
  } else {
    // Signals / History tabs
    if (state.activeSport !== "all") {
      const s = SPORTS.find(x => x.key === state.activeSport);
      active.push({ key: "sport", label: s ? s.label : state.activeSport });
    }
    if (state.activeLeague !== "all") {
      const row = [...state.signalsData, ...state.histData].find(r => r.league_key === state.activeLeague);
      active.push({ key: "league", label: LEAGUE_SHORT_NAMES[state.activeLeague] || (row ? row.league_name : state.activeLeague) });
    }
    if (state.activeSignalType !== "all") {
      const bt = (SIGNAL_TYPES[state.activeSport] || []).find(t => t.key === state.activeSignalType);
      active.push({ key: "signaltype", label: bt ? bt.label : state.activeSignalType });
    }
    if (state.teamSearch) {
      active.push({ key: "team", label: `"${state.teamSearch}"` });
    }
    if (state.activeDateHist !== "all") {
      const r = DATE_RANGES_HIST.find(t => t.key === state.activeDateHist);
      active.push({ key: "datehist", label: r ? r.label : state.activeDateHist });
    }
  }
  const count = active.length;

  const badge = document.getElementById("filter-badge");
  if (count > 0) { badge.textContent = count; badge.classList.remove("hidden"); }
  else           { badge.classList.add("hidden"); }

  const clearBtn = document.getElementById("clear-filters-btn");
  clearBtn.disabled = count === 0;
  const resetBtn = document.getElementById("burger-reset-btn");
  if (resetBtn) resetBtn.disabled = count === 0;

  const chipsHtml = active.map(f =>
    `<span class="inline-flex items-center gap-1 pl-2 pr-1 py-0.5 rounded-full border border-blue-200 bg-blue-100 text-blue-800 dark:border-blue-800 dark:bg-blue-900/30 dark:text-blue-300 text-xs font-medium">
      ${esc(f.label)}
      <button data-filter-key="${f.key}" class="chip-remove ml-0.5 rounded-full hover:bg-blue-200 dark:hover:bg-blue-800 w-4 h-4 flex items-center justify-center leading-none" aria-label="Remove filter">&times;</button>
    </span>`
  ).join("");
  document.getElementById("active-filter-chips").innerHTML = chipsHtml;
  const mobileChips = document.getElementById("active-filter-chips-mobile");
  if (mobileChips) mobileChips.innerHTML = chipsHtml;
}
// Alias kept for any legacy callers
export const updateFilterBadge = updateFilterUI;

// ── Main tab switching ─────────────────────────────────────────
export function setMainTab(tab) {
  // Clear filters that don't exist on the target tab (sport persists across all tabs)
  if (tab === "analytics") {
    state.activeLeague      = "all";
    state.activeSignalType  = "all";
    state.teamSearch        = "";
    state.activeDateSignals = "today";
    state.activeDateHist    = "all";
    const ts  = document.getElementById("team-search");
    const tsm = document.getElementById("team-search-mobile");
    if (ts)  ts.value  = "";
    if (tsm) tsm.value = "";
  } else if (tab === "history") {
    state.activeLeague             = "all";
    state.activeSignalType         = "all";
    state.activeDateSignals        = "today";
    state.analyticsActiveDateRange = "all";
  } else if (tab === "signals") {
    state.activeDateHist           = "all";
    state.analyticsActiveDateRange = "all";
  }

  state.mainTab = tab;
  document.getElementById("panel-signals").classList.toggle("hidden", tab !== "signals");
  document.getElementById("panel-history").classList.toggle("hidden", tab !== "history");
  document.getElementById("panel-analytics").classList.toggle("hidden", tab !== "analytics");
  // date-select is in the desktop action bar (signals tab only)
  document.getElementById("date-select")?.classList.toggle("hidden", tab !== "signals");
  // date history pills live in the burger drawer
  document.getElementById("date-hist-section")?.classList.toggle("hidden", tab !== "history");

  document.querySelectorAll(".main-tab-btn").forEach(btn => {
    const isActive = btn.dataset.main === tab;
    const base = btn.closest("nav")
      ? "main-tab-btn inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-lg border-b-2 transition-colors"
      : "main-tab-btn px-4 py-2 text-sm font-medium border-b-2 transition-colors";
    btn.className = [
      base,
      isActive ? "border-indigo-500 text-indigo-600 dark:text-indigo-400"
               : "border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400",
    ].join(" ");
  });

  document.querySelectorAll(".bottom-nav-btn").forEach(btn => {
    const isActive = btn.dataset.main === tab;
    btn.classList.toggle("text-indigo-600",       isActive);
    btn.classList.toggle("dark:text-indigo-400",  isActive);
    btn.classList.toggle("text-gray-400",         !isActive);
    btn.classList.toggle("dark:text-gray-500",    !isActive);
  });

  if (document.getElementById("burger-drawer")) renderBurgerDrawerPills();
  updateFilterUI();
  if (tab === "history") renderHistory();
}

// ── Loading / error ────────────────────────────────────────────
export function showLoading() {
  document.getElementById("cards-container").innerHTML = `
    <div class="flex flex-col items-center gap-3 py-16">
      <span class="spinner"></span>
      <span class="text-xs text-gray-400">Fetching from Supabase…</span>
    </div>`;
}
export function showError(msg) {
  const el = document.getElementById("error-banner");
  el.textContent = "Error: " + msg;
  el.classList.remove("hidden");
}

// ── Render signals panel ───────────────────────────────────────
export function renderSignalsPanel() {
  const allMatches = groupIntoMatches(state.signalsData).filter(m => state.activeSport === "all" || m.sport === state.activeSport);
  renderSportPills();
  renderLeaguePills(allMatches);
  renderSignalTypePills();
  renderDatePills();

  let filtered = allMatches;

  if (state.activeLeague !== "all")
    filtered = filtered.filter(m => m.league_key === state.activeLeague);

  if (state.activeSignalType !== "all") {
    const isPrefix = state.activeSignalType.endsWith("_");
    filtered = filtered.filter(m => m.signals.some(b =>
      isPrefix ? b.outcome.startsWith(state.activeSignalType) : b.outcome === state.activeSignalType
    ));
  }

  if (state.teamSearch) {
    const q = state.teamSearch.toLowerCase();
    filtered = filtered.filter(m =>
      m.home_team.toLowerCase().includes(q) || m.away_team.toLowerCase().includes(q)
    );
  }

  if (state.activeDateSignals !== "all") {
    const tz      = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const todayD  = new Date().toLocaleDateString("en-CA", { timeZone: tz });
    const tmrwD   = new Date(Date.now() + 864e5).toLocaleDateString("en-CA", { timeZone: tz });
    const weekEndD = new Date(Date.now() + 7 * 864e5).toLocaleDateString("en-CA", { timeZone: tz });
    filtered = filtered.filter(m => {
      const d = new Date(m.kickoff).toLocaleDateString("en-CA", { timeZone: tz });
      if (state.activeDateSignals === "today")    return d === todayD;
      if (state.activeDateSignals === "tomorrow") return d === tmrwD;
      if (state.activeDateSignals === "week")     return d >= todayD && d <= weekEndD;
      return true;
    });
  }

  if (state.activeSignalType !== "all") {
    const isPrefix = state.activeSignalType.endsWith("_");
    filtered = filtered.map(m => ({
      ...m,
      signals: m.signals.filter(b =>
        isPrefix ? b.outcome.startsWith(state.activeSignalType) : b.outcome === state.activeSignalType
      ),
    }));
  }

  const container = document.getElementById("cards-container");
  if (filtered.length === 0) {
    container.innerHTML = `<p class="text-center text-gray-400 py-12">No signals match the current filters.</p>`;
    return;
  }

  const tz       = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const todayStr = new Date().toLocaleDateString("en-CA", { timeZone: tz });
  const tmrwStr  = new Date(Date.now() + 864e5).toLocaleDateString("en-CA", { timeZone: tz });
  function dayLabel(iso) {
    const d = new Date(iso).toLocaleDateString("en-CA", { timeZone: tz });
    if (d === todayStr) return "Today";
    if (d === tmrwStr)  return "Tomorrow";
    return fmtDate(iso);
  }

  const byDate = new Map();
  for (const m of filtered) {
    const d = dayLabel(m.kickoff);
    if (!byDate.has(d)) byDate.set(d, []);
    byDate.get(d).push(m);
  }

  let html = "";
  for (const [dateLabel, matches] of byDate) {
    html += `<h2 class="text-sm font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mt-6 mb-3 first:mt-0">${dateLabel} <span class="ml-1 normal-case font-normal opacity-60">• ${matches.length} match${matches.length === 1 ? "" : "es"}</span></h2>`;
    html += `<div class="grid gap-4 sm:grid-cols-2">`;
    html += matches.map(renderCard).join("");
    html += `</div>`;
  }
  container.innerHTML = html;
}

// ── History stats grid ─────────────────────────────────────────
export function updateStatsGrid(filteredData) {
  const hits    = filteredData.filter(r => r.result === "hit");
  const misses  = filteredData.filter(r => r.result === "miss");
  const settled = hits.length + misses.length;

  const avgOdds = filteredData.length
    ? (filteredData.reduce((s, r) => s + r.odds, 0) / filteredData.length).toFixed(2)
    : "—";
  const avgEv = filteredData.length
    ? "+" + (filteredData.reduce((s, r) => s + r.ev, 0) / filteredData.length * 100).toFixed(1) + "%"
    : "—";

  const expectedHits = settled
    ? [...hits, ...misses].reduce((s, r) => s + (r.true_prob || 0), 0)
    : 0;
  const recordHtml = settled
    ? `${hits.length}H <span class="text-sm font-medium text-gray-400 dark:text-gray-500 mx-0.5">/ ${expectedHits.toFixed(1)}EH</span>`
    : "—";

  const hitRateColor = settled
    ? (hits.length >= misses.length ? "text-green-500" : "text-red-500")
    : "";
  const hitRateHtml = settled
    ? `<span class="${hitRateColor}">${misses.length ? (hits.length / misses.length * 100).toFixed(1) : "∞"}%</span>`
    : "—";

  const pnl    = hits.reduce((s, r)   => s + (r.odds - 1) * stakeFor(r.odds), 0)
               + misses.reduce((s, r) => s - stakeFor(r.odds), 0);
  const staked = [...hits, ...misses].reduce((s, r) => s + stakeFor(r.odds), 0);
  const roi    = staked ? (pnl / staked * 100).toFixed(1) : null;
  const pnlStr = staked
    ? `€${staked.toFixed(0)} · ${pnl >= 0 ? "+" : ""}€${pnl.toFixed(0)} · ${roi}%`
    : "—";

  const setVal  = (id, val)  => { const el = document.querySelector(`#${id} > p:last-child`); if (el) el.textContent = val; };
  const setHtml = (id, html) => { const el = document.querySelector(`#${id} > p:last-child`); if (el) el.innerHTML   = html; };
  const setColor = (id, positive) => {
    const el = document.querySelector(`#${id} > p:last-child`);
    if (!el) return;
    el.classList.remove("text-green-500", "text-red-500");
    if (positive === true)  el.classList.add("text-green-500");
    if (positive === false) el.classList.add("text-red-500");
  };

  setVal("stat-total", filteredData.length);
  setHtml("stat-avg-odds", avgOdds !== "—"
    ? `${avgOdds} <span class="text-sm font-medium text-gray-400 dark:text-gray-500">/ ${avgEv}</span>`
    : "—");
  setHtml("stat-record",  recordHtml);
  setHtml("stat-winrate", hitRateHtml);
  setHtml("stat-pnl",     pnlStr);
  if (staked) setColor("stat-pnl", pnl >= 0);
}

// ── History panel ──────────────────────────────────────────────
export function renderHistory() {
  renderDatePills();

  const allRows = state.histData.map(r => ({ ...r, _status: r.result === "hit" ? "hit" : "miss" }));
  const tabCounts = {
    settled: allRows.length,
    hit:     allRows.filter(r => r._status === "hit").length,
    miss:    allRows.filter(r => r._status === "miss").length,
    pending: state.pendingData.length,
  };

  document.querySelectorAll(".hist-status-btn").forEach(btn => {
    const isActive = btn.dataset.status === state.histStatusFilter;
    btn.className = isActive
      ? "hist-status-btn inline-flex items-center gap-1.5 whitespace-nowrap rounded-lg px-3 py-1.5 text-sm font-medium transition-colors bg-indigo-50 border border-indigo-200 text-indigo-700 dark:bg-indigo-900/30 dark:border-indigo-800 dark:text-indigo-400"
      : "hist-status-btn inline-flex items-center gap-1.5 whitespace-nowrap rounded-lg px-3 py-1.5 text-sm font-medium transition-colors bg-white border border-gray-200 text-gray-600 dark:bg-gray-900 dark:border-gray-800 dark:text-gray-400 hover:border-gray-300 dark:hover:border-gray-700";
    const label = { settled: "Settled", hit: "Hit", miss: "Miss", pending: "Pending" }[btn.dataset.status];
    const n = tabCounts[btn.dataset.status] ?? 0;
    btn.innerHTML = `${label} <span class="text-xs opacity-60">(${n})</span>`;
  });

  const visible = state.histStatusFilter === "pending"
                ? state.pendingData.map(r => ({ ...r, _status: "pending" }))
                : state.histStatusFilter === "settled"
                  ? allRows
                  : allRows.filter(r => r._status === state.histStatusFilter);

  updateStatsGrid(allRows);

  const sorted = [...visible].sort((a, b) => {
    const av = a[state.histSortCol] ?? "", bv = b[state.histSortCol] ?? "";
    const dir = state.histSortDir === "desc" ? -1 : 1;
    return av < bv ? -dir : av > bv ? dir : 0;
  });

  // Mobile: card layout
  const container = document.getElementById("hist-cards-container");
  if (sorted.length === 0) {
    container.innerHTML = `<p class="text-center text-gray-400 py-12 whitespace-normal">No history yet.</p>`;
  } else {
    const matches = groupHistoryIntoMatches(sorted);
    const byDate  = new Map();
    for (const m of matches) {
      const d = fmtDate(m.kickoff);
      if (!byDate.has(d)) byDate.set(d, []);
      byDate.get(d).push(m);
    }
    let html = "";
    for (const [dateLabel, dayMatches] of byDate) {
      html += `<h2 class="text-sm font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mt-6 mb-3 first:mt-0">${dateLabel} <span class="ml-1 normal-case font-normal opacity-60">• ${dayMatches.length} match${dayMatches.length === 1 ? "" : "es"}</span></h2>`;
      html += `<div class="grid gap-4">`;
      html += dayMatches.map(m => renderCard(m, { showResult: true })).join("");
      html += `</div>`;
    }
    container.innerHTML = html;
  }

  // Desktop: table layout
  const thead = document.getElementById("hist-thead");
  thead.innerHTML = "<tr>" + HIST_COLS.map(c => {
    const sk    = c.sortKey || c.key;
    const align = c.align === "right" ? "text-right" : c.align === "center" ? "text-center" : "text-left";
    const inner = c.labelHtml
      ? `<span class="inline-flex items-center ${c.align === "right" ? "justify-end" : ""}">${c.labelHtml}</span>`
      : esc(c.label);
    const sortCls = c.sortable ? " sortable" : "";
    return `<th class="px-4 py-3 ${align}${sortCls}" data-col="${sk}">${inner}</th>`;
  }).join("") + "</tr>";
  thead.querySelectorAll("th.sortable").forEach(th => {
    th.addEventListener("click", () => {
      if (state.histSortCol === "kickoff") state.histSortDir = state.histSortDir === "desc" ? "asc" : "desc";
      else { state.histSortCol = "kickoff"; state.histSortDir = "desc"; }
      renderHistory();
    });
  });

  const tbody = document.getElementById("hist-tbody");
  if (sorted.length === 0) {
    tbody.innerHTML = `<tr><td colspan="${HIST_COLS.length}" class="px-4 py-10 text-center text-gray-400 whitespace-normal">No history yet.</td></tr>`;
    return;
  }

  const matchGroups   = [];
  const matchGroupMap = new Map();
  for (const r of sorted) {
    const key = `${r.kickoff}|${r.home_team}|${r.away_team}`;
    if (!matchGroupMap.has(key)) {
      const group = { key, rows: [r] };
      matchGroups.push(group);
      matchGroupMap.set(key, group);
    } else {
      matchGroupMap.get(key).rows.push(r);
    }
  }
  const BLANK_IN_REPEAT = new Set(["kickoff", "league_name", "home_team", "_score"]);
  tbody.innerHTML = matchGroups.map((group, gi) => {
    const stripeCls = gi % 2 === 0 ? "" : "bg-gray-50 dark:bg-gray-800/40";
    return group.rows.map((r, ri) => {
      const borderCls = gi > 0 && ri === 0 ? "border-t border-gray-200 dark:border-gray-700" : "";
      const cells = HIST_COLS.map(c => {
        const align   = c.align === "right" ? "text-right" : c.align === "center" ? "text-center" : "";
        const content = ri > 0 && BLANK_IN_REPEAT.has(c.key) ? "" : c.render(r);
        return `<td class="px-4 py-1.5 ${align}">${content}</td>`;
      }).join("");
      return `<tr class="${stripeCls} ${borderCls} hover:bg-blue-50 dark:hover:bg-blue-900/10 transition-colors">${cells}</tr>`;
    }).join("");
  }).join("");
}

// ── History pagination helpers ─────────────────────────────────
export function updateHistoryCountUI() {
  const label   = document.getElementById("history-count");
  const spinner = document.getElementById("history-spinner");
  if (!label) return;
  spinner.classList.add("hidden");
  const shown = state.historyLoaded.length;
  if (state.historyTotal === 0) {
    label.textContent = "";
  } else if (shown >= state.historyTotal) {
    label.textContent = `All ${state.historyTotal} signals loaded`;
    const sentinel = document.getElementById("history-sentinel");
    if (state.historyObserver && sentinel) state.historyObserver.unobserve(sentinel);
  } else {
    label.textContent = `${shown} / ${state.historyTotal} signals`;
  }
}

export async function resetHistoryPagination() {
  state.historyPage     = 0;
  state.historyLoaded   = [];
  state.historyTotal    = 0;
  state.historyFetching = false;
  const sentinel = document.getElementById("history-sentinel");
  if (state.historyObserver && sentinel) state.historyObserver.observe(sentinel);
  const [{ data, count }, pendingResult] = await Promise.all([fetchHistoryPage(0), fetchPendingSignals()]);
  state.historyTotal  = count;
  state.historyLoaded = data;
  state.histData      = state.historyLoaded;
  state.pendingData   = pendingResult;
  renderHistory();
  updateHistoryCountUI();
}

// ── Burger drawer (all viewports) ─────────────────────────────
export function openBurgerDrawer() {
  document.getElementById("burger-drawer").classList.add("open");
  document.getElementById("filter-backdrop").classList.add("open");
  document.getElementById("burger-btn")?.setAttribute("aria-expanded", "true");
  state.burgerDrawerOpen = true;
  document.body.style.overflow = "hidden";
  renderBurgerDrawerPills();
}
export function closeBurgerDrawer() {
  document.getElementById("burger-drawer").classList.remove("open");
  document.getElementById("filter-backdrop").classList.remove("open");
  document.getElementById("burger-btn")?.setAttribute("aria-expanded", "false");
  state.burgerDrawerOpen = false;
  document.body.style.overflow = "";
}
