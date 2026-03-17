import { state } from "./state.js";
import { fetchHistoryPage } from "./api.js";

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
  epl:        "EPL",
  ucl:        "UCL",
  laliga:     "La Liga",
  bundesliga: "Bundesliga",
  seriea:     "Serie A",
  ligue1:     "Ligue 1",
};
const LEAGUE_TOTAL_MATCHDAYS = {
  epl:        38,
  laliga:     38,
  bundesliga: 34,
  seriea:     38,
  ligue1:     34,
};
const LEAGUE_COLORS = {
  epl:        "bg-purple-100 text-purple-800 dark:bg-purple-900/40 dark:text-purple-300",
  laliga:     "bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300",
  bundesliga: "bg-red-100    text-red-800    dark:bg-red-900/40    dark:text-red-300",
  seriea:     "bg-blue-100   text-blue-800   dark:bg-blue-900/40   dark:text-blue-300",
  ligue1:     "bg-green-100  text-green-800  dark:bg-green-900/40  dark:text-green-300",
  ucl:        "bg-indigo-100 text-indigo-800 dark:bg-indigo-900/40 dark:text-indigo-300",
  nba:        "bg-red-100    text-red-800    dark:bg-red-950/60    dark:text-red-300",
};
export const SPORTS = [
  { key: "football",   label: "⚽️ Football" },
  { key: "basketball", label: "🏀 Basketball" },
  { key: "tennis",     label: "🎾 Tennis" },
];
export const SPORT_EMOJI = { football: "⚽️", basketball: "🏀", tennis: "🎾" };
export const BET_TYPES = {
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
const DATE_RANGES_BETS = [
  { key: "all",      label: "All" },
  { key: "today",    label: "Today" },
  { key: "tomorrow", label: "Tomorrow" },
  { key: "week",     label: "This week" },
];
const DATE_RANGES_HIST = [
  { key: "all", label: "All time" },
  { key: "7d",  label: "Last 7 days" },
  { key: "30d", label: "Last 30 days" },
  { key: "3m",  label: "Last 3 months" },
];
const HIST_COLS = [
  { key: "kickoff",       label: "Date",   render: r => { const d = new Date(r.kickoff); const tz = Intl.DateTimeFormat().resolvedOptions().timeZone; const date = d.toLocaleDateString(undefined, { day: "numeric", month: "short", timeZone: tz }); const time = d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", timeZone: tz }); return `<span class="whitespace-nowrap leading-tight">${esc(date)}<br><span class="text-gray-400 dark:text-gray-500 text-xs">${esc(time)}</span></span>`; } },
  { key: "league_name",   label: "League", render: r => `<span class="whitespace-nowrap">${leagueBadge(r.league_key, LEAGUE_SHORT_NAMES[r.league_key] || r.league_name)}</span>` },
  { key: "home_team",     label: "Match",  render: r => `<span class="whitespace-nowrap">${esc(r.home_team)} <span class="text-gray-400 mx-0.5">v</span> ${esc(r.away_team)}</span>` },
  { key: "outcome_label", label: "Bet",    render: r => `<span class="whitespace-nowrap px-2 py-0.5 rounded-full text-xs font-medium ${betBadgeCls(r.result, true)}">${esc(r.outcome_label)}</span>` },
  { key: "_score",        label: "Score",  render: r => r.actual_home_goals != null ? `${r.actual_home_goals}–${r.actual_away_goals}` : "—", sortKey: "actual_home_goals", align: "center" },
  { key: "odds",          label: "Odds",   render: r => `<span class="font-mono">${Number(r.odds).toFixed(2)}</span>`, align: "right" },
  { key: "true_prob",     label: "Prob%",  render: r => `${(r.true_prob * 100).toFixed(1)}%`, align: "right" },
  { key: "ev",            label: "EV%",    render: r => evLabel(r.ev), align: "right" },
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
function surfaceChip(surface) {
  const cls = surface === "Clay"  ? "bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300"
            : surface === "Grass" ? "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300"
            :                       "bg-sky-100 text-sky-800 dark:bg-sky-900/40 dark:text-sky-300";
  return `<span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap ${cls}">${esc(surface)}</span>`;
}
function formBubbles(form) {
  if (!Array.isArray(form) || form.length === 0) return "";
  return form.map(r => {
    const cls = r === "W" ? "bg-green-500" : r === "D" ? "bg-gray-400 dark:bg-gray-500" : "bg-red-500";
    return `<span class="${cls} inline-block w-2.5 h-2.5 rounded-full" title="${r}"></span>`;
  }).join("");
}
function betBadgeCls(result, colored) {
  if (!colored) return "bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300";
  if (result === "won")  return "bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300";
  if (result === "lost") return "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300";
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
        bets: [],
      });
    }
    map.get(key).bets.push({
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
        actual_home_goals: row.actual_home_goals,
        actual_away_goals: row.actual_away_goals,
        bets: [],
      });
    }
    map.get(key).bets.push({
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
  const hasHighEV = m.bets.some(b => b.ev >= 0.20);
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
  if (showResult && m.actual_home_goals != null) {
    score = isTennis
      ? `<span class="text-sm font-bold tabular-nums">${m.actual_home_goals}–${m.actual_away_goals} sets</span>`
      : `<span class="text-sm font-bold tabular-nums">${m.actual_home_goals}–${m.actual_away_goals}</span>`;
  }

  const betLabel = b => {
    if (isBasketball && m.handicap_line != null) {
      const sign = m.handicap_line > 0 ? "+" : "";
      return `${esc(b.outcome_label)} (${sign}${m.handicap_line})`;
    }
    return esc(b.outcome_label);
  };

  const betsRows = m.bets.map(b => `
    <tr class="border-t border-gray-100 dark:border-gray-700/50">
      <td class="py-1.5 pr-2">
        <div class="max-w-full"><span class="inline-block max-w-full truncate px-2 py-0.5 rounded-full text-xs font-medium align-middle ${betBadgeCls(b.result, showResult)}">${betLabel(b)}</span></div>
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

  return `
  <div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
    <${headerTag} ${headerAttr} class="flex items-start justify-between px-4 py-2.5 bg-gray-50 dark:bg-gray-800/60 border-b border-gray-200 dark:border-gray-700 ${headerHoverCls}">
      <div class="flex flex-wrap items-center gap-2 mr-3">
        ${leagueBadge(m.league_key, badgeText)}
        ${isTennis && m.surface ? surfaceChip(m.surface) : ""}
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
            <th class="w-[32%] pb-1 pr-2 text-left font-medium">Bet</th>
            <th class="pb-1 pr-2 text-right font-medium">Odds</th>
            <th class="pb-1 pr-2 text-right font-medium">Prob</th>
            <th class="pb-1 text-right font-medium">EV</th>
          </tr>
        </thead>
        <tbody>${betsRows}</tbody>
      </table>
    </div>
    ${teamNewsPanel(m)}
  </div>`;
}

// ── Sport pills ────────────────────────────────────────────────
export function renderSportPills() {
  document.getElementById("sport-pills").innerHTML = `
    <div class="grid grid-cols-3 w-full md:w-auto rounded-lg border border-gray-300 dark:border-gray-700 overflow-hidden text-sm font-medium">
      ${SPORTS.map((s, i) => {
        const isActive = state.activeSport === s.key;
        const border = i > 0 ? "border-l border-gray-300 dark:border-gray-700" : "";
        const cls = isActive
          ? "bg-indigo-600 text-white"
          : "bg-white dark:bg-gray-900 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800";
        return `<button class="sport-pill w-full inline-flex items-center justify-center whitespace-nowrap px-2 py-1.5 transition-colors ${border} ${cls}" data-sport="${s.key}">${s.label}</button>`;
      }).join("")}
    </div>`;
  document.querySelectorAll(".sport-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      state.activeSport   = btn.dataset.sport;
      state.activeLeague  = "all";
      state.activeBetType = "all";
      state.teamSearch    = "";
      document.getElementById("team-search").value = "";
      updateFilterUI();
      renderBetsPanel();
      resetHistoryPagination();
    });
  });
  updateSportNavBtn();
}

export function updateSportNavBtn() {
  const icon = document.getElementById("sport-nav-icon");
  if (icon) icon.textContent = SPORT_EMOJI[state.activeSport] || "⚽️";
  document.querySelectorAll(".sport-pop-btn").forEach(btn => {
    const isActive = btn.dataset.sport === state.activeSport;
    btn.className = "sport-pop-btn flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-colors "
      + (isActive
          ? "bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400 font-medium"
          : "text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800");
  });
}

// ── League pills ───────────────────────────────────────────────
export function renderLeaguePills(matches) {
  const counts = {};
  for (const m of matches) counts[m.league_key] = (counts[m.league_key] || { name: m.league_name, n: 0 });
  for (const m of matches) counts[m.league_key].n++;

  const active   = "bg-indigo-600 text-white";
  const inactive = "border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 hover:border-indigo-400 hover:text-indigo-600 dark:hover:text-indigo-400";

  let html = `<button class="league-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${state.activeLeague === "all" ? active : inactive}" data-league="all">
    All <span class="ml-1 opacity-70">(${matches.length})</span>
  </button>`;
  for (const [key, { name, n }] of Object.entries(counts)) {
    html += `<button class="league-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${state.activeLeague === key ? active : inactive}" data-league="${esc(key)}">
      ${esc(LEAGUE_SHORT_NAMES[key] || name)} <span class="ml-1 opacity-70">(${n})</span>
    </button>`;
  }
  document.getElementById("league-pills").innerHTML = html;
  document.querySelectorAll(".league-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      state.activeLeague = btn.dataset.league;
      updateFilterUI();
      renderBetsPanel();
      resetHistoryPagination();
    });
  });
}

// ── Date range pills ───────────────────────────────────────────
export function renderDatePills() {
  const active   = "bg-indigo-600 text-white";
  const inactive = "border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 hover:border-indigo-400 hover:text-indigo-600 dark:hover:text-indigo-400";

  document.getElementById("date-bets-pills").innerHTML = DATE_RANGES_BETS.map(t =>
    `<button class="date-bets-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${state.activeDateBets === t.key ? active : inactive}" data-range="${t.key}">${t.label}</button>`
  ).join("");
  document.querySelectorAll(".date-bets-pill").forEach(btn => {
    btn.addEventListener("click", () => { state.activeDateBets = btn.dataset.range; updateFilterUI(); renderBetsPanel(); });
  });

  document.getElementById("date-hist-pills").innerHTML = DATE_RANGES_HIST.map(t =>
    `<button class="date-hist-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${state.activeDateHist === t.key ? active : inactive}" data-range="${t.key}">${t.label}</button>`
  ).join("");
  document.querySelectorAll(".date-hist-pill").forEach(btn => {
    btn.addEventListener("click", () => { state.activeDateHist = btn.dataset.range; updateFilterUI(); resetHistoryPagination(); });
  });
}

// ── Bet-type pills ─────────────────────────────────────────────
export function renderBetTypePills() {
  const container = document.getElementById("bet-type-pills");
  const section   = container.closest("div[class]");
  const types     = BET_TYPES[state.activeSport];
  if (!types) {
    section.classList.add("hidden");
    state.activeBetType = "all";
    return;
  }
  section.classList.remove("hidden");
  const active   = "bg-indigo-600 text-white";
  const inactive = "border border-gray-300 dark:border-gray-700 text-gray-700 dark:text-gray-300 hover:border-indigo-400 hover:text-indigo-600 dark:hover:text-indigo-400";
  container.innerHTML = types.map(t =>
    `<button class="bet-type-pill flex-shrink-0 px-3 py-1 rounded-full text-sm font-medium transition-colors ${state.activeBetType === t.key ? active : inactive}" data-type="${t.key}">${t.label}</button>`
  ).join("");
  document.querySelectorAll(".bet-type-pill").forEach(btn => {
    btn.addEventListener("click", () => {
      state.activeBetType = btn.dataset.type;
      updateFilterUI();
      renderBetsPanel();
      resetHistoryPagination();
    });
  });
}

// ── Burger drawer pill mirror ──────────────────────────────────
export function renderBurgerDrawerPills() {
  // League
  const leagueSrc  = document.getElementById("league-pills");
  const leagueDest = document.getElementById("league-pills-mobile");
  if (leagueSrc && leagueDest) {
    leagueDest.innerHTML = leagueSrc.innerHTML;
    leagueDest.querySelectorAll("[data-league]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.activeLeague = btn.dataset.league;
        updateFilterUI();
        renderBetsPanel();
        resetHistoryPagination();
        renderBurgerDrawerPills();
      });
    });
  }
  // Bet type
  const btSrc  = document.getElementById("bet-type-pills");
  const btDest = document.getElementById("bet-type-pills-mobile");
  if (btSrc && btDest) {
    btDest.innerHTML = btSrc.innerHTML;
    btDest.querySelectorAll("[data-type]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.activeBetType = btn.dataset.type;
        updateFilterUI();
        renderBetsPanel();
        resetHistoryPagination();
        renderBurgerDrawerPills();
      });
    });
  }
  // Date (bets)
  const dbSrc  = document.getElementById("date-bets-pills");
  const dbDest = document.getElementById("date-bets-pills-mobile");
  if (dbSrc && dbDest) {
    dbDest.innerHTML = dbSrc.innerHTML;
    dbDest.querySelectorAll("[data-range]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.activeDateBets = btn.dataset.range;
        updateFilterUI();
        renderBetsPanel();
        renderBurgerDrawerPills();
      });
    });
  }
  // Date (history)
  const dhSrc  = document.getElementById("date-hist-pills");
  const dhDest = document.getElementById("date-hist-pills-mobile");
  if (dhSrc && dhDest) {
    dhDest.innerHTML = dhSrc.innerHTML;
    dhDest.querySelectorAll("[data-range]").forEach(btn => {
      btn.addEventListener("click", () => {
        state.activeDateHist = btn.dataset.range;
        updateFilterUI();
        resetHistoryPagination();
        renderBurgerDrawerPills();
      });
    });
  }
  // Show correct date section for current tab
  const dbSection = document.getElementById("date-bets-section-mobile");
  const dhSection = document.getElementById("date-hist-section-mobile");
  if (dbSection) dbSection.classList.toggle("hidden", state.mainTab !== "bets");
  if (dhSection) dhSection.classList.toggle("hidden", state.mainTab !== "history");
}

// ── Filter UI (badge + chips) ──────────────────────────────────
export function updateFilterUI() {
  const active = [];
  if (state.activeLeague !== "all") {
    const row = [...state.betsData, ...state.histData].find(r => r.league_key === state.activeLeague);
    active.push({ key: "league", label: LEAGUE_SHORT_NAMES[state.activeLeague] || (row ? row.league_name : state.activeLeague) });
  }
  if (state.activeBetType !== "all") {
    const bt = (BET_TYPES[state.activeSport] || []).find(t => t.key === state.activeBetType);
    active.push({ key: "bettype", label: bt ? bt.label : state.activeBetType });
  }
  if (state.teamSearch) {
    active.push({ key: "team", label: `"${state.teamSearch}"` });
  }
  if (state.activeDateBets !== "all") {
    const r = DATE_RANGES_BETS.find(t => t.key === state.activeDateBets);
    active.push({ key: "datebets", label: r ? r.label : state.activeDateBets });
  }
  if (state.activeDateHist !== "all") {
    const r = DATE_RANGES_HIST.find(t => t.key === state.activeDateHist);
    active.push({ key: "datehist", label: r ? r.label : state.activeDateHist });
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
  if (tab === "history") state.activeDateBets = "all";
  else                   state.activeDateHist = "all";

  state.mainTab = tab;
  document.getElementById("panel-bets").classList.toggle("hidden", tab !== "bets");
  document.getElementById("panel-history").classList.toggle("hidden", tab !== "history");
  document.getElementById("date-bets-section").classList.toggle("hidden", tab !== "bets");
  document.getElementById("date-hist-section").classList.toggle("hidden", tab !== "history");

  document.querySelectorAll(".main-tab-btn").forEach(btn => {
    const isActive = btn.dataset.main === tab;
    const base = btn.closest("nav")
      ? "main-tab-btn px-3 py-1.5 text-sm font-medium rounded-lg border-b-2 transition-colors"
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

// ── Render bets panel ──────────────────────────────────────────
export function renderBetsPanel() {
  const allMatches = groupIntoMatches(state.betsData).filter(m => m.sport === state.activeSport);
  renderSportPills();
  renderLeaguePills(allMatches);
  renderBetTypePills();
  renderDatePills();

  let filtered = allMatches;

  if (state.activeLeague !== "all")
    filtered = filtered.filter(m => m.league_key === state.activeLeague);

  if (state.activeBetType !== "all") {
    const isPrefix = state.activeBetType.endsWith("_");
    filtered = filtered.filter(m => m.bets.some(b =>
      isPrefix ? b.outcome.startsWith(state.activeBetType) : b.outcome === state.activeBetType
    ));
  }

  if (state.teamSearch) {
    const q = state.teamSearch.toLowerCase();
    filtered = filtered.filter(m =>
      m.home_team.toLowerCase().includes(q) || m.away_team.toLowerCase().includes(q)
    );
  }

  if (state.activeDateBets !== "all") {
    const tz       = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const today    = new Date(new Date().toLocaleDateString("en-CA", { timeZone: tz }));
    const tomorrow = new Date(today.getTime() + 864e5);
    const dayAfter = new Date(today.getTime() + 2 * 864e5);
    const weekEnd  = new Date(today.getTime() + 7 * 864e5);
    filtered = filtered.filter(m => {
      const ko = new Date(m.kickoff);
      if (state.activeDateBets === "today")    return ko >= today    && ko < tomorrow;
      if (state.activeDateBets === "tomorrow") return ko >= tomorrow && ko < dayAfter;
      if (state.activeDateBets === "week")     return ko >= today    && ko < weekEnd;
      return true;
    });
  }

  if (state.activeBetType !== "all") {
    const isPrefix = state.activeBetType.endsWith("_");
    filtered = filtered.map(m => ({
      ...m,
      bets: m.bets.filter(b =>
        isPrefix ? b.outcome.startsWith(state.activeBetType) : b.outcome === state.activeBetType
      ),
    }));
  }

  const container = document.getElementById("cards-container");
  if (filtered.length === 0) {
    container.innerHTML = `<p class="text-center text-gray-400 py-12">No bets match the current filters.</p>`;
    return;
  }

  const byDate = new Map();
  for (const m of filtered) {
    const d = fmtDate(m.kickoff);
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
  const wins    = filteredData.filter(r => r.result === "won");
  const losses  = filteredData.filter(r => r.result === "lost");
  const settled = wins.length + losses.length;

  const avgOdds = filteredData.length
    ? (filteredData.reduce((s, r) => s + r.odds, 0) / filteredData.length).toFixed(2)
    : "—";
  const avgEv = filteredData.length
    ? "+" + (filteredData.reduce((s, r) => s + r.ev, 0) / filteredData.length * 100).toFixed(1) + "%"
    : "—";

  const expectedWins = settled
    ? [...wins, ...losses].reduce((s, r) => s + (r.true_prob || 0), 0)
    : 0;
  const recordHtml = settled
    ? `${wins.length}W <span class="text-sm font-medium text-gray-400 dark:text-gray-500 mx-0.5">/ ${expectedWins.toFixed(1)}EW</span>`
    : "—";

  const winRateColor = settled
    ? (wins.length >= expectedWins ? "text-green-500" : "text-red-500")
    : "";
  const winRateHtml = settled
    ? `<span class="${winRateColor}">${(wins.length / settled * 100).toFixed(1)}%</span>`
    : "—";

  const pnl    = wins.reduce((s, r)   => s + (r.odds - 1) * stakeFor(r.odds), 0)
               + losses.reduce((s, r) => s - stakeFor(r.odds), 0);
  const staked = [...wins, ...losses].reduce((s, r) => s + stakeFor(r.odds), 0);
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
  setHtml("stat-winrate", winRateHtml);
  setHtml("stat-pnl",     pnlStr);
  if (staked) setColor("stat-pnl", pnl >= 0);
}

// ── History panel ──────────────────────────────────────────────
export function renderHistory() {
  renderDatePills();

  const allRows = state.histData.map(r => ({ ...r, _status: r.result === "won" ? "won" : "lost" }));
  const tabCounts = {
    settled: allRows.length,
    won:     allRows.filter(r => r._status === "won").length,
    lost:    allRows.filter(r => r._status === "lost").length,
  };

  document.querySelectorAll(".hist-status-btn").forEach(btn => {
    const isActive = btn.dataset.status === state.histStatusFilter;
    btn.className = isActive
      ? "hist-status-btn inline-flex items-center gap-1.5 whitespace-nowrap rounded-lg px-3 py-1.5 text-sm font-medium transition-colors bg-indigo-50 border border-indigo-200 text-indigo-700 dark:bg-indigo-900/30 dark:border-indigo-800 dark:text-indigo-400"
      : "hist-status-btn inline-flex items-center gap-1.5 whitespace-nowrap rounded-lg px-3 py-1.5 text-sm font-medium transition-colors bg-white border border-gray-200 text-gray-600 dark:bg-gray-900 dark:border-gray-800 dark:text-gray-400 hover:border-gray-300 dark:hover:border-gray-700";
    const label = { settled: "Settled", won: "Won", lost: "Lost" }[btn.dataset.status];
    const n = tabCounts[btn.dataset.status] ?? 0;
    btn.innerHTML = `${label} <span class="text-xs opacity-60">(${n})</span>`;
  });

  const visible = state.histStatusFilter === "settled" ? allRows
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
    return `<th class="sortable px-4 py-3 ${align}" data-col="${sk}">${esc(c.label)}</th>`;
  }).join("") + "</tr>";
  thead.querySelectorAll("th.sortable").forEach(th => {
    th.addEventListener("click", () => {
      const col = th.dataset.col;
      if (state.histSortCol === col) state.histSortDir = state.histSortDir === "desc" ? "asc" : "desc";
      else { state.histSortCol = col; state.histSortDir = "desc"; }
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
    label.textContent = `All ${state.historyTotal} settled bets loaded`;
    const sentinel = document.getElementById("history-sentinel");
    if (state.historyObserver && sentinel) state.historyObserver.unobserve(sentinel);
  } else {
    label.textContent = `${shown} / ${state.historyTotal} settled bets`;
  }
}

export async function resetHistoryPagination() {
  state.historyPage     = 0;
  state.historyLoaded   = [];
  state.historyTotal    = 0;
  state.historyFetching = false;
  const sentinel = document.getElementById("history-sentinel");
  if (state.historyObserver && sentinel) state.historyObserver.observe(sentinel);
  const { data, count } = await fetchHistoryPage(0);
  state.historyTotal  = count;
  state.historyLoaded = data;
  state.histData      = state.historyLoaded;
  renderHistory();
  updateHistoryCountUI();
}

// ── Filter drawer (desktop right-side) ────────────────────────
export function openDrawer() {
  document.getElementById("filter-drawer").classList.add("open");
  document.getElementById("filter-backdrop").classList.add("open");
  document.body.style.overflow = "hidden";
}
export function closeDrawer() {
  document.getElementById("filter-drawer").classList.remove("open");
  document.getElementById("filter-backdrop").classList.remove("open");
  document.body.style.overflow = "";
}

// ── Burger drawer (mobile left-side) ──────────────────────────
export function openBurgerDrawer() {
  document.getElementById("burger-drawer").classList.add("open");
  document.getElementById("filter-backdrop").classList.add("open");
  document.getElementById("burger-btn").setAttribute("aria-expanded", "true");
  state.burgerDrawerOpen = true;
  document.body.style.overflow = "hidden";
  renderBurgerDrawerPills();
}
export function closeBurgerDrawer() {
  document.getElementById("burger-drawer").classList.remove("open");
  document.getElementById("filter-backdrop").classList.remove("open");
  document.getElementById("burger-btn").setAttribute("aria-expanded", "false");
  state.burgerDrawerOpen = false;
  document.body.style.overflow = "";
}
