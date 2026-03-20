// Chart is loaded globally via <script> tag in index.html (chart.umd.min.js)
const Chart = window.Chart;

import { state } from './state.js';
import { fetchAllHistory } from './api.js';
import { updateStatsGrid, updateFilterUI, renderBurgerDrawerPills } from './ui.js';

// ── Stake sizing (mirrors ui.js) ───────────────────────────────
function stakeFor(odds) {
  if (odds < 2) return 20;
  if (odds < 3) return 10;
  return 5;
}

// ── Dark mode helpers ──────────────────────────────────────────
const isDark    = () => window.matchMedia('(prefers-color-scheme: dark)').matches;
const gridColor = () => isDark() ? 'rgba(255,255,255,0.07)' : 'rgba(0,0,0,0.06)';
const tickColor = () => isDark() ? '#9ca3af' : '#6b7280';

// ── Chart instances ────────────────────────────────────────────
let pnlChart = null;
let roiChart = null;

// ── Granularity state ──────────────────────────────────────────
let roiGranularity = 'week';

// ── Data builders ──────────────────────────────────────────────
function buildPnlData(records) {
  let cumulative = 0;
  const labels = [];
  const values = [];
  for (const r of records) {
    const stake = stakeFor(r.odds);
    if (r.result === 'hit')  cumulative += (r.odds - 1) * stake;
    if (r.result === 'miss') cumulative -= stake;
    labels.push((r.settled_at ?? r.kickoff ?? '').slice(0, 10));
    values.push(+cumulative.toFixed(2));
  }
  return { labels, values };
}

function isoWeek(d) {
  const jan4 = new Date(d.getFullYear(), 0, 4);
  const week = Math.ceil(((d - jan4) / 864e5 + jan4.getDay() + 1) / 7);
  return `${d.getFullYear()}-W${String(week).padStart(2, '0')}`;
}

function buildRoiData(records, granularity) {
  const buckets = new Map();
  for (const r of records) {
    const d   = new Date(r.settled_at ?? r.kickoff);
    const key = granularity === 'week'
      ? isoWeek(d)
      : `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    if (!buckets.has(key)) buckets.set(key, { pnl: 0, staked: 0 });
    const b     = buckets.get(key);
    const stake = stakeFor(r.odds);
    b.staked += stake;
    if (r.result === 'hit')  b.pnl += (r.odds - 1) * stake;
    if (r.result === 'miss') b.pnl -= stake;
  }
  const sorted = [...buckets.entries()].sort((a, b) => (a[0] < b[0] ? -1 : 1));
  return {
    labels: sorted.map(([k])    => k),
    values: sorted.map(([, v]) => v.staked ? +(v.pnl / v.staked * 100).toFixed(1) : 0),
  };
}

function buildSportData(records) {
  const sports = new Map();
  for (const r of records) {
    const sport = r.sport ?? 'football';
    if (!sports.has(sport)) sports.set(sport, { hits: 0, misses: 0, pnl: 0, staked: 0 });
    const s     = sports.get(sport);
    const stake = stakeFor(r.odds);
    s.staked += stake;
    if (r.result === 'hit')  { s.hits++;   s.pnl += (r.odds - 1) * stake; }
    if (r.result === 'miss') { s.misses++; s.pnl -= stake; }
  }
  return [...sports.entries()].map(([sport, s]) => ({
    sport,
    signals: s.hits + s.misses,
    roi:     s.staked ? +(s.pnl / s.staked * 100).toFixed(1) : 0,
    pnl:     +s.pnl.toFixed(0),
  }));
}

// ── Chart renderers ────────────────────────────────────────────
function renderPnlChart(records) {
  const { labels, values } = buildPnlData(records);
  const ctx = document.getElementById('chart-pnl')?.getContext('2d');
  if (!ctx) return;
  pnlChart?.destroy();
  const positive = (values.at(-1) ?? 0) >= 0;
  pnlChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        data: values,
        borderColor:     positive ? '#4ade80' : '#f87171',
        backgroundColor: positive ? 'rgba(74,222,128,0.08)' : 'rgba(248,113,113,0.08)',
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 4,
        fill: true,
        tension: 0.3,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => `P&L: ${ctx.parsed.y >= 0 ? '+' : ''}€${ctx.parsed.y}`,
          },
        },
      },
      scales: {
        x: { ticks: { color: tickColor(), maxTicksLimit: 6 }, grid: { color: gridColor() } },
        y: { ticks: { color: tickColor(), callback: v => `€${v}` }, grid: { color: gridColor() } },
      },
    },
  });
}

function renderRoiChart(records) {
  const { labels, values } = buildRoiData(records, roiGranularity);
  const ctx = document.getElementById('chart-roi')?.getContext('2d');
  if (!ctx) return;
  roiChart?.destroy();
  roiChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: values.map(v => v >= 0 ? 'rgba(74,222,128,0.7)' : 'rgba(248,113,113,0.7)'),
        borderColor:     values.map(v => v >= 0 ? '#4ade80' : '#f87171'),
        borderWidth: 1,
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => `ROI: ${ctx.parsed.y >= 0 ? '+' : ''}${ctx.parsed.y}%`,
          },
        },
      },
      scales: {
        x: { ticks: { color: tickColor() }, grid: { color: gridColor() } },
        y: { ticks: { color: tickColor(), callback: v => `${v}%` }, grid: { color: gridColor() } },
      },
    },
  });
}

function renderSportTable(records) {
  const container = document.getElementById('sport-table-container');
  const tbody     = document.getElementById('sport-table-body');
  if (!tbody || !container) return;
  if (state.activeSport !== 'all') {
    container.classList.add('hidden');
    return;
  }
  container.classList.remove('hidden');
  const rows = buildSportData(records);
  const label = { football: '⚽ Football', basketball: '🏀 Basketball', tennis: '🎾 Tennis' };
  tbody.innerHTML = rows.length
    ? rows.map(r => `
        <tr class="border-t border-gray-200 dark:border-gray-800">
          <td class="px-4 py-3 text-sm font-medium">${label[r.sport] ?? r.sport}</td>
          <td class="px-4 py-3 text-sm text-center text-gray-500">${r.signals}</td>
          <td class="px-4 py-3 text-sm text-center font-medium ${r.roi >= 0 ? 'text-green-500' : 'text-red-500'}">${r.roi >= 0 ? '+' : ''}${r.roi}%</td>
          <td class="px-4 py-3 text-sm text-center font-medium ${r.pnl >= 0 ? 'text-green-500' : 'text-red-500'}">${r.pnl >= 0 ? '+' : ''}€${r.pnl}</td>
        </tr>`).join('')
    : '<tr><td colspan="4" class="px-4 py-6 text-center text-sm text-gray-400">No settled data yet.</td></tr>';
}

// ── Public API ─────────────────────────────────────────────────
export async function refreshAnalytics() {
  const spinner = document.getElementById('analytics-spinner');
  spinner?.classList.remove('hidden');
  try {
    state.analyticsData = await fetchAllHistory();
    updateStatsGrid(state.analyticsData);
    renderPnlChart(state.analyticsData);
    renderRoiChart(state.analyticsData);
    renderSportTable(state.analyticsData);
  } finally {
    spinner?.classList.add('hidden');
  }
}


export function setupAnalytics() {
  // ROI period toggle (week / month)
  const weekBtn  = document.getElementById('roi-toggle-week');
  const monthBtn = document.getElementById('roi-toggle-month');
  if (weekBtn && monthBtn) {
    function activateRoi(btn, other, gran) {
      btn.classList.add('bg-indigo-500', 'text-white');
      btn.classList.remove('text-gray-500', 'dark:text-gray-400');
      other.classList.remove('bg-indigo-500', 'text-white');
      other.classList.add('text-gray-500', 'dark:text-gray-400');
      roiGranularity = gran;
      renderRoiChart(state.analyticsData);
    }
    weekBtn.addEventListener('click',  () => activateRoi(weekBtn,  monthBtn, 'week'));
    monthBtn.addEventListener('click', () => activateRoi(monthBtn, weekBtn,  'month'));
  }

  // Analytics filter pills live in the burger drawer (dynamically rendered).
  // Use document-level event delegation so clicks always work regardless of re-renders.
  document.addEventListener('click', e => {
    const sportBtn = e.target.closest('.analytics-sport-btn');
    if (sportBtn) {
      state.activeSport = sportBtn.dataset.sport;
      renderBurgerDrawerPills();
      updateFilterUI();
      refreshAnalytics();
    }
    const dateBtn = e.target.closest('.analytics-date-btn');
    if (dateBtn) {
      state.analyticsActiveDateRange = dateBtn.dataset.range;
      renderBurgerDrawerPills();
      refreshAnalytics();
    }
  });
}
