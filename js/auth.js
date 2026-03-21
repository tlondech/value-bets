import { sb, TRIAL_DAYS } from "./config.js";

// ── Auth helpers ───────────────────────────────────────────────

export async function getSession() {
  const { data: { session } } = await sb.auth.getSession();
  return session;
}

export async function getFreshToken() {
  const { data: { session } } = await sb.auth.getSession();
  return session?.access_token ?? null;
}

export async function signInWithMagicLink(email) {
  const { error } = await sb.auth.signInWithOtp({
    email,
    options: {
      emailRedirectTo: window.location.origin + window.location.pathname,
    },
  });
  if (error) throw error;
}

export async function signOut() {
  await sb.auth.signOut();
  window.location.reload();
}

export function onAuthStateChange(cb) {
  return sb.auth.onAuthStateChange(cb);
}

// ── Auth screen HTML ───────────────────────────────────────────

function evClass(ev) {
  if (ev >= 0.20) return "ev-danger";
  if (ev >= 0.10) return "ev-warning";
  return "ev-success";
}

function buildShowcaseCard(signal) {
  if (!signal) return null;

  const sport = signal.sport;
  const kickoffDate = new Date(signal.kickoff);
  const dateStr = kickoffDate.toLocaleDateString("en-GB", { day: "numeric", month: "short" });
  const evPct = `+${(signal.ev * 100).toFixed(1)}%`;
  const probPct = `${(signal.true_prob * 100).toFixed(1)}%`;
  const oddsStr = Number(signal.odds).toFixed(2);
  const ev = Number(signal.ev);

  // League badge colors
  const badgeClass = sport === "basketball"
    ? "bg-red-100 text-red-800 dark:bg-red-950/60 dark:text-red-300"
    : sport === "tennis"
      ? "bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300"
      : "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300";

  const leagueLabel = signal.stage
    ? `${signal.league_name} · ${signal.stage}`
    : signal.league_name;

  // Score display
  const scoreHtml = (signal.actual_home_score != null && signal.actual_away_score != null)
    ? `<span class="text-xs font-mono text-gray-400 dark:text-gray-500 ml-1">${signal.actual_home_score}–${signal.actual_away_score}</span>`
    : "";

  // Tennis: set scores from score_detail
  const tennisScore = signal.score_detail
    ? `<span class="text-xs font-mono text-gray-400 dark:text-gray-500 ml-1">${signal.score_detail}</span>`
    : scoreHtml;

  const homeCrest = signal.home_crest
    ? `<img src="${signal.home_crest}" alt="" class="w-7 h-7 object-contain flex-shrink-0">`
    : `<span class="w-7 h-7 flex-shrink-0"></span>`;
  const awayCrest = signal.away_crest
    ? `<img src="${signal.away_crest}" alt="" class="w-7 h-7 object-contain flex-shrink-0">`
    : `<span class="w-7 h-7 flex-shrink-0"></span>`;

  const homeRank = sport === "tennis" && signal.home_rank
    ? `<span class="text-xs text-gray-400 dark:text-gray-500 ml-1">#${signal.home_rank}</span>` : "";
  const awayRank = sport === "tennis" && signal.away_rank
    ? `<span class="text-xs text-gray-400 dark:text-gray-500 ml-1">#${signal.away_rank}</span>` : "";

  return `
    <div class="relative pointer-events-none select-none">
      <span class="absolute top-2 right-2 z-10 text-[10px] font-bold uppercase tracking-wider bg-green-600/80 text-white px-1.5 py-0.5 rounded">Hit ✓</span>
      <div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden opacity-90">
        <div class="flex items-start justify-between px-4 py-2.5 bg-gray-50 dark:bg-gray-800/60 border-b border-gray-200 dark:border-gray-700">
          <div class="flex flex-wrap items-center gap-2 mr-3">
            <span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap ${badgeClass}">${leagueLabel}</span>
          </div>
          <span class="text-sm font-semibold tabular-nums text-gray-500 dark:text-gray-400">${dateStr}</span>
        </div>
        <div class="px-4 py-3 space-y-2">
          <div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-1.5 min-w-0">
              ${homeCrest}
              <span class="font-semibold truncate">${signal.home_canonical || signal.home_team}</span>${homeRank}
            </div>
            ${sport !== "tennis" ? tennisScore : ""}
          </div>
          <div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-1.5 min-w-0">
              ${awayCrest}
              <span class="font-semibold truncate">${signal.away_canonical || signal.away_team}</span>${awayRank}
            </div>
            ${sport === "tennis" ? tennisScore : ""}
          </div>
        </div>
        <div class="px-4 pb-3 border-t border-gray-100 dark:border-gray-800 pt-3">
          <table class="w-full text-sm table-fixed">
            <thead>
              <tr class="text-xs text-gray-400 uppercase">
                <th class="w-[32%] pb-1 pr-2 text-left font-medium">Signal</th>
                <th class="pb-1 pr-2 text-right font-medium">Odds</th>
                <th class="pb-1 pr-2 text-right font-medium">Prob</th>
                <th class="pb-1 text-right font-medium">EV</th>
              </tr>
            </thead>
            <tbody>
              <tr class="border-t border-gray-100 dark:border-gray-700/50">
                <td class="py-1.5 pr-2"><span class="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300">${signal.outcome_label}</span></td>
                <td class="py-1.5 pr-2 text-right font-mono text-sm">${oddsStr}</td>
                <td class="py-1.5 pr-2 text-right text-sm text-gray-500 dark:text-gray-400">${probPct}</td>
                <td class="py-1.5 text-right text-sm font-semibold"><span class="${evClass(ev)}">${evPct}</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>`;
}

export function renderAuthScreen(showcaseSignals = null) {
  const sampleCard1 = `
    <div class="relative pointer-events-none select-none">
      <span class="absolute top-2 right-2 z-10 text-[10px] font-bold uppercase tracking-wider bg-indigo-600/80 text-white px-1.5 py-0.5 rounded">Sample</span>
      <div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden opacity-90">
        <div class="flex items-start justify-between px-4 py-2.5 bg-gray-50 dark:bg-gray-800/60 border-b border-gray-200 dark:border-gray-700">
          <div class="flex flex-wrap items-center gap-2 mr-3">
            <span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300">Champions League · QF</span>
          </div>
          <span class="text-sm font-semibold tabular-nums text-gray-500 dark:text-gray-400">21:00</span>
        </div>
        <div class="px-4 py-3 space-y-2">
          <div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-1.5 min-w-0">
              <img src="https://crests.football-data.org/86.png" alt="Real Madrid" class="w-7 h-7 object-contain flex-shrink-0">
              <span class="font-semibold truncate">Real Madrid</span>
            </div>
            <div class="flex gap-1">
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-gray-400 dark:bg-gray-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
            </div>
          </div>
          <div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-1.5 min-w-0">
              <img src="https://crests.football-data.org/5.png" alt="Bayern München" class="w-7 h-7 object-contain flex-shrink-0">
              <span class="font-semibold truncate">Bayern München</span>
            </div>
            <div class="flex gap-1">
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-red-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-gray-400 dark:bg-gray-500 inline-block w-2.5 h-2.5 rounded-full"></span>
            </div>
          </div>
        </div>
        <div class="px-4 pb-3 border-t border-gray-100 dark:border-gray-800 pt-3">
          <table class="w-full text-sm table-fixed">
            <thead>
              <tr class="text-xs text-gray-400 uppercase">
                <th class="w-[32%] pb-1 pr-2 text-left font-medium">Signal</th>
                <th class="pb-1 pr-2 text-right font-medium">Odds</th>
                <th class="pb-1 pr-2 text-right font-medium">Prob</th>
                <th class="pb-1 text-right font-medium">EV</th>
              </tr>
            </thead>
            <tbody>
              <tr class="border-t border-gray-100 dark:border-gray-700/50">
                <td class="py-1.5 pr-2"><span class="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300">Home Win</span></td>
                <td class="py-1.5 pr-2 text-right font-mono text-sm">2.10</td>
                <td class="py-1.5 pr-2 text-right text-sm text-gray-500 dark:text-gray-400">54.1%</td>
                <td class="py-1.5 text-right text-sm font-semibold"><span class="ev-warning">+13.6%</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>`;

  const sampleCard2 = `
    <div class="relative pointer-events-none select-none">
      <span class="absolute top-2 right-2 z-10 text-[10px] font-bold uppercase tracking-wider bg-indigo-600/80 text-white px-1.5 py-0.5 rounded">Sample</span>
      <div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden opacity-90">
        <div class="flex items-start justify-between px-4 py-2.5 bg-gray-50 dark:bg-gray-800/60 border-b border-gray-200 dark:border-gray-700">
          <div class="flex flex-wrap items-center gap-2 mr-3">
            <span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300">Roland Garros · Clay</span>
          </div>
          <span class="text-sm font-semibold tabular-nums text-gray-500 dark:text-gray-400">14:00</span>
        </div>
        <div class="px-4 py-3 space-y-2">
          <div class="flex items-center gap-1.5 min-w-0">
            <img src="https://flagcdn.com/w40/it.png" alt="Italy" class="w-7 h-7 object-contain flex-shrink-0">
            <span class="font-semibold">Jannik Sinner</span>
          </div>
          <div class="flex items-center gap-1.5 min-w-0">
            <img src="https://flagcdn.com/w40/es.png" alt="Spain" class="w-7 h-7 object-contain flex-shrink-0">
            <span class="font-semibold">Carlos Alcaraz</span>
          </div>
        </div>
        <div class="px-4 pb-3 border-t border-gray-100 dark:border-gray-800 pt-3">
          <table class="w-full text-sm table-fixed">
            <thead>
              <tr class="text-xs text-gray-400 uppercase">
                <th class="w-[32%] pb-1 pr-2 text-left font-medium">Signal</th>
                <th class="pb-1 pr-2 text-right font-medium">Odds</th>
                <th class="pb-1 pr-2 text-right font-medium">Prob</th>
                <th class="pb-1 text-right font-medium">EV</th>
              </tr>
            </thead>
            <tbody>
              <tr class="border-t border-gray-100 dark:border-gray-700/50">
                <td class="py-1.5 pr-2"><span class="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300">Sinner Win</span></td>
                <td class="py-1.5 pr-2 text-right font-mono text-sm">2.10</td>
                <td class="py-1.5 pr-2 text-right text-sm text-gray-500 dark:text-gray-400">55.8%</td>
                <td class="py-1.5 text-right text-sm font-semibold"><span class="ev-warning">+17.2%</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>`;

  const sampleCard3 = `
    <div class="relative pointer-events-none select-none">
      <span class="absolute top-2 right-2 z-10 text-[10px] font-bold uppercase tracking-wider bg-indigo-600/80 text-white px-1.5 py-0.5 rounded">Sample</span>
      <div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden opacity-90">
        <div class="flex items-start justify-between px-4 py-2.5 bg-gray-50 dark:bg-gray-800/60 border-b border-gray-200 dark:border-gray-700">
          <div class="flex flex-wrap items-center gap-2 mr-3">
            <span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap bg-red-100 text-red-800 dark:bg-red-950/60 dark:text-red-300">NBA</span>
          </div>
          <span class="text-sm font-semibold tabular-nums text-gray-500 dark:text-gray-400">01:30</span>
        </div>
        <div class="px-4 py-3 space-y-2">
          <div class="flex items-center gap-1.5 min-w-0">
            <img src="https://cdn.nba.com/logos/nba/1610612738/primary/L/logo.svg" alt="Celtics" class="w-7 h-7 object-contain flex-shrink-0">
            <span class="font-semibold">Celtics</span>
          </div>
          <div class="flex items-center gap-1.5 min-w-0">
            <img src="https://cdn.nba.com/logos/nba/1610612747/primary/L/logo.svg" alt="Lakers" class="w-7 h-7 object-contain flex-shrink-0">
            <span class="font-semibold">Lakers</span>
          </div>
        </div>
        <div class="px-4 pb-3 border-t border-gray-100 dark:border-gray-800 pt-3">
          <table class="w-full text-sm table-fixed">
            <thead>
              <tr class="text-xs text-gray-400 uppercase">
                <th class="w-[32%] pb-1 pr-2 text-left font-medium">Signal</th>
                <th class="pb-1 pr-2 text-right font-medium">Odds</th>
                <th class="pb-1 pr-2 text-right font-medium">Prob</th>
                <th class="pb-1 text-right font-medium">EV</th>
              </tr>
            </thead>
            <tbody>
              <tr class="border-t border-gray-100 dark:border-gray-700/50">
                <td class="py-1.5 pr-2"><span class="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300">Celtics (-3.5)</span></td>
                <td class="py-1.5 pr-2 text-right font-mono text-sm">1.95</td>
                <td class="py-1.5 pr-2 text-right text-sm text-gray-500 dark:text-gray-400">58.4%</td>
                <td class="py-1.5 text-right text-sm font-semibold"><span class="ev-warning">+13.9%</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>`;

  return `
    <div class="min-h-screen bg-white dark:bg-gray-950 text-gray-900 dark:text-gray-100 overflow-x-hidden">
      <div class="max-w-6xl mx-auto px-6 py-12 md:py-20 flex flex-col md:flex-row md:items-start gap-12 md:gap-16">

        <!-- LEFT PANEL -->
        <div class="flex-1 max-w-md">

          <!-- Hero -->
          <div class="mb-10">
            <div class="text-xs font-semibold text-indigo-600 dark:text-indigo-400 uppercase tracking-widest mb-3">Signal Arena</div>
            <h1 class="text-3xl font-extrabold mb-3 leading-tight text-gray-900 dark:text-white">Spot mispriced lines before bookmakers correct them</h1>
            <p class="text-gray-600 dark:text-gray-400 text-base leading-relaxed">Statistical models surface mispriced odds across football, basketball and tennis.</p>
          </div>

          <!-- How it works -->
          <div class="mb-8">
            <p class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-4">How it works</p>
            <div class="space-y-3">
              <div class="flex gap-3 items-start">
                <span class="text-lg mt-0.5 shrink-0">🧮</span>
                <p class="text-sm text-gray-600 dark:text-gray-400"><span class="font-semibold text-gray-800 dark:text-gray-200">We do the math</span> — Our models analyse hundreds of matches across football, basketball, and tennis to estimate the real probability of each outcome — more accurately than bookmakers price them.</p>
              </div>
              <div class="flex gap-3 items-start">
                <span class="text-lg mt-0.5 shrink-0">📊</span>
                <p class="text-sm text-gray-600 dark:text-gray-400"><span class="font-semibold text-gray-800 dark:text-gray-200">We find the gaps</span> — When a bookmaker's odds are higher than they should be, that's a mispriced line. We flag these automatically against live odds, all day, every day.</p>
              </div>
              <div class="flex gap-3 items-start">
                <span class="text-lg mt-0.5 shrink-0">🎯</span>
                <p class="text-sm text-gray-600 dark:text-gray-400"><span class="font-semibold text-gray-800 dark:text-gray-200">You get the strongest signals</span> — Only the highest-edge opportunities land in your feed, with a direct link to act before the line moves.</p>
              </div>
            </div>
          </div>

          <!-- Feature list -->
          <ul class="mb-8 space-y-2.5">
            <li class="flex items-start gap-2.5 text-sm text-gray-700 dark:text-gray-300">
              <span class="text-green-500 dark:text-green-400 mt-0.5 shrink-0">✓</span>Football, basketball &amp; tennis — all in one place
            </li>
            <li class="flex items-start gap-2.5 text-sm text-gray-700 dark:text-gray-300">
              <span class="text-green-500 dark:text-green-400 mt-0.5 shrink-0">✓</span>Dixon-Coles, Elo, and Gaussian models
            </li>
            <li class="flex items-start gap-2.5 text-sm text-gray-700 dark:text-gray-300">
              <span class="text-green-500 dark:text-green-400 mt-0.5 shrink-0">✓</span>Full history with ROI tracking
            </li>
            <li class="flex items-start gap-2.5 text-sm text-gray-700 dark:text-gray-300">
              <span class="text-green-500 dark:text-green-400 mt-0.5 shrink-0">✓</span>Bookmaker links on every signal
            </li>
          </ul>

          <!-- Sign-in form -->
          <div id="auth-form-wrap">
            <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1.5" for="auth-email">
              Email address
            </label>
            <input id="auth-email" type="email" autocomplete="email" inputmode="email"
              placeholder="you@example.com"
              class="w-full px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-indigo-500 dark:focus:ring-indigo-400 mb-3" />
            <button id="auth-submit-btn"
              class="w-full py-2.5 rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-semibold transition-colors disabled:opacity-50 disabled:cursor-not-allowed">
              Send magic link
            </button>
            <p id="auth-error" class="hidden mt-3 text-xs text-red-500 dark:text-red-400 text-center"></p>
            <p class="text-xs text-gray-400 dark:text-gray-600 mt-2.5 text-center">${TRIAL_DAYS}-day free trial · cancel anytime · card required</p>
          </div>

          <div id="auth-sent-wrap" class="hidden text-center">
            <div class="text-4xl mb-4">📬</div>
            <p class="text-sm font-medium text-gray-800 dark:text-gray-200 mb-1">Check your inbox</p>
            <p class="text-xs text-gray-500">We sent a sign-in link to <strong id="auth-sent-email" class="text-gray-700 dark:text-gray-300"></strong>. Click it to log in.</p>
            <button id="auth-resend-btn" class="mt-5 text-xs text-indigo-600 dark:text-indigo-400 hover:underline">Send a new link</button>
          </div>

          <p class="text-xs text-gray-400 dark:text-gray-600 mt-8 leading-relaxed">
            For informational purposes only. Not financial or wagering advice. Participate responsibly. ·
            <a href="terms.html" class="hover:text-gray-600 dark:hover:text-gray-400 transition-colors">Terms</a> ·
            <a href="privacy.html" class="hover:text-gray-600 dark:hover:text-gray-400 transition-colors">Privacy</a>
          </p>
        </div>

        <!-- RIGHT PANEL -->
        <div class="flex flex-col gap-4 w-full md:w-96 shrink-0 md:mt-4">
          ${(() => {
            const football   = showcaseSignals?.football   ? buildShowcaseCard(showcaseSignals.football)   : null;
            const basketball = showcaseSignals?.basketball ? buildShowcaseCard(showcaseSignals.basketball) : null;
            const tennis     = showcaseSignals?.tennis     ? buildShowcaseCard(showcaseSignals.tennis)     : null;
            const hasReal    = football || basketball || tennis;
            const label      = hasReal ? "Recent hits" : "Example signals";
            return `<p class="text-xs font-semibold text-gray-500 dark:text-gray-600 uppercase tracking-wider mb-1">${label}</p>
          ${football   ?? sampleCard1}
          ${basketball ?? sampleCard3}
          ${tennis     ?? sampleCard2}`;
          })()}
        </div>

      </div>
    </div>`;
}

export function attachAuthListeners() {
  const emailInput = document.getElementById("auth-email");
  const submitBtn  = document.getElementById("auth-submit-btn");
  const errorEl    = document.getElementById("auth-error");
  const formWrap   = document.getElementById("auth-form-wrap");
  const sentWrap   = document.getElementById("auth-sent-wrap");
  const sentEmail  = document.getElementById("auth-sent-email");
  const resendBtn  = document.getElementById("auth-resend-btn");

  async function submit() {
    const email = emailInput.value.trim();
    if (!email) return;
    submitBtn.disabled = true;
    submitBtn.textContent = "Sending…";
    errorEl.classList.add("hidden");
    try {
      await signInWithMagicLink(email);
      sentEmail.textContent = email;
      formWrap.classList.add("hidden");
      sentWrap.classList.remove("hidden");
    } catch (err) {
      errorEl.textContent = err.message || "Something went wrong. Please try again.";
      errorEl.classList.remove("hidden");
      submitBtn.disabled = false;
      submitBtn.textContent = "Send magic link";
    }
  }

  submitBtn.addEventListener("click", submit);
  emailInput.addEventListener("keydown", e => { if (e.key === "Enter") submit(); });

  resendBtn?.addEventListener("click", () => {
    formWrap.classList.remove("hidden");
    sentWrap.classList.add("hidden");
    submitBtn.disabled = false;
    submitBtn.textContent = "Send magic link";
  });
}
