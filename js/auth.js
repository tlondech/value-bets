import { sb } from "./config.js";

// ── Auth helpers ───────────────────────────────────────────────

export async function getSession() {
  const { data: { session } } = await sb.auth.getSession();
  return session;
}

export async function signInWithMagicLink(email) {
  const { error } = await sb.auth.signInWithOtp({
    email,
    options: {},
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

export function renderAuthScreen() {
  const sampleCard1 = `
    <div class="relative pointer-events-none select-none">
      <span class="absolute top-2 right-2 z-10 text-[10px] font-bold uppercase tracking-wider bg-indigo-600/80 text-white px-1.5 py-0.5 rounded">Sample</span>
      <div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden opacity-90">
        <div class="flex items-start justify-between px-4 py-2.5 bg-gray-50 dark:bg-gray-800/60 border-b border-gray-200 dark:border-gray-700">
          <div class="flex flex-wrap items-center gap-2 mr-3">
            <span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300">Ligue 1 • MD 28/34</span>
          </div>
          <span class="text-sm font-semibold tabular-nums text-gray-500 dark:text-gray-400">21:00</span>
        </div>
        <div class="px-4 py-3 space-y-2">
          <div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-1.5 min-w-0">
              <img src="https://crests.football-data.org/524.png" alt="PSG" class="w-7 h-7 object-contain flex-shrink-0">
              <span class="font-semibold truncate">PSG</span>
            </div>
            <div class="flex gap-1">
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-gray-400 dark:bg-gray-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-red-500 inline-block w-2.5 h-2.5 rounded-full"></span>
            </div>
          </div>
          <div class="flex items-center justify-between gap-2">
            <div class="flex items-center gap-1.5 min-w-0">
              <img src="https://crests.football-data.org/516.png" alt="Marseille" class="w-7 h-7 object-contain flex-shrink-0">
              <span class="font-semibold truncate">Marseille</span>
            </div>
            <div class="flex gap-1">
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-red-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-green-500 inline-block w-2.5 h-2.5 rounded-full"></span>
              <span class="bg-gray-400 dark:bg-gray-500 inline-block w-2.5 h-2.5 rounded-full"></span>
            </div>
          </div>
        </div>
        <div class="px-4 pb-3 border-t border-gray-100 dark:border-gray-800 pt-3">
          <table class="w-full text-sm table-fixed">
            <thead>
              <tr class="text-xs text-gray-400 uppercase">
                <th class="w-[32%] pb-1 pr-2 text-left font-medium">Bet</th>
                <th class="pb-1 pr-2 text-right font-medium">Odds</th>
                <th class="pb-1 pr-2 text-right font-medium">Prob</th>
                <th class="pb-1 text-right font-medium">EV</th>
              </tr>
            </thead>
            <tbody>
              <tr class="border-t border-gray-100 dark:border-gray-700/50">
                <td class="py-1.5 pr-2"><span class="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300">Home Win</span></td>
                <td class="py-1.5 pr-2 text-right font-mono text-sm">1.85</td>
                <td class="py-1.5 pr-2 text-right text-sm text-gray-500 dark:text-gray-400">62.3%</td>
                <td class="py-1.5 text-right text-sm font-semibold"><span class="ev-warning">+14.7%</span></td>
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
            <span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap bg-teal-100 text-teal-800 dark:bg-teal-900/40 dark:text-teal-300">Roland Garros · SF</span>
            <span class="inline-block px-2 py-0.5 rounded text-xs font-semibold whitespace-nowrap bg-orange-100 text-orange-800 dark:bg-orange-900/40 dark:text-orange-300">Clay</span>
          </div>
          <span class="text-sm font-semibold tabular-nums text-gray-500 dark:text-gray-400">14:00</span>
        </div>
        <div class="px-4 py-3 space-y-2">
          <div class="flex items-center gap-1.5 min-w-0">
            <img src="https://flagcdn.com/w40/it.png" alt="Italy" class="w-7 h-7 object-contain flex-shrink-0">
            <span class="font-semibold">Sinner</span>
          </div>
          <div class="flex items-center gap-1.5 min-w-0">
            <img src="https://flagcdn.com/w40/es.png" alt="Spain" class="w-7 h-7 object-contain flex-shrink-0">
            <span class="font-semibold">Alcaraz</span>
          </div>
        </div>
        <div class="px-4 pb-3 border-t border-gray-100 dark:border-gray-800 pt-3">
          <table class="w-full text-sm table-fixed">
            <thead>
              <tr class="text-xs text-gray-400 uppercase">
                <th class="w-[32%] pb-1 pr-2 text-left font-medium">Bet</th>
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
                <th class="w-[32%] pb-1 pr-2 text-left font-medium">Bet</th>
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
    <div class="min-h-screen bg-gray-950 text-gray-100">
      <div class="max-w-6xl mx-auto px-6 py-12 md:py-20 flex flex-col md:flex-row md:items-start gap-12 md:gap-16">

        <!-- LEFT PANEL -->
        <div class="flex-1 max-w-md">

          <!-- Hero -->
          <div class="mb-10">
            <div class="text-xs font-semibold text-indigo-400 uppercase tracking-widest mb-3">Daily Value Bets</div>
            <h1 class="text-3xl font-extrabold mb-3 leading-tight text-white">Find value bets before the bookmakers close them</h1>
            <p class="text-gray-400 text-base leading-relaxed">Statistical models surface mispriced odds across football, basketball and tennis.</p>
          </div>

          <!-- How it works -->
          <div class="mb-8">
            <p class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-4">How it works</p>
            <div class="flex gap-3">
              <div class="flex-1 bg-gray-900 rounded-xl p-3.5 border border-gray-800 text-center">
                <div class="text-xl mb-1.5">🧮</div>
                <div class="text-xs font-semibold text-gray-300 mb-1">Models run</div>
                <div class="text-xs text-gray-600">Probability estimates per match</div>
              </div>
              <div class="flex-1 bg-gray-900 rounded-xl p-3.5 border border-gray-800 text-center">
                <div class="text-xl mb-1.5">📊</div>
                <div class="text-xs font-semibold text-gray-300 mb-1">EV calculated</div>
                <div class="text-xs text-gray-600">Against live bookmaker lines</div>
              </div>
              <div class="flex-1 bg-gray-900 rounded-xl p-3.5 border border-gray-800 text-center">
                <div class="text-xl mb-1.5">🎯</div>
                <div class="text-xs font-semibold text-gray-300 mb-1">Best bets surfaced</div>
                <div class="text-xs text-gray-600">With links and ROI tracking</div>
              </div>
            </div>
          </div>

          <!-- Feature list -->
          <ul class="mb-8 space-y-2.5">
            <li class="flex items-start gap-2.5 text-sm text-gray-300">
              <span class="text-green-400 mt-0.5 shrink-0">✓</span>Football, basketball &amp; tennis — all in one place
            </li>
            <li class="flex items-start gap-2.5 text-sm text-gray-300">
              <span class="text-green-400 mt-0.5 shrink-0">✓</span>Dixon-Coles, Elo, and Gaussian models
            </li>
            <li class="flex items-start gap-2.5 text-sm text-gray-300">
              <span class="text-green-400 mt-0.5 shrink-0">✓</span>Full bet history with ROI tracking
            </li>
            <li class="flex items-start gap-2.5 text-sm text-gray-300">
              <span class="text-green-400 mt-0.5 shrink-0">✓</span>Direct bookmaker links on every bet
            </li>
          </ul>

          <!-- Sign-in form -->
          <div id="auth-form-wrap">
            <label class="block text-sm font-medium text-gray-300 mb-1.5" for="auth-email">
              Email address
            </label>
            <input id="auth-email" type="email" autocomplete="email" inputmode="email"
              placeholder="you@example.com"
              class="w-full px-3 py-2 rounded-lg border border-gray-700 bg-gray-900 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:ring-2 focus:ring-indigo-400 mb-3" />
            <button id="auth-submit-btn"
              class="w-full py-2.5 rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-semibold transition-colors disabled:opacity-50 disabled:cursor-not-allowed">
              Send magic link
            </button>
            <p id="auth-error" class="hidden mt-3 text-xs text-red-400 text-center"></p>
            <p class="text-xs text-gray-700 mt-2.5 text-center">7-day free trial · cancel anytime · card required</p>
          </div>

          <div id="auth-sent-wrap" class="hidden text-center">
            <div class="text-4xl mb-4">📬</div>
            <p class="text-sm font-medium text-gray-200 mb-1">Check your inbox</p>
            <p class="text-xs text-gray-500">We sent a sign-in link to <strong id="auth-sent-email" class="text-gray-300"></strong>. Click it to log in.</p>
            <button id="auth-resend-btn" class="mt-5 text-xs text-indigo-400 hover:underline">Send a new link</button>
          </div>

          <p class="text-xs text-gray-700 mt-8 leading-relaxed">
            For informational purposes only. Not financial or betting advice. Bet responsibly. ·
            <a href="terms.html" class="hover:text-gray-500 transition-colors">Terms</a> ·
            <a href="privacy.html" class="hover:text-gray-500 transition-colors">Privacy</a>
          </p>
        </div>

        <!-- RIGHT PANEL (desktop only) -->
        <div class="hidden md:flex flex-col gap-4 w-96 shrink-0 mt-4">
          <p class="text-xs font-semibold text-gray-600 uppercase tracking-wider mb-1">Example bets</p>
          ${sampleCard1}
          ${sampleCard3}
          ${sampleCard2}
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
