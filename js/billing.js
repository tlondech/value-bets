import { sb, TRIAL_DAYS } from "./config.js";
import { signOut } from "./auth.js";

// ── Subscription helpers ───────────────────────────────────────

export async function fetchSubscription(userId) {
  const { data } = await sb
    .from("subscriptions")
    .select("status, current_period_end, trial_used")
    .eq("user_id", userId)
    .maybeSingle();
  return data; // null if no row yet
}

// Retries fetchSubscription until the subscription is active/trialing.
// Used after Stripe checkout to wait for the webhook to fire.
export async function pollSubscription(userId, retries = 8, intervalMs = 1500) {
  for (let i = 0; i < retries; i++) {
    if (i > 0) await new Promise(r => setTimeout(r, intervalMs));
    const sub = await fetchSubscription(userId);
    if (sub && ["active", "trialing"].includes(sub.status)) return sub;
  }
  return null;
}

// ── Auto-redirect to Stripe Checkout ──────────────────────────

export async function startCheckout() {
  document.body.innerHTML = `
    <div class="min-h-screen bg-gray-950 flex flex-col items-center justify-center px-6 text-center">
      <div class="w-full max-w-xs">
        <div class="spinner mx-auto mb-8"></div>
        <h1 class="text-lg font-bold text-white mb-1">Starting your free trial</h1>
        <p class="text-sm text-gray-400 mb-2">${TRIAL_DAYS} days free, then €19.99/month. Cancel any time.</p>
        <p class="text-xs text-gray-600 mb-8">You won't be charged until your trial ends.</p>
        <p id="checkout-error" class="hidden text-xs text-red-400 mb-6"></p>
        <p class="text-xs text-gray-700 leading-relaxed">
          For informational purposes only. Not financial or wagering advice. Participate responsibly.
        </p>
      </div>
    </div>`;

  try {
    const { data, error } = await sb.functions.invoke("create-checkout-session");
    if (error) throw error;
    window.location.href = data.url;
  } catch (err) {
    const errorEl = document.getElementById("checkout-error");
    errorEl.textContent = (err.message || "Could not start checkout.") + " Please try again.";
    errorEl.classList.remove("hidden");
    document.querySelector(".spinner").classList.add("hidden");
  }
}

// ── Path A — New user welcome screen ──────────────────────────

export function renderNewUserWelcome(session) {
  const email = escHtml(session?.user?.email || "");
  return `
    <div class="min-h-screen bg-gray-950 flex flex-col items-center justify-center px-6 text-center">
      <div class="w-full max-w-sm">
        <div class="mb-8">
          <h1 class="text-3xl font-extrabold text-white mb-2">Welcome to Signal Arena</h1>
          <p class="text-gray-400 text-sm">Predictive model outputs for football, tennis &amp; NBA.</p>
        </div>
        <div class="bg-gray-900 border border-gray-800 rounded-2xl p-6 mb-6 text-left">
          <p class="text-sm font-semibold text-white mb-3">What you get</p>
          <ul class="space-y-1.5 text-sm text-gray-400">
            <li>&#x2713; Live +EV signals from predictive models</li>
            <li>&#x2713; Odds, probabilities &amp; EV for every signal</li>
            <li>&#x2713; Football, tennis &amp; NBA coverage</li>
            <li>&#x2713; Full history &amp; analytics dashboard</li>
          </ul>
          <div class="mt-4 pt-4 border-t border-gray-800 text-xs text-gray-500">
            ${TRIAL_DAYS} days free &middot; then €19.99/month &middot; cancel any time
          </div>
        </div>
        <button id="new-user-trial-btn"
          class="w-full py-3.5 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white font-semibold text-sm transition-colors">
          Start my free trial
        </button>
        <p id="new-user-error" class="hidden text-xs text-red-400 mt-3"></p>
        <p class="text-xs text-gray-700 mt-4">Card required. You won't be charged until your trial ends.</p>
        <p class="text-xs text-gray-700 mt-6">Signed in as ${email} &middot; <button id="new-user-signout" class="underline hover:text-gray-500">Sign out</button></p>
      </div>
    </div>`;
}

export function attachNewUserWelcomeListeners() {
  document.getElementById("new-user-trial-btn")?.addEventListener("click", async () => {
    const btn = document.getElementById("new-user-trial-btn");
    btn.textContent = "Redirecting to checkout\u2026";
    btn.disabled = true;
    try {
      const { data, error } = await sb.functions.invoke("create-checkout-session");
      if (error) throw error;
      window.location.href = data.url;
    } catch (err) {
      const errorEl = document.getElementById("new-user-error");
      errorEl.textContent = (err.message || "Could not start checkout.") + " Please try again.";
      errorEl.classList.remove("hidden");
      btn.textContent = "Start my free trial";
      btn.disabled = false;
    }
  });
  document.getElementById("new-user-signout")?.addEventListener("click", signOut);
}

// ── Path B — Expired trial paywall ────────────────────────────

export function renderExpiredPaywall(session) {
  const email = escHtml(session?.user?.email || "");
  return `
    <div class="min-h-screen bg-gray-950 flex flex-col items-center justify-center px-6 text-center">
      <div class="w-full max-w-sm">
        <div class="mb-8">
          <div class="w-12 h-12 rounded-full bg-gray-800 flex items-center justify-center mx-auto mb-4">
            <svg class="w-6 h-6 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
              <path stroke-linecap="round" stroke-linejoin="round" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"/>
            </svg>
          </div>
          <h1 class="text-2xl font-extrabold text-white mb-2">Your free trial has ended</h1>
          <p class="text-gray-400 text-sm">Our models are analysing today's fixtures &mdash; subscribe to see today's signals.</p>
        </div>
        <button id="expired-subscribe-btn"
          class="w-full py-3.5 rounded-xl bg-indigo-600 hover:bg-indigo-700 text-white font-semibold text-sm transition-colors mb-3">
          Subscribe &middot; &euro;19.99/month
        </button>
        <button id="expired-browse-btn"
          class="w-full py-3 rounded-xl border border-gray-700 text-gray-400 hover:text-white hover:border-gray-600 text-sm transition-colors">
          Browse with limited access &rarr;
        </button>
        <p id="expired-error" class="hidden text-xs text-red-400 mt-3"></p>
        <p class="text-xs text-gray-700 mt-6">Signed in as ${email} &middot; <button id="expired-signout" class="underline hover:text-gray-500">Sign out</button></p>
      </div>
    </div>`;
}

export function attachExpiredPaywallListeners(onBrowse) {
  document.getElementById("expired-subscribe-btn")?.addEventListener("click", async () => {
    const btn = document.getElementById("expired-subscribe-btn");
    btn.textContent = "Redirecting\u2026";
    btn.disabled = true;
    try {
      const { data, error } = await sb.functions.invoke("create-checkout-session");
      if (error) throw error;
      window.location.href = data.url;
    } catch (err) {
      const errorEl = document.getElementById("expired-error");
      errorEl.textContent = (err.message || "Could not start checkout.") + " Please try again.";
      errorEl.classList.remove("hidden");
      btn.textContent = "Subscribe \u00b7 \u20ac19.99/month";
      btn.disabled = false;
    }
  });
  document.getElementById("expired-browse-btn")?.addEventListener("click", onBrowse);
  document.getElementById("expired-signout")?.addEventListener("click", signOut);
}

// ── Path C — Trial countdown banner ───────────────────────────

export function renderTrialBanner(sub) {
  const daysLeft = Math.ceil((new Date(sub.current_period_end) - Date.now()) / 86400000);
  const dayWord  = daysLeft === 1 ? "day" : "days";
  return `
    <div id="trial-banner" class="bg-indigo-600 text-white text-sm text-center py-2 px-4 flex items-center justify-center gap-3 flex-wrap">
      <span>Trial active &mdash; <strong>${daysLeft} ${dayWord} remaining</strong></span>
      <a href="https://billing.stripe.com/p/login/eVq14o0pj88nfmafkL63K00" target="_blank" rel="noopener noreferrer"
        class="underline font-semibold whitespace-nowrap">Upgrade now</a>
      <button id="trial-banner-dismiss" class="ml-2 text-white/60 hover:text-white text-lg leading-none">&times;</button>
    </div>`;
}

// ── Account menu (rendered into the header) ───────────────────

export function renderAccountMenu(session, sub) {
  const email      = escHtml(session?.user?.email || "");
  const isActive   = sub && ["active", "trialing"].includes(sub.status);
  const statusText = sub ? capitalise(sub.status) : "No subscription";
  const statusCls  = isActive ? "text-green-500" : "text-gray-400";

  return `
    <div id="account-dropdown"
      class="absolute right-0 top-full mt-2 w-56 bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-xl shadow-lg z-50 py-1 text-sm">
      <div class="px-4 py-3 border-b border-gray-100 dark:border-gray-800">
        <p class="font-medium text-gray-900 dark:text-gray-100 truncate">${email}</p>
        <p class="text-xs ${statusCls} mt-0.5">${statusText}</p>
      </div>
      ${isActive ? `
      <button id="billing-portal-btn"
        class="w-full text-left px-4 py-2.5 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
        Manage billing
      </button>` : `
      <button id="subscribe-from-menu-btn"
        class="w-full text-left px-4 py-2.5 text-indigo-600 dark:text-indigo-400 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors font-medium">
        Subscribe
      </button>`}
      <button id="account-signout-btn"
        class="w-full text-left px-4 py-2.5 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
        Sign out
      </button>
    </div>`;
}

export function attachAccountMenuListeners(sub) {
  const isActive = sub && ["active", "trialing"].includes(sub.status);

  document.getElementById("account-signout-btn")?.addEventListener("click", signOut);

  if (isActive) {
    document.getElementById("billing-portal-btn")?.addEventListener("click", () => {
      window.open("https://billing.stripe.com/p/login/eVq14o0pj88nfmafkL63K00", "_blank");
    });
  } else {
    document.getElementById("subscribe-from-menu-btn")?.addEventListener("click", async () => {
      const btn = document.getElementById("subscribe-from-menu-btn");
      btn.textContent = "Redirecting…";
      btn.disabled = true;
      const { data } = await sb.functions.invoke("create-checkout-session");
      if (data?.url) window.location.href = data.url;
    });
  }
}

// ── Utilities ──────────────────────────────────────────────────

function escHtml(s) {
  return String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
function capitalise(s) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : "";
}
