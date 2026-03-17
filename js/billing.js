import { sb, SUPABASE_URL } from "./config.js";
import { signOut } from "./auth.js";

const CREATE_CHECKOUT_FN = `${SUPABASE_URL}/functions/v1/create-checkout-session`;

// ── Subscription helpers ───────────────────────────────────────

export async function fetchSubscription(userId) {
  const { data } = await sb
    .from("subscriptions")
    .select("status, current_period_end, trial_used")
    .eq("user_id", userId)
    .single();
  return data; // null if no row yet
}

// ── Paywall screen ─────────────────────────────────────────────

export function renderPaywall(session) {
  return `
    <div class="min-h-screen bg-gray-100 dark:bg-gray-950 flex items-center justify-center px-4">
      <div class="w-full max-w-sm bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-2xl px-8 py-10 shadow-sm text-center">
        <div class="text-4xl mb-4">⚡</div>
        <h1 class="text-xl font-bold text-gray-900 dark:text-gray-100 mb-2">Subscribe to unlock</h1>
        <p class="text-sm text-gray-500 dark:text-gray-400 mb-6">
          Get daily statistical value bets across football, basketball, and tennis — powered by Dixon-Coles, Elo, and Gaussian models.
        </p>

        <ul class="text-left space-y-2 text-sm text-gray-600 dark:text-gray-400 mb-8">
          <li class="flex items-start gap-2"><span class="text-green-500 mt-0.5">✓</span> Football, tennis &amp; NBA value bets</li>
          <li class="flex items-start gap-2"><span class="text-green-500 mt-0.5">✓</span> Updated 4× daily via live odds</li>
          <li class="flex items-start gap-2"><span class="text-green-500 mt-0.5">✓</span> Full history &amp; ROI tracker</li>
          <li class="flex items-start gap-2"><span class="text-green-500 mt-0.5">✓</span> Bookmaker links on every bet</li>
        </ul>

        <button id="subscribe-btn"
          class="w-full py-2.5 rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-semibold transition-colors disabled:opacity-50 disabled:cursor-not-allowed mb-3">
          Start 7-day free trial
        </button>
        <p class="text-xs text-gray-400 dark:text-gray-500 mb-6">Card required. Cancel any time.</p>

        <p id="paywall-error" class="hidden text-xs text-red-500 mb-4"></p>

        <p class="text-xs text-gray-400 dark:text-gray-500">
          Signed in as ${escHtml(session.user.email)}.
          <button id="paywall-signout-btn" class="text-indigo-500 hover:underline ml-1">Sign out</button>
        </p>

        <p class="text-xs text-gray-400 dark:text-gray-600 mt-6 leading-relaxed">
          For informational purposes only. Not financial or betting advice. Bet responsibly.
        </p>
      </div>
    </div>`;
}

export function attachPaywallListeners(session) {
  const subscribeBtn = document.getElementById("subscribe-btn");
  const errorEl      = document.getElementById("paywall-error");

  subscribeBtn?.addEventListener("click", async () => {
    subscribeBtn.disabled = true;
    subscribeBtn.textContent = "Redirecting…";
    errorEl.classList.add("hidden");
    try {
      const res = await fetch(CREATE_CHECKOUT_FN, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${session.access_token}`,
        },
      });
      if (!res.ok) throw new Error(await res.text());
      const { url } = await res.json();
      window.location.href = url;
    } catch (err) {
      errorEl.textContent = err.message || "Could not start checkout. Please try again.";
      errorEl.classList.remove("hidden");
      subscribeBtn.disabled = false;
      subscribeBtn.textContent = "Start 7-day free trial";
    }
  });

  document.getElementById("paywall-signout-btn")?.addEventListener("click", signOut);
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

export function attachAccountMenuListeners(session, sub) {
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
      const res = await fetch(CREATE_CHECKOUT_FN, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${session.access_token}`,
        },
      });
      if (res.ok) {
        const { url } = await res.json();
        window.location.href = url;
      }
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
