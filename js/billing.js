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
