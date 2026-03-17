import { sb } from "./config.js";

// ── Auth helpers ───────────────────────────────────────────────

export async function getSession() {
  const { data: { session } } = await sb.auth.getSession();
  return session;
}

export async function signInWithMagicLink(email) {
  const { error } = await sb.auth.signInWithOtp({
    email,
    options: { emailRedirectTo: window.location.origin + window.location.pathname },
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
  return `
    <div class="min-h-screen bg-gray-100 dark:bg-gray-950 flex items-center justify-center px-4">
      <div class="w-full max-w-sm bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-800 rounded-2xl px-8 py-10 shadow-sm">
        <h1 class="text-xl font-bold text-gray-900 dark:text-gray-100 mb-1">Daily Value Bets</h1>
        <p class="text-sm text-gray-500 dark:text-gray-400 mb-8">Sign in to access your subscription.</p>

        <div id="auth-form-wrap">
          <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1.5" for="auth-email">
            Email address
          </label>
          <input id="auth-email" type="email" autocomplete="email" inputmode="email"
            placeholder="you@example.com"
            class="w-full px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-400 mb-4" />
          <button id="auth-submit-btn"
            class="w-full py-2.5 rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white text-sm font-semibold transition-colors disabled:opacity-50 disabled:cursor-not-allowed">
            Send magic link
          </button>
          <p id="auth-error" class="hidden mt-3 text-xs text-red-500 text-center"></p>
        </div>

        <div id="auth-sent-wrap" class="hidden text-center">
          <div class="text-4xl mb-4">📬</div>
          <p class="text-sm font-medium text-gray-800 dark:text-gray-200 mb-1">Check your inbox</p>
          <p class="text-xs text-gray-500 dark:text-gray-400">We sent a sign-in link to <strong id="auth-sent-email"></strong>. Click it to log in.</p>
          <button id="auth-resend-btn" class="mt-5 text-xs text-indigo-500 hover:underline">Send a new link</button>
        </div>

        <p class="text-xs text-gray-400 dark:text-gray-600 mt-8 text-center leading-relaxed">
          For informational purposes only. Not financial or betting advice. Bet responsibly.
        </p>
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
