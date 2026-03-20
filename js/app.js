import { state } from "./state.js";
import { fetchSignals, fetchHistoryPage, fetchPendingSignals } from "./api.js";
import { refreshAnalytics, setupAnalytics } from "./analytics.js";
import {
  relativeDate,
  setMainTab,
  showLoading,
  showError,
  renderSignalsPanel,
  renderHistory,
  updateFilterUI,
  updateFilterBadge,
  updateHistoryCountUI,
  resetHistoryPagination,
  openBurgerDrawer,
  closeBurgerDrawer,
  renderBurgerDrawerPills,
} from "./ui.js";
import {
  getSession,
  onAuthStateChange,
  renderAuthScreen,
  attachAuthListeners,
} from "./auth.js";
import {
  fetchSubscription,
  pollSubscription,
  startCheckout,
  renderAccountMenu,
  attachAccountMenuListeners,
} from "./billing.js";

// ── Refresh all data ───────────────────────────────────────────
export async function refreshData() {
  try {
    // Reset pagination counters before fetching — filters stay untouched in state
    state.historyPage     = 0;
    state.historyLoaded   = [];
    state.historyFetching = false;

    const [signalsResult, histResult, pendingResult] = await Promise.all([fetchSignals(), fetchHistoryPage(0), fetchPendingSignals()]);
    state.signalsData   = signalsResult;
    state.historyTotal  = histResult.count;
    state.historyLoaded = histResult.data;
    state.histData      = state.historyLoaded;
    state.pendingData   = pendingResult;

    // Re-observe sentinel in case it was unobserved when all pages were loaded
    const sentinel = document.getElementById("history-sentinel");
    if (state.historyObserver && sentinel) state.historyObserver.observe(sentinel);

    const allRows = [...state.signalsData, ...state.histData];
    const latestRun = allRows.length
      ? new Date(Math.max(...allRows.map(r => new Date(r.created_at))))
      : new Date();
    const lastUpdatedText = "Last updated " + relativeDate(latestRun);
    document.getElementById("last-updated").textContent = lastUpdatedText;
    document.getElementById("last-updated-mobile").textContent = lastUpdatedText;

    // If "today" yields no signals, fall back to "this week" automatically
    if (state.activeDateSignals === "today") {
      const tz     = Intl.DateTimeFormat().resolvedOptions().timeZone;
      const todayD = new Date().toLocaleDateString("en-CA", { timeZone: tz });
      const hasToday = state.signalsData.some(r =>
        new Date(r.kickoff).toLocaleDateString("en-CA", { timeZone: tz }) === todayD
      );
      if (!hasToday) state.activeDateSignals = "week";
    }

    // Render whichever panel is currently visible
    if (state.mainTab === "history") renderHistory();
    else renderSignalsPanel();

    updateHistoryCountUI();
  } catch (err) {
    showError(err.message || String(err));
    document.getElementById("cards-container").innerHTML = "";
  }
}

// ── Pull-to-refresh (mobile only) ─────────────────────────────
(function setupPullToRefresh() {
  const THRESHOLD = 80;
  const MAX_PULL  = 110;
  const indicator = document.getElementById("ptr-indicator");
  if (!indicator) return;

  let startY     = 0;
  let pulling    = false;
  let refreshing = false;

  function setIndicator(translatePx, opacity, animated) {
    indicator.style.transition = animated ? "transform 0.3s ease, opacity 0.3s ease" : "none";
    indicator.style.transform  = `translateY(calc(-100% + ${translatePx}px))`;
    indicator.style.opacity    = String(opacity);
  }

  document.addEventListener("touchstart", e => {
    if (refreshing || window.scrollY !== 0) return;
    startY  = e.touches[0].clientY;
    pulling = true;
  }, { passive: true });

  document.addEventListener("touchmove", e => {
    if (!pulling || refreshing) return;
    const dy = e.touches[0].clientY - startY;
    if (dy <= 0) { pulling = false; return; }
    const travel  = Math.min(dy * 0.5, MAX_PULL);
    const opacity = Math.min(dy / THRESHOLD, 1);
    setIndicator(travel, opacity, false);
  }, { passive: true });

  document.addEventListener("touchend", async e => {
    if (!pulling || refreshing) { pulling = false; return; }
    const dy = e.changedTouches[0].clientY - startY;
    pulling = false;
    if (dy < THRESHOLD) {
      setIndicator(0, 0, true);
      return;
    }
    refreshing = true;
    setIndicator(MAX_PULL, 1, true);
    try {
      await refreshData();
    } finally {
      setIndicator(0, 0, true);
      refreshing = false;
    }
  }, { passive: true });
})();

// ── Bottom nav scroll-hide (mobile only) ──────────────────────
(function setupBottomNavScroll() {
  const nav = document.getElementById("bottom-nav");
  if (!nav) return;
  let lastY = 0, ticking = false;
  window.addEventListener("scroll", () => {
    if (ticking) return;
    ticking = true;
    requestAnimationFrame(() => {
      if (window.innerWidth < 768) {
        if (window.scrollY > lastY && window.scrollY > 60) {
          nav.classList.add("hide-nav");
        } else {
          nav.classList.remove("hide-nav");
        }
      }
      lastY = window.scrollY;
      ticking = false;
    });
  }, { passive: true });
})();

// ── Init ───────────────────────────────────────────────────────
async function init() {
  const params       = new URLSearchParams(window.location.search);
  const fromCheckout = params.get("checkout") === "success";

  // ── 1. Auth guard ─────────────────────────────────────────────
  const session = await getSession();
  if (!session) {
    document.body.innerHTML = renderAuthScreen();
    attachAuthListeners();
    onAuthStateChange((_event, s) => { if (s) init(); });
    return;
  }

  // ── 2. Subscription guard ─────────────────────────────────────
  let sub = await fetchSubscription(session.user.id);

  // Stripe webhook may not have fired yet — poll briefly before giving up
  if (fromCheckout && (!sub || !["active", "trialing"].includes(sub.status))) {
    showLoading();
    sub = await pollSubscription(session.user.id, 8, 1500);
  }

  if (!sub || !["active", "trialing"].includes(sub.status)) {
    await startCheckout();
    return;
  }

  if (fromCheckout) {
    history.replaceState(null, "", window.location.pathname);
  }

  // ── 3. Mount account menu on the account icon(s) ──────────────
  // Two buttons exist (mobile + desktop) but only one is visible at a time.
  document.querySelectorAll(".account-btn").forEach(btn => {
    btn.addEventListener("click", e => {
      e.stopPropagation();
      const existing = document.getElementById("account-dropdown");
      if (existing) { existing.remove(); return; }
      btn.closest("div.relative").insertAdjacentHTML("beforeend", renderAccountMenu(session, sub));
      attachAccountMenuListeners(sub);
    });
  });
  document.addEventListener("click", () => {
    document.getElementById("account-dropdown")?.remove();
  });

  setMainTab("signals");

  if (fromCheckout) {
    const banner = document.getElementById("welcome-banner");
    if (banner) {
      banner.classList.remove("hidden");
      document.getElementById("welcome-dismiss")?.addEventListener("click", () => banner.classList.add("hidden"));
      setTimeout(() => banner.classList.add("hidden"), 8000);
    }
  }

  // ── Smart Signal onboarding modal (shown once per user) ───────
  if (!localStorage.getItem("smart_signal_ack")) {
    const modal = document.getElementById("smart-signal-modal");
    if (modal) {
      modal.classList.remove("hidden");
      // Block Escape — modal is intentionally un-skippable
      const blockEsc = e => { if (e.key === "Escape") e.stopImmediatePropagation(); };
      document.addEventListener("keydown", blockEsc, true);
      document.getElementById("smart-modal-ack")?.addEventListener("click", () => {
        localStorage.setItem("smart_signal_ack", "1");
        modal.classList.add("hidden");
        document.removeEventListener("keydown", blockEsc, true);
      }, { once: true });
    }
  }

  setupAnalytics();
  function resetAndRefresh() {
    resetHistoryPagination();
  }

  // Desktop tab buttons
  document.querySelectorAll(".main-tab-btn").forEach(btn =>
    btn.addEventListener("click", () => {
      setMainTab(btn.dataset.main);
      if (btn.dataset.main === "analytics") refreshAnalytics();
    })
  );

  // History status tabs (won / lost / settled)
  document.querySelectorAll(".hist-status-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      state.histStatusFilter = btn.dataset.status;
      state.histSortCol      = "kickoff";
      state.histSortDir      = "desc";
      renderHistory();
    });
  });

  // Reset filters from signals empty state
  document.getElementById("panel-signals").addEventListener("click", e => {
    if (!e.target.closest("[data-action='signals-reset-filters']")) return;
    state.activeSport      = "all";
    state.activeLeague     = "all";
    state.activeSignalType = "all";
    state.teamSearch       = "";
    document.getElementById("team-search").value        = "";
    document.getElementById("team-search-mobile").value = "";
    updateFilterUI();
    renderBurgerDrawerPills();
    renderSignalsPanel();
  });

  // Reset filters from history empty state
  document.getElementById("panel-history").addEventListener("click", e => {
    if (!e.target.closest("[data-action='hist-reset-filters']")) return;
    state.activeSport      = "all";
    state.activeLeague     = "all";
    state.activeSignalType = "all";
    state.teamSearch       = "";
    state.activeDateHist   = "all";
    document.getElementById("team-search").value        = "";
    document.getElementById("team-search-mobile").value = "";
    updateFilterUI();
    renderBurgerDrawerPills();
    resetAndRefresh();
  });

  // Filters button (desktop) opens the same left drawer as the mobile burger
  document.getElementById("filters-toggle").addEventListener("click", openBurgerDrawer);

  // Shared backdrop
  document.getElementById("filter-backdrop").addEventListener("click", closeBurgerDrawer);

  // Burger drawer (mobile)
  document.getElementById("burger-btn").addEventListener("click", openBurgerDrawer);
  document.getElementById("burger-drawer-close").addEventListener("click", closeBurgerDrawer);
  document.getElementById("burger-reset-btn").addEventListener("click", () => {
    state.activeSport              = "all";
    state.activeLeague             = "all";
    state.activeSignalType         = "all";
    state.teamSearch               = "";
    state.activeDateSignals        = "today";
    state.activeDateHist           = "all";
    state.analyticsActiveDateRange = "all";

    document.getElementById("team-search").value = "";
    document.getElementById("team-search-mobile").value = "";
    updateFilterUI();
    renderBurgerDrawerPills();
    if (state.mainTab === "analytics") refreshAnalytics();
    else { renderSignalsPanel(); resetAndRefresh(); }
    closeBurgerDrawer();
  });

  // Bottom nav tabs (mobile)
  document.querySelectorAll(".bottom-nav-btn").forEach(btn =>
    btn.addEventListener("click", () => {
      setMainTab(btn.dataset.main);
      if (btn.dataset.main === "analytics") refreshAnalytics();
    })
  );

  // Desktop team search
  document.getElementById("team-search").addEventListener("input", e => {
    state.teamSearch = e.target.value.trim();
    updateFilterBadge();
    renderSignalsPanel();
    resetAndRefresh();
  });

  // Mobile team search (header)
  document.getElementById("team-search-mobile").addEventListener("input", e => {
    state.teamSearch = e.target.value.trim();
    document.getElementById("team-search").value = state.teamSearch;
    updateFilterBadge();
    renderSignalsPanel();
    resetAndRefresh();
  });

  // Chip remove (delegated on both chip containers)
  function removeFilterChip(key) {
    if (key === "sport") {
      state.activeSport      = "all";
      state.activeLeague     = "all";
      state.activeSignalType = "all";
    }
    if (key === "league")      { state.activeLeague      = "all"; }
    if (key === "signaltype")  { state.activeSignalType  = "all"; }
    if (key === "team")        {
      state.teamSearch = "";
      document.getElementById("team-search").value = "";
      document.getElementById("team-search-mobile").value = "";
    }
    if (key === "datehist")    { state.activeDateHist    = "all"; }
    if (key === "analyticsdate")  { state.analyticsActiveDateRange = "all"; }
    updateFilterUI();
    renderBurgerDrawerPills();
    if (state.mainTab === "analytics") refreshAnalytics();
    else { renderSignalsPanel(); resetAndRefresh(); }
  }
  ["active-filter-chips", "active-filter-chips-mobile"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener("click", e => {
      const btn = e.target.closest(".chip-remove");
      if (btn) removeFilterChip(btn.dataset.filterKey);
    });
  });

  // Infinite scroll for history
  const sentinel = document.getElementById("history-sentinel");
  state.historyObserver = new IntersectionObserver(async (entries) => {
    if (!entries[0].isIntersecting) return;
    if (state.historyFetching || state.historyLoaded.length >= state.historyTotal) return;
    state.historyFetching = true;
    document.getElementById("history-spinner").classList.remove("hidden");
    try {
      state.historyPage++;
      const { data } = await fetchHistoryPage(state.historyPage);
      state.historyLoaded = [...state.historyLoaded, ...data];
      state.histData      = state.historyLoaded;
      renderHistory();
      updateHistoryCountUI();
    } catch (err) {
      showError(err.message || String(err));
    }
    state.historyFetching = false;
  }, { rootMargin: "200px" });
  state.historyObserver.observe(sentinel);

  // Desktop refresh button
  const refreshBtn  = document.getElementById("refresh-btn");
  const refreshIcon = document.getElementById("refresh-icon");
  refreshBtn.addEventListener("click", async () => {
    if (refreshIcon.classList.contains("spinning")) return;
    refreshIcon.classList.add("spinning");
    refreshBtn.disabled = true;
    try {
      await refreshData();
    } finally {
      refreshIcon.classList.remove("spinning");
      refreshBtn.disabled = false;
    }
  });

  showLoading();
  await refreshData();
}

init();

// Global fixed tooltip for .info-icon elements (avoids overflow-x-auto clipping)
const _gtt = document.getElementById("global-tooltip");
document.addEventListener("mouseenter", e => {
  if (!(e.target instanceof Element)) return;
  const icon = e.target.closest(".info-icon");
  if (!icon || !_gtt) return;
  const bubble = icon.querySelector("span");
  if (!bubble) return;
  _gtt.textContent = bubble.textContent;
  _gtt.classList.add("visible");
  const ir = icon.getBoundingClientRect();
  const gap = 6;
  // Measure tooltip to decide above vs below
  _gtt.style.top = "0"; _gtt.style.left = "0";
  const tw = _gtt.offsetWidth, th = _gtt.offsetHeight;
  // Clamp to the nearest scrollable ancestor's right edge (e.g. overflow-x-auto table)
  let rightBound = window.innerWidth - 4;
  let el = icon.parentElement;
  while (el && el !== document.body) {
    const ox = window.getComputedStyle(el).overflowX;
    if (ox === "auto" || ox === "scroll" || ox === "hidden") {
      rightBound = Math.min(rightBound, el.getBoundingClientRect().right - 4);
      break;
    }
    el = el.parentElement;
  }
  const left = Math.min(Math.max(ir.left + ir.width / 2 - tw / 2, 4), rightBound - tw);
  const top = ir.top - th - gap < 0 ? ir.bottom + gap : ir.top - th - gap;
  _gtt.style.left = left + "px";
  _gtt.style.top  = top  + "px";
}, true);
document.addEventListener("mouseleave", e => {
  if (!(e.target instanceof Element) || !e.target.closest(".info-icon") || !_gtt) return;
  _gtt.classList.remove("visible");
}, true);

// Tennis set-score tooltip — works on both hover and tap
function _showTennisTip(el) {
  if (!_gtt) return;
  const tip = el.dataset.tip;
  if (!tip) return;
  _gtt.textContent = tip;
  _gtt.classList.add("visible");
  _gtt.style.top = "0"; _gtt.style.left = "0";
  const tw = _gtt.offsetWidth, th = _gtt.offsetHeight;
  const ir = el.getBoundingClientRect();
  const gap = 6;
  const left = Math.min(Math.max(ir.left + ir.width / 2 - tw / 2, 4), window.innerWidth - tw - 4);
  const top  = ir.top - th - gap < 0 ? ir.bottom + gap : ir.top - th - gap;
  _gtt.style.left = left + "px";
  _gtt.style.top  = top  + "px";
}
const _TIP_SEL = ".tennis-score-tip, .signal-label-tip";
let _tipHideTimer = null;
let _lastTipTouch = 0;
document.addEventListener("mouseenter", e => {
  if (Date.now() - _lastTipTouch < 500) return;
  if (!(e.target instanceof Element)) return;
  const el = e.target.closest(_TIP_SEL);
  if (el) _showTennisTip(el);
}, true);
document.addEventListener("mouseleave", e => {
  if (Date.now() - _lastTipTouch < 500) return;
  if (e.target instanceof Element && e.target.closest(_TIP_SEL) && _gtt) _gtt.classList.remove("visible");
}, true);
document.addEventListener("touchend", e => {
  const el = e.target.closest(_TIP_SEL);
  if (!el || !_gtt) return;
  e.preventDefault(); // block synthesised click (prevents Winamax link opening on score tap)
  _lastTipTouch = Date.now();
  clearTimeout(_tipHideTimer);
  _showTennisTip(el);
  _tipHideTimer = setTimeout(() => _gtt.classList.remove("visible"), 2500);
}, true);
