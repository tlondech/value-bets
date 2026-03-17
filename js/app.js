import { state } from "./state.js";
import { fetchBets, fetchHistoryPage } from "./api.js";
import {
  relativeDate,
  setMainTab,
  showLoading,
  showError,
  renderBetsPanel,
  renderHistory,
  updateFilterUI,
  updateFilterBadge,
  updateHistoryCountUI,
  resetHistoryPagination,
  openDrawer,
  closeDrawer,
  openBurgerDrawer,
  closeBurgerDrawer,
} from "./ui.js";
import {
  getSession,
  onAuthStateChange,
  renderAuthScreen,
  attachAuthListeners,
} from "./auth.js";
import {
  fetchSubscription,
  renderPaywall,
  attachPaywallListeners,
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

    const [betsResult, histResult] = await Promise.all([fetchBets(), fetchHistoryPage(0)]);
    state.betsData      = betsResult;
    state.historyTotal  = histResult.count;
    state.historyLoaded = histResult.data;
    state.histData      = state.historyLoaded;

    // Re-observe sentinel in case it was unobserved when all pages were loaded
    const sentinel = document.getElementById("history-sentinel");
    if (state.historyObserver && sentinel) state.historyObserver.observe(sentinel);

    const allRows = [...state.betsData, ...state.histData];
    const latestRun = allRows.length
      ? new Date(Math.max(...allRows.map(r => new Date(r.created_at))))
      : new Date();
    const lastUpdatedText = "Last updated " + relativeDate(latestRun);
    document.getElementById("last-updated").textContent = lastUpdatedText;
    document.getElementById("last-updated-mobile").textContent = lastUpdatedText;

    // Render whichever panel is currently visible
    if (state.mainTab === "history") renderHistory();
    else renderBetsPanel();

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
  // ── 1. Auth guard ─────────────────────────────────────────────
  const session = await getSession();
  if (!session) {
    document.body.innerHTML = renderAuthScreen();
    attachAuthListeners();
    onAuthStateChange((_event, s) => { if (s) init(); });
    return;
  }

  // ── 2. Subscription guard ─────────────────────────────────────
  const sub = await fetchSubscription(session.user.id);
  if (!sub || !["active", "trialing"].includes(sub.status)) {
    document.body.innerHTML = renderPaywall(session);
    attachPaywallListeners(session);
    return;
  }

  // ── 3. Mount account menu on the account icon(s) ──────────────
  // Two buttons exist (mobile + desktop) but only one is visible at a time.
  document.querySelectorAll(".account-btn").forEach(btn => {
    btn.addEventListener("click", e => {
      e.stopPropagation();
      const existing = document.getElementById("account-dropdown");
      if (existing) { existing.remove(); return; }
      btn.closest("div.relative").insertAdjacentHTML("beforeend", renderAccountMenu(session, sub));
      attachAccountMenuListeners(session, sub);
    });
  });
  document.addEventListener("click", () => {
    document.getElementById("account-dropdown")?.remove();
  });

  setMainTab("bets");

  // Desktop tab buttons
  document.querySelectorAll(".main-tab-btn").forEach(btn =>
    btn.addEventListener("click", () => setMainTab(btn.dataset.main))
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

  // Clear filters (desktop)
  document.getElementById("clear-filters-btn").addEventListener("click", () => {
    state.activeLeague   = "all";
    state.activeBetType  = "all";
    state.teamSearch     = "";
    state.activeDateBets = "all";
    state.activeDateHist = "all";
    document.getElementById("team-search").value = "";
    document.getElementById("team-search-mobile").value = "";
    updateFilterUI();
    renderBetsPanel();
    resetHistoryPagination();
  });

  // Right filter drawer (desktop)
  document.getElementById("filters-toggle").addEventListener("click", openDrawer);
  document.getElementById("drawer-close").addEventListener("click", closeDrawer);

  // Shared backdrop — dispatch to whichever drawer is open
  document.getElementById("filter-backdrop").addEventListener("click", () => {
    if (state.burgerDrawerOpen) closeBurgerDrawer(); else closeDrawer();
  });

  // Burger drawer (mobile)
  document.getElementById("burger-btn").addEventListener("click", openBurgerDrawer);
  document.getElementById("burger-drawer-close").addEventListener("click", closeBurgerDrawer);
  document.getElementById("burger-reset-btn").addEventListener("click", () => {
    state.activeLeague   = "all";
    state.activeBetType  = "all";
    state.teamSearch     = "";
    state.activeDateBets = "all";
    state.activeDateHist = "all";
    document.getElementById("team-search").value = "";
    document.getElementById("team-search-mobile").value = "";
    updateFilterUI();
    renderBetsPanel();
    resetHistoryPagination();
    closeBurgerDrawer();
  });

  // Bottom nav tabs (mobile)
  document.querySelectorAll(".bottom-nav-btn").forEach(btn =>
    btn.addEventListener("click", () => setMainTab(btn.dataset.main))
  );

  // Sport popover (bottom nav, mobile)
  const sportPopover = document.getElementById("sport-popover");
  const sportNavBtn  = document.getElementById("sport-nav-btn");
  sportNavBtn.addEventListener("click", e => {
    e.stopPropagation();
    const isOpen = !sportPopover.classList.contains("hidden");
    sportPopover.classList.toggle("hidden", isOpen);
    sportNavBtn.setAttribute("aria-expanded", String(!isOpen));
  });
  document.querySelectorAll(".sport-pop-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      state.activeSport   = btn.dataset.sport;
      state.activeLeague  = "all";
      state.activeBetType = "all";
      state.teamSearch    = "";
      document.getElementById("team-search").value = "";
      document.getElementById("team-search-mobile").value = "";
      sportPopover.classList.add("hidden");
      sportNavBtn.setAttribute("aria-expanded", "false");
      updateFilterUI();
      renderBetsPanel();
      resetHistoryPagination();
    });
  });
  document.addEventListener("click", e => {
    if (!sportNavBtn.contains(e.target) && !sportPopover.contains(e.target)) {
      sportPopover.classList.add("hidden");
      sportNavBtn.setAttribute("aria-expanded", "false");
    }
  });

  // Desktop team search
  document.getElementById("team-search").addEventListener("input", e => {
    state.teamSearch = e.target.value.trim();
    updateFilterBadge();
    renderBetsPanel();
    resetHistoryPagination();
  });

  // Mobile team search (header)
  document.getElementById("team-search-mobile").addEventListener("input", e => {
    state.teamSearch = e.target.value.trim();
    document.getElementById("team-search").value = state.teamSearch;
    updateFilterBadge();
    renderBetsPanel();
    resetHistoryPagination();
  });

  // Chip remove (delegated on both chip containers)
  function removeFilterChip(key) {
    if (key === "league")   { state.activeLeague   = "all"; }
    if (key === "bettype")  { state.activeBetType  = "all"; }
    if (key === "team")     {
      state.teamSearch = "";
      document.getElementById("team-search").value = "";
      document.getElementById("team-search-mobile").value = "";
    }
    if (key === "datebets") { state.activeDateBets = "all"; }
    if (key === "datehist") { state.activeDateHist = "all"; }
    updateFilterUI();
    renderBetsPanel();
    resetHistoryPagination();
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
