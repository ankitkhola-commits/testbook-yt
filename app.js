const ranges = {
  "28": { label: "Last 28 days" },
  "7": { label: "Last 7 days" },
  month: { label: "This month" },
  prevMonth: { label: "Previous month" },
};

const formats = {
  videos: { label: "Videos", color: "#3c6ee8" },
  shorts: { label: "Shorts", color: "#d91632" },
  live: { label: "Live", color: "#0f9f96" },
};

const competitorCategories = ["Testbook", "Teaching", "UGC NET", "CGL", "Odisha", "OPSC", "Bengali", "Marathi", "MPSC", "AE JE", "Bihar", "Banking", "Railways", "Punjab", "Telugu", "Tamil"];
const competitorAutoRefreshMs = 24 * 60 * 60 * 1000; // once a day
let competitorAutoRefreshTimer = null;
const activeProgressBars = {};

const candidateMappings = {
  "Saijal": [
    "TET PRT",
    "TET PRT Testbook",
    "TGT PGT",
    "TGT PGT Testbook",
    "CTET",
    "CTET Testbook",
    "UGC NET",
    "UGC NET Testbook",
    "NET JRF",
    "Testbook NET JRF",
    "Bihar Teaching",
    "Bihar Teaching Exams by Testbook"
  ],
  "Mohit": [
    "Bihar Testbook",
    "Testbook",
    "Punjab",
    "Punjab Testbook"
  ],
  "Vinayak": [
    "Banking",
    "Banking Testbook",
    "MPSC",
    "SuperCoaching MPSC by Testbook",
    "Railways",
    "Railway Testbook"
  ],
  "Aditya": [
    "Bengali",
    "Testbook Bengali",
    "WBPSC",
    "WBPSC Testbook",
    "Marathi",
    "SuperCoaching Marathi by Testbook",
    "Odisha Teaching",
    "Odisha Teaching by Testbook",
    "Odisha Testbook",
    "TET Factory",
    "TET Factory by Testbook"
  ],
  "Vivek": [
    "AE JE Testbook",
    "SSC Testbook",
    "Testbook Tamil",
    "Testbook Telugu"
  ]
};

function getCandidateName(channelTitle) {
  const cleanTitle = String(channelTitle || "").trim().toLowerCase();
  for (const [candidate, channels] of Object.entries(candidateMappings)) {
    if (channels.some(ch => ch.trim().toLowerCase() === cleanTitle)) {
      return candidate;
    }
  }
  return "Other";
}

const ytmMappings = {
  "Nitin": [
    "SuperCoaching MPSC by Testbook",
    "SuperCoaching Marathi by Testbook",
    "UCcpVPJAwpfJlcGE1J84QXvA"
  ],
  "Shubham": [
    "Bihar Testbook",
    "Testbook",
    "Banking Testbook"
  ],
  "Raubnish": [
    "Odisha Testbook",
    "Odisha Teaching by Testbook",
    "Sambhab IAS"
  ],
  "Narendra/Amit": [
    "UGC NET Testbook",
    "Testbook NET JRF"
  ],
  "Abhinav": [
    "TET PRT Testbook",
    "TGT PGT Testbook",
    "CTET Testbook",
    "Bihar Teaching Exams by Testbook"
  ],
  "Shukendu": [
    "Testbook Bengali",
    "WBPSC Testbook"
  ],
  "Ashish Tyagi": [
    "Punjab Testbook"
  ],
  "Lubna": [
    "Railway Testbook"
  ],
  "Vivek": [
    "AE JE Testbook",
    "SSC Testbook",
    "Testbook Tamil",
    "Testbook Telugu"
  ]
};

function getYtmName(channelTitle, channelId) {
  const cleanTitle = String(channelTitle || "").trim().toLowerCase();
  const cleanId = String(channelId || "").trim().toLowerCase();
  for (const [manager, channels] of Object.entries(ytmMappings)) {
    if (channels.some(ch => {
      const cleanCh = ch.trim().toLowerCase();
      return cleanCh === cleanTitle || cleanCh === cleanId;
    })) {
      return manager;
    }
  }
  return "Other";
}

let state = {
  connected: false,
  viewer: null,
  teamAuthEnabled: true,
  allowedEmailDomain: "testbook.com",
  googleConfigured: false,
  youtubeApiKeyConfigured: false,
  claudeConfigured: false,
  allConfigured: false,
  maxChannels: 200,
  channels: [],
  selectedChannelId: "",
  channelSearch: "",
  activeRange: "month",
  viewMetric: "hybrid",
  selectedMonth: currentMonthValue(),
  activeView: "dashboard",
  activeCompetitorCategory: null,
  competitorRequestId: 0,
  competitorLastLoadedAt: 0,
  researchRequestId: 0,
  researchKeyword: "",
  researchRange: "48h",
  researchFilter: "All",
  researchResults: [],
  researchIdeas: [],
  report: null,
  seoResults: [],
  seoFilter: "All",
  seoSortLowestFirst: true,
  ytmResults: [],
  ytmFilter: "All",
  ytmSortLowestFirst: true,
};

const setupScreen = document.querySelector("#setupScreen");
const accessScreen = document.querySelector("#accessScreen");
const appShell = document.querySelector("#appShell");
const channelList = document.querySelector("#channelList");
const channelSearchInput = document.querySelector("#channelSearchInput");
const rangeSelect = document.querySelector("#rangeSelect");
const monthSelect = document.querySelector("#monthSelect");
const monthWrap = document.querySelector("#monthWrap");
const researchKeywordInput = document.querySelector("#researchKeywordInput");
const researchRangeSelect = document.querySelector("#researchRangeSelect");
const competitorDialog = document.querySelector("#competitorDialog");
const competitorForm = document.querySelector("#competitorForm");

document.querySelector("#connectChannelButton")?.addEventListener("click", () => {
  window.location.href = "/auth/google";
});

document.querySelector("#keywordsConnectChannelButton")?.addEventListener("click", () => {
  window.location.href = "/auth/google";
});

document.querySelector("#teamLoginButton")?.addEventListener("click", () => {
  window.location.href = "/auth/team-google";
});

document.querySelector("#refreshButton").addEventListener("click", () => {
  if (state.activeView === "competitors") {
    loadCategoryCompetitors({ force: true });
    return;
  }
  if (state.activeView === "research") {
    loadResearch({ force: true });
    return;
  }
  if (state.activeView === "seo") {
    loadSeoAudit({ force: true });
    return;
  }
  if (state.activeView === "ytm") {
    loadYtmAudit({ force: true });
    return;
  }
  if (state.activeView === "targets") {
    loadTargets({ force: true });
    return;
  }
  loadDashboard({ force: true });
});

window.switchWorkspaceTab = function(tabName) {
  if (!tabName) return;
  state.activeView = tabName;

  // Hide dashboard progress bar if navigating away
  if (state.activeView !== "dashboard") {
    const container = document.getElementById("dashboardProgressBarContainer");
    if (container) {
      container.classList.add("is-hidden");
    }
  }

  applyView();

  if (state.activeView === "competitors") {
    enterCompetitorView();
    return;
  }
  stopCompetitorAutoRefresh();
  if (state.activeView === "research") renderResearchView();
  if (state.activeView === "seo") renderSeoAuditView();
  if (state.activeView === "targets") loadTargets();
  if (state.activeView === "comments") loadComments();
  if (state.activeView === "admin-reports") loadAdminMonthlyReport();
  if (state.activeView === "youtube-ops") loadYouTubeOps();
};

document.querySelectorAll("[data-view-tab]").forEach((button) => {
  button.addEventListener("click", () => {
    window.switchWorkspaceTab(button.dataset.viewTab);
  });
});

rangeSelect.addEventListener("change", (event) => {
  const val = event.target.value;
  state.activeRange = val;
  const isAd = (state.isAuditAdmin === undefined || state.isAuditAdmin === true);
  
  if (isAd) {
    setupAdminCustomFilters(isAd);
    if (val !== "selectMonth") {
      loadDashboard();
    }
  } else {
    loadDashboard();
  }
});

const viewMetricSelect = document.querySelector("#viewMetricSelect");
if (viewMetricSelect) {
  viewMetricSelect.addEventListener("change", (event) => {
    state.viewMetric = event.target.value;
    if (state.activeView === "targets") {
      renderTargetsTable();
    } else if (state.activeView === "admin-reports") {
      renderAdminMonthlyReport();
    } else if (state.report) {
      renderReport(state.report);
    }
  });
}

const monthSelectInput = document.querySelector("#monthSelectInput");
const applyCustomRangeBtn = document.querySelector("#applyCustomRangeBtn");

if (monthSelectInput) {
  const now = new Date();
  const yyyy = now.getFullYear();
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  monthSelectInput.value = `${yyyy}-${mm}`;
  state.selectedMonth = monthSelectInput.value;
  
  monthSelectInput.addEventListener("change", (e) => {
    state.selectedMonth = e.target.value;
  });
}

if (applyCustomRangeBtn) {
  applyCustomRangeBtn.addEventListener("click", () => {
    if (state.activeRange === "selectMonth") {
      if (!state.selectedMonth) {
        alert("Please select a month first.");
        return;
      }
    }
    loadDashboard();
  });
}

channelSearchInput.addEventListener("input", (event) => {
  state.channelSearch = event.target.value.trim().toLowerCase();
  renderChannels();
});

researchKeywordInput?.addEventListener("input", (event) => {
  state.researchKeyword = event.target.value;
});

researchRangeSelect?.addEventListener("change", (event) => {
  state.researchRange = event.target.value;
});

document.querySelector("#researchRunButton")?.addEventListener("click", () => {
  loadResearch({ force: true });
});



document.querySelector("#channelForm").addEventListener("submit", (event) => {
  event.preventDefault();
  window.location.href = "/auth/google";
});

competitorForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await api("/api/competitors", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      channelId: document.querySelector("#competitorOwnerInput").value,
      name: document.querySelector("#competitorNameInput").value.trim(),
      competitorChannelId: document.querySelector("#competitorChannelIdInput").value,
      format: document.querySelector("#competitorFormatInput").value,
    }),
  });
  competitorForm.reset();
  competitorDialog.close();
  await loadDashboard();
});

document.addEventListener("click", async (event) => {
  const copyBtn = event.target.closest(".copy-btn");
  if (copyBtn) {
    const targetId = copyBtn.dataset.copyTarget;
    const targetInput = document.getElementById(targetId);
    if (targetInput) {
      try {
        await navigator.clipboard.writeText(targetInput.value);
        const originalText = copyBtn.textContent;
        copyBtn.textContent = "Copied!";
        copyBtn.classList.add("copied");
        setTimeout(() => {
          copyBtn.textContent = originalText;
          copyBtn.classList.remove("copied");
        }, 1500);
      } catch (err) {
        alert("Failed to copy to clipboard.");
      }
    }
    return;
  }

  const suggestBtn = event.target.closest('[data-seo-action="suggest"]');
  if (suggestBtn) {
    const videoId = suggestBtn.dataset.videoId;
    const channelId = suggestBtn.dataset.channelId;
    const title = suggestBtn.dataset.videoTitle;
    showSeoSuggestions(videoId, channelId, title);
    return;
  }

  const channelToggle = event.target.closest(".channel-map-toggle");
  if (channelToggle) {
    const tags = channelToggle.closest(".benchmark-channel-cell")?.querySelector(".linked-channel-tags");
    if (tags) {
      tags.hidden = !tags.hidden;
      channelToggle.classList.toggle("active", !tags.hidden);
    }
    return;
  }

  const target = event.target.closest("[data-action]");
  if (!target) return;
  const action = target.dataset.action;

  if (action === "save-config") {
    await saveLocalConfig();
  }

  if (action === "toggle-sidebar") {
    appShell.classList.toggle("sidebar-collapsed");
  }

  if (action === "connect-google") {
    await saveLocalConfig({ redirectAfterSave: true });
  }

  if (action === "check-status") {
    await boot();
  }

  if (action === "reset-local") {
    await api("/api/reset", { method: "POST" });
    await boot();
  }

  if (action === "search-competitor") {
    await searchCompetitors();
  }
});

document.querySelector("#researchFilterRow")?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-research-filter]");
  if (!button) return;
  state.researchFilter = button.dataset.researchFilter;
  renderResearchFilters();
  renderResearchResults();
});

document.querySelector("#seoRunButton")?.addEventListener("click", () => {
  loadSeoAudit({ force: true });
});


document.querySelector("#seoSortLowestToggle")?.addEventListener("change", (event) => {
  state.seoSortLowestFirst = event.target.checked;
  renderSeoAuditResults();
});

document.querySelector("#seoChannelFilters")?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-seo-filter]");
  if (!button) return;
  state.seoFilter = button.dataset.seoFilter;
  renderSeoChannelFilters();
  renderSeoSummary();
  renderSeoAuditResults();
});

document.querySelector("#seoExportCsvButton")?.addEventListener("click", () => {
  exportSeoToCsv();
});

document.querySelector("#seoCopySheetsButton")?.addEventListener("click", (event) => {
  copySeoForSheets(event.target);
});

document.querySelector("#ytmRunButton")?.addEventListener("click", () => {
  loadYtmAudit({ force: true });
});

document.querySelector("#ytmSortLowestToggle")?.addEventListener("change", (event) => {
  state.ytmSortLowestFirst = event.target.checked;
  renderYtmAuditResults();
});

document.querySelector("#ytmChannelFilters")?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-ytm-filter]");
  if (!button) return;
  state.ytmFilter = button.dataset.ytmFilter;
  renderYtmChannelFilters();
  renderYtmSummary();
  renderYtmAuditResults();
});

document.querySelector("#ytmExportCsvButton")?.addEventListener("click", () => {
  exportYtmToCsv();
});

document.querySelector("#ytmCopySheetsButton")?.addEventListener("click", (event) => {
  copyYtmForSheets(event.target);
});

boot();

async function boot() {
  if (location.protocol === "file:") {
    showSetupOnly({
      redirectUri: "http://localhost:4173/oauth2callback",
      viewer: null,
      teamAuthEnabled: false,
      allowedEmailDomain: "testbook.com",
      googleConfigured: false,
      youtubeApiKeyConfigured: false,
      claudeConfigured: false,
      allConfigured: false,
    }, "Open http://localhost:4173. The live OAuth app cannot run from file://.");
    return;
  }

  try {
    const status = await api("/api/status");
    state = { ...state, ...status };
    if (status.teamAuthEnabled && !status.viewer) {
      showAccessOnly(status, status.viewerAllowlistEnabled
        ? "Login with an admin-approved Google email to continue."
        : `Sign in with your ${status.allowedEmailDomain || "team"} Google account to continue.`);
      return;
    }
    if (!status.allConfigured) {
      showSetupOnly(status, "Save Google OAuth Client ID, Client Secret, YouTube API key, and Anthropic key to continue.");
      return;
    }
    if (!status.connected) {
      showConnectDashboard(status, "Setup saved successfully. Now login with Google and choose your YouTube channel or Brand Account.");
      return;
    }
    await loadDashboard();
  } catch (error) {
    renderError(error.message);
  }
}

async function loadDashboard(options = {}) {
  const dashboardSteps = [
    { time: 0, text: "Connecting to YouTube API..." },
    { time: 3, text: "Retrieving channel analytics data..." },
    { time: 7, text: "Generating publishing charts and growth metrics..." }
  ];
  const progressBar = startProgressBar("dashboardProgressBarContainer", "dashboardProgressBarFill", "dashboardProgressBarLabel", dashboardSteps);

  try {
    showDashboard();
    setLoading();
    const channelQuery = state.selectedChannelId ? `&channelId=${encodeURIComponent(state.selectedChannelId)}` : "";
    const monthQuery = state.activeRange === "selectMonth" ? `&month=${encodeURIComponent(state.selectedMonth)}` : "";
    const customStartQuery = state.activeRange === "custom" && state.selectedStartDate ? `&startDate=${encodeURIComponent(state.selectedStartDate)}` : "";
    const customEndQuery = state.activeRange === "custom" && state.selectedEndDate ? `&endDate=${encodeURIComponent(state.selectedEndDate)}` : "";
    const compareModeQuery = state.compareMode ? `&compareMode=${encodeURIComponent(state.compareMode)}` : "";
    const forceQuery = options.force ? "&force=1" : "";
    const report = await api(`/api/dashboard?range=${state.activeRange}${monthQuery}${customStartQuery}${customEndQuery}${compareModeQuery}${channelQuery}${forceQuery}`);
    if (progressBar) progressBar.stop(true, "Dashboard Loaded! (100%)", "Dashboard Load Failed!");
    state.report = report;
    state.channels = report.channels;
    state.selectedChannelId = report.selectedChannelId;
    renderReport(report);
  } catch (error) {
    if (progressBar) progressBar.stop(false, "Dashboard Loaded! (100%)", "Dashboard Load Failed!");
    showConnectDashboard(state, error.message);
  }
}

function renderReport(report) {
  showDashboard();
  renderChannels();
  renderGrowth(report.totals, report.comparisonTotals);
  renderMetrics(report.totals, report.comparisonTotals, report.isAllInOne);
  
  const facultyPanel = document.querySelector("#facultyPerformancePanel");
  if (report.isAllInOne) {
    renderAllInOneDashboard(report);
    facultyPanel?.classList.add("is-hidden");
  } else {
    renderUploadTable(report.series);
    renderViewsSplit(report.totals);
    renderSubscriberContent(report.topContent);
    renderAverageViews(report.totals);
    renderYoutubeSearch(report);
    
    facultyPanel?.classList.remove("is-hidden");
    renderFacultyPerformance(report.facultyPerformance);
  }
  renderCompetitorCategoryTabs();
  renderResearchView();
  applyView();
}

function renderChannels() {
  channelList.innerHTML = "";
  const connectedCount = state.report?.connectedCount || state.channels.filter((channel) => channel.id !== "all-in-one").length;
  document.querySelector("#slotCount").textContent = `${connectedCount} connected`;

  const visibleChannels = state.channels.filter((channel) => {
    const haystack = `${channel.name || ""} ${channel.handle || ""}`.toLowerCase();
    return !state.channelSearch || haystack.includes(state.channelSearch);
  });

  if (!visibleChannels.length) {
    channelList.innerHTML = emptyCard("No channel found.");
    return;
  }

  visibleChannels.forEach((channel) => {
    channelList.append(channelButton(channel));
  });
}

function channelButton(channel) {
  const isAd = (state.isAuditAdmin === undefined || state.isAuditAdmin === true);

  const container = document.createElement("div");
  container.className = `channel-list-item ${channel.id === state.selectedChannelId ? "active" : ""}`;

  const button = document.createElement("button");
  button.className = "channel-button";
  button.innerHTML = `
    <span>
      <strong>${escapeHtml(channel.name)}</strong>
    </span>
  `;
  button.addEventListener("click", () => {
    state.selectedChannelId = channel.id;
    if (channel.id !== "all-in-one") {
      state.selectedKeywordsChannelId = channel.id;
    }
    loadDashboard();
    if (state.activeView === "keywords") {
      loadKeywords();
    }
  });
  container.appendChild(button);

  if (isAd && channel.id !== "all-in-one") {
    const removeBtn = document.createElement("button");
    removeBtn.className = "channel-remove-btn";
    removeBtn.innerHTML = "&times;";
    removeBtn.title = `Remove ${channel.name}`;
    removeBtn.addEventListener("click", async (e) => {
      e.stopPropagation();
      if (confirm(`Are you sure you want to remove the channel "${channel.name}" from the dashboard?`)) {
        try {
          await api(`/api/channels/${channel.id}`, { method: "DELETE" });
          if (state.selectedChannelId === channel.id) {
            state.selectedChannelId = "";
          }
          await loadDashboard();
        } catch (err) {
          alert(err.message || "Failed to remove channel");
        }
      }
    });
    container.appendChild(removeBtn);
  }

  return container;
}

function renderGrowth(totals, comparisonTotals = {}) {
  const useHybrid = state.viewMetric === "hybrid";
  const viewsVal = Number((useHybrid ? totals.organicHybridViews : totals.organicViews) || 0);
  const compViewsVal = Number((useHybrid ? comparisonTotals.organicHybridViews : comparisonTotals.organicViews) || 0);

  document.querySelector("#viewsTrend").textContent = viewsVal.toLocaleString();
  document.querySelector("#subTrend").textContent = `+${totals.subscribers.toLocaleString()}`;
  document.querySelector("#viewsTrendDetail").textContent = rangeLabel();
  document.querySelector("#subTrendDetail").textContent = rangeLabel();
  renderDelta("#viewsDelta", viewsVal, compViewsVal);
  renderDelta("#subDelta", totals.subscribers, comparisonTotals.subscribers);
}

function renderMetrics(totals, comparisonTotals = state.report?.comparisonTotals || {}, isAllInOne = false) {
  const useHybrid = state.viewMetric === "hybrid";
  if (isAllInOne) {
    document.querySelector("#metric1Label").textContent = useHybrid ? "Views gained (Hybrid)" : "Views gained";
    document.querySelector("#metric2Label").textContent = "Subscribers gained";
    document.querySelector("#metric3Label").textContent = "Shorts views";
    document.querySelector("#metric4Label").textContent = "Video views";
    document.querySelector("#metric5Label").textContent = "Live views";
    document.querySelector("#metric3Detail").textContent = "All authorized channels";
    document.querySelector("#metric4Detail").textContent = "All authorized channels";
    document.querySelector("#metric5Detail").textContent = "All authorized channels";
    
    const viewsObj = useHybrid ? totals.hybridViews : totals.views;
    const compViewsObj = useHybrid ? (comparisonTotals.hybridViews || {}) : (comparisonTotals.views || {});
    
    document.querySelector("#videoCount").textContent = Number(totals.views.shorts || 0).toLocaleString(); // Shorts always standard
    document.querySelector("#shortCount").textContent = Number(viewsObj.videos || 0).toLocaleString();
    document.querySelector("#liveCount").textContent = Number(viewsObj.live || 0).toLocaleString();
    
    renderDelta("#videoDelta", totals.views.shorts, compViewsObj.shorts);
    renderDelta("#shortDelta", viewsObj.videos, compViewsObj.videos);
    renderDelta("#liveDelta", viewsObj.live, compViewsObj.live);
    return;
  }
  document.querySelector("#metric1Label").textContent = useHybrid ? "Views gained (Hybrid)" : "Views gained";
  document.querySelector("#metric2Label").textContent = "Subscribers gained";
  document.querySelector("#metric3Label").textContent = "Videos published";
  document.querySelector("#metric4Label").textContent = "Shorts published";
  document.querySelector("#metric5Label").textContent = "Live published";
  document.querySelector("#metric3Detail").textContent = "Long-form uploads";
  document.querySelector("#metric4Detail").textContent = "Shorts uploaded";
  document.querySelector("#metric5Detail").textContent = "Live streams";
  const uploadsKnown = totals.uploadsKnown !== false;
  document.querySelector("#videoCount").textContent = formatKnownCount(totals.uploads.videos, uploadsKnown);
  document.querySelector("#shortCount").textContent = formatKnownCount(totals.uploads.shorts, uploadsKnown);
  document.querySelector("#liveCount").textContent = formatKnownCount(totals.uploads.live, uploadsKnown);
  renderDelta("#videoDelta", uploadsKnown ? totals.uploads.videos : 0, comparisonTotals.uploads?.videos);
  renderDelta("#shortDelta", uploadsKnown ? totals.uploads.shorts : 0, comparisonTotals.uploads?.shorts);
  renderDelta("#liveDelta", uploadsKnown ? totals.uploads.live : 0, comparisonTotals.uploads?.live);
}

function renderUploadTable(series) {
  const useHybrid = state.viewMetric === "hybrid";
  const visibleSeries = dashboardVisibleSeries(series).slice().reverse();
  document.querySelector("#uploadChart").innerHTML = `
    <div class="publish-row publish-head">
      <span>Date</span>
      <span>${useHybrid ? "Organic views (Hybrid)" : "Organic views"}</span>
      <span>Subscribers</span>
      <span>Videos</span>
      <span>Shorts</span>
      <span>Live</span>
      <span>Total</span>
      <span>Shares</span>
    </div>
    ${visibleSeries.map((day) => {
      const subVal = Number(day.subscribers || 0);
      const subText = (subVal >= 0 ? "+" : "") + subVal.toLocaleString();
      const subColor = subVal >= 0 ? "var(--accent)" : "#dc2626";
      const viewsVal = Number((useHybrid ? day.organicHybridViews : day.organicViews) || 0);
      
      return `
        <div class="publish-row">
          <strong>${escapeHtml(day.label)}</strong>
          <span class="organic-cell">${viewsVal.toLocaleString()}</span>
          <span style="color: ${subColor}; font-weight: 500;">${subText}</span>
          <span>${formatKnownCount(day.uploads.videos, day.uploadsKnown !== false)}</span>
          <span>${formatKnownCount(day.uploads.shorts, day.uploadsKnown !== false)}</span>
          <span>${formatKnownCount(day.uploads.live, day.uploadsKnown !== false)}</span>
          <b>${formatKnownCount(day.uploads.total, day.uploadsKnown !== false)}</b>
          <span>${Number(day.shares || 0).toLocaleString()}</span>
        </div>
      `;
    }).join("") || `<div class="publish-row empty-row"><strong>No organic/search data in this range yet.</strong></div>`}
  `;
}

function renderViewsSplit(totals) {
  const useHybrid = state.viewMetric === "hybrid";
  const viewsObj = useHybrid ? totals.hybridViews : totals.views;
  
  const values = ["shorts", "videos", "live"].map((key) => viewsObj[key]);
  const total = Math.max(1, values.reduce((sum, value) => sum + value, 0));
  let cursor = 0;
  const stops = ["shorts", "videos", "live"].map((key) => {
    const start = cursor;
    cursor += (viewsObj[key] / total) * 100;
    return `${formats[key].color} ${start}% ${cursor}%`;
  });
  document.querySelector("#viewsDonut").style.background = `conic-gradient(${stops.join(", ")})`;
  document.querySelector("#viewsLegend").innerHTML = ["shorts", "videos", "live"].map((key) => `
    <div class="legend-row">
      <i class="dot ${key === "videos" ? "video" : key === "shorts" ? "short" : "live"}"></i>
      <strong>${formats[key].label}</strong>
      <span>${Math.round((viewsObj[key] / total) * 100)}%</span>
    </div>
  `).join("");
}

function renderSubscriberContent(content) {
  document.querySelector("#subscriberContent").innerHTML = content.length ? content.map((row, index) => `
    <div class="rank-row">
      <b>${index + 1}</b>
      <span>
        <strong>${escapeHtml(row.title)}</strong>
        <span>${escapeHtml(row.format)} · ${row.views.toLocaleString()} views</span>
      </span>
      <em>+${Number(row.subscribers || 0).toLocaleString()}</em>
    </div>
  `).join("") : emptyCard("No top content returned yet.");
}

function renderAverageViews(totals) {
  document.querySelector("#averageViews").innerHTML = ["shorts", "videos", "live"].map((key) => {
    const publishedViews = totals.publishedViews?.[key] || 0;
    const average = totals.uploadsKnown === false ? null : Math.round(publishedViews / Math.max(1, totals.uploads[key]));
    return `
      <div class="average-card">
        <span>${formats[key].label}</span>
        <strong>${average == null ? "-" : average.toLocaleString()}</strong>
      </div>
    `;
  }).join("");
}

function renderAllInOneDashboard(report) {
  const useHybrid = state.viewMetric === "hybrid";
  const organicKey = useHybrid ? "organicHybridViews" : "organicViews";
  renderAllInOneDailyTotals(report.allInOne?.dailyTotals || []);
  renderAllInOneTopContent(report.topContent || []);
  renderChannelRankings("#allInOneOrganicChannels", report.allInOne?.channelRankings?.[organicKey] || [], organicKey);
  renderChannelRankings("#allInOneSubscriberChannels", report.allInOne?.channelRankings?.subscribers || [], "subscribers");
}

function renderAllInOneDailyTotals(days) {
  const useHybrid = state.viewMetric === "hybrid";
  const container = document.querySelector("#allInOneDailyTotals");
  const visibleDays = days
    .filter((day) => Number((useHybrid ? day.organicHybridViews : day.organicViews) || 0) !== 0 || Number(day.subscribers || 0) !== 0)
    .slice()
    .reverse();
  container.innerHTML = `
    <div class="daily-total-row daily-total-head">
      <span>Date</span>
      <span>${useHybrid ? "Total views (Hybrid)" : "Total views"}</span>
      <span>Subscribers</span>
    </div>
    ${visibleDays.map((day) => `
      <div class="daily-total-row">
        <strong>${escapeHtml(day.label)}</strong>
        <span>${Number((useHybrid ? day.organicHybridViews : day.organicViews) || 0).toLocaleString()}</span>
        <em>${Number(day.subscribers || 0) >= 0 ? "+" : ""}${Number(day.subscribers || 0).toLocaleString()}</em>
      </div>
    `).join("") || `<div class="daily-total-row empty-row"><strong>No daily totals returned yet.</strong></div>`}
  `;
}

function renderAllInOneTopContent(content) {
  const container = document.querySelector("#allInOneTopContent");
  container.innerHTML = content.length ? content.slice(0, 20).map((row, index) => `
    <div class="rank-row all-in-one-row">
      ${renderRankBadge(index)}
      <span>
        <strong>${escapeHtml(row.title)}</strong>
        <span><mark class="channel-tag own-tag">${escapeHtml(row.channelTitle || "Channel")}</mark>${escapeHtml(row.format)} · ${row.views.toLocaleString()} views</span>
      </span>
      <em>+${Number(row.subscribers || 0).toLocaleString()}</em>
    </div>
  `).join("") : emptyCard("No top content returned yet.");
}

function renderChannelRankings(selector, rows, key) {
  const container = document.querySelector(selector);
  container.innerHTML = rows.length ? rows.map((row, index) => `
    <div class="rank-row all-in-one-row">
      ${renderRankBadge(index)}
      <span>
        <strong>${escapeHtml(row.name)}</strong>
      </span>
      <em>${Number(row[key] || 0).toLocaleString()}</em>
    </div>
  `).join("") : emptyCard("No channel ranking available yet.");
}

function renderCompetitors(competitors) {
  const grid = document.querySelector("#competitorGrid");
  if (!competitors.length) {
    grid.innerHTML = emptyCard("Select a channel and add up to 6 competitors.");
    return;
  }
  grid.innerHTML = competitors.map((item) => `
    <article class="competitor-card">
      <header>
        <strong>${escapeHtml(item.name)}</strong>
        <button aria-label="Remove ${escapeHtml(item.name)}" data-remove-competitor="${item.id}">×</button>
      </header>
      <span>${escapeHtml(item.format || "Balanced")}</span>
      <div class="benchmark"><span>Views in range</span><strong>${item.views == null ? "API key needed" : item.views.toLocaleString()}</strong></div>
      <div class="benchmark"><span>Uploads</span><strong>${item.uploads == null ? "-" : item.uploads}</strong></div>
      <ol class="top-content">
        ${(item.topContent || []).slice(0, 3).map((content, index) => `
          <li><b>${index + 1}</b><span>${escapeHtml(content.title)}</span><strong>${content.views.toLocaleString()}</strong></li>
        `).join("") || `<li><span>${escapeHtml(item.note || "No public videos found in this range.")}</span></li>`}
      </ol>
    </article>
  `).join("");
  grid.querySelectorAll("[data-remove-competitor]").forEach((button) => {
    button.addEventListener("click", async () => {
      await api(`/api/competitors/${state.selectedChannelId}/${button.dataset.removeCompetitor}`, { method: "DELETE" });
      await loadDashboard();
    });
  });
}

function renderCompetitorCategoryTabs() {
  const container = document.querySelector("#competitorCategoryTabs");
  container.innerHTML = competitorCategories.map((category) => `
    <button class="${category === state.activeCompetitorCategory ? "active" : ""}" type="button" data-category="${escapeHtml(category)}">${escapeHtml(category)}</button>
  `).join("");
  container.querySelectorAll("[data-category]").forEach((button) => {
    button.addEventListener("click", async () => {
      state.activeCompetitorCategory = button.dataset.category;
      renderCompetitorCategoryTabs();
      document.querySelector("#competitorCategoryHeading").textContent = state.activeCompetitorCategory;
      await loadCategoryCompetitors(); // use cache
    });
  });
}

function enterCompetitorView() {
  loadCategoryCompetitors(); // use cache
  startCompetitorAutoRefresh();
}

function startCompetitorAutoRefresh() {
  stopCompetitorAutoRefresh();
  competitorAutoRefreshTimer = setInterval(() => {
    if (state.activeView === "competitors") {
      loadCategoryCompetitors({ silent: true }); // use cache
    }
  }, competitorAutoRefreshMs);
}

function stopCompetitorAutoRefresh() {
  if (!competitorAutoRefreshTimer) return;
  clearInterval(competitorAutoRefreshTimer);
  competitorAutoRefreshTimer = null;
}

async function loadCategoryCompetitors(options = {}) {
  const target = document.querySelector("#competitorBenchmark");
  const requestId = ++state.competitorRequestId;
  const category = state.activeCompetitorCategory;
  if (!document.querySelector("#competitorCategoryTabs").innerHTML.trim()) {
    renderCompetitorCategoryTabs();
  }
  document.querySelector("#competitorPageTitle").textContent = "Channel benchmark";
  document.querySelector("#competitorCategoryHeading").textContent = category || "";
  document.querySelector("#competitorRangeLabel").textContent = "Last 7 days";
  
  if (!category) {
    target.innerHTML = emptyCard("Select your channel to run analysis");
    return;
  }
  
  let progressBar = null;
  if (!options.silent) {
    target.innerHTML = emptyCard("Loading competitor benchmark...");
    const competitorSteps = [
      { time: 0, text: "Fetching competitor lists..." },
      { time: 2, text: "Querying YouTube API for recent video statistics..." },
      { time: 5, text: "Calculating engagement metrics and video benchmark scores..." }
    ];
    progressBar = startProgressBar("competitorProgressBarContainer", "competitorProgressBarFill", "competitorProgressBarLabel", competitorSteps);
  }
  try {
    const forceQuery = options.force ? "&force=1" : "";
    const data = await api(`/api/category-competitors?category=${encodeURIComponent(category)}&range=7${forceQuery}`);
    if (requestId !== state.competitorRequestId) {
      if (progressBar) progressBar.stop(false, "Benchmark Load Failed!");
      return;
    }
    state.competitorLastLoadedAt = Date.now();
    renderCategoryBenchmark(data);
    if (progressBar) progressBar.stop(true, "Benchmark Loaded! (100%)", "Benchmark Load Failed!");
  } catch (error) {
    if (requestId !== state.competitorRequestId) return;
    target.innerHTML = emptyCard(error.message);
    if (progressBar) progressBar.stop(false, "Benchmark Load Failed!");
  }
}

window.addEventListener("focus", () => {
  if (state.activeView !== "competitors") return;
  if (Date.now() - state.competitorLastLoadedAt > 12 * 60 * 60 * 1000) {
    loadCategoryCompetitors({ silent: true }); // use cache
  }
});

document.addEventListener("visibilitychange", () => {
  if (document.hidden || state.activeView !== "competitors") return;
  if (Date.now() - state.competitorLastLoadedAt > 12 * 60 * 60 * 1000) {
    loadCategoryCompetitors({ silent: true }); // use cache
  }
});

function renderCategoryBenchmark(data) {
  const target = document.querySelector("#competitorBenchmark");
  if (!data.available) {
    target.innerHTML = emptyCard(data.message || "This category is not mapped yet.");
    return;
  }
  const leaders = benchmarkLeaders(data.groups);
  const ownedChannelIds = new Set(data.ownedChannelIds || []);
  const topVodLive = [...data.top.videos, ...data.top.live].sort((a, b) => b.views - a.views);
  target.innerHTML = `
    <div class="competitor-top-grid">
      <div class="benchmark-table">
        <div class="benchmark-row benchmark-head">
          <span>Channel</span>
          <span>Views</span>
          <span>Short</span>
          <span>Video</span>
          <span>Live</span>
        </div>
        ${data.groups.map((group) => `
          <div class="benchmark-row">
            ${benchmarkChannelCell(group)}
            ${benchmarkCell(group.views, leaders.views, group.name)}
            ${benchmarkCell(group.averageViews.shorts, leaders.shorts, group.name)}
            ${benchmarkCell(group.averageViews.videos, leaders.videos, group.name)}
            ${benchmarkCell(group.averageViews.live, leaders.live, group.name)}
          </div>
        `).join("")}
      </div>
      <article class="panel-subsection last24-card">
        <div class="panel-header compact-header">
          <div>
            <p class="eyebrow">Last 24 hours</p>
            <h2>Top performing content</h2>
          </div>
        </div>
        <div class="rank-list">${contentRows(data.top.last24, 10, ownedChannelIds)}</div>
      </article>
    </div>
    <div class="competitor-content-grid">
      ${contentBlock("Top shorts", data.top.shorts, ownedChannelIds)}
      ${contentBlock("Top VOD + live", topVodLive, ownedChannelIds)}
    </div>
  `;
}

function benchmarkChannelCell(group) {
  return `
    <div class="benchmark-channel-cell">
      <button class="channel-map-toggle" type="button" title="Show mapped channels" aria-label="Show mapped channels for ${escapeHtml(group.name)}">⏻</button>
      <strong>${escapeHtml(group.name)}</strong>
      <div class="linked-channel-tags" hidden>
        ${group.channels.map((channel) => `<span>${escapeHtml(channel.title)}</span>`).join("")}
      </div>
    </div>
  `;
}

function contentBlock(title, items, ownedChannelIds) {
  return `
    <article class="panel-subsection">
      <p class="eyebrow">${escapeHtml(title)}</p>
      <div class="rank-list">${contentRows(items, 10, ownedChannelIds)}</div>
    </article>
  `;
}

function contentRows(items, limit = 5, ownedChannelIds = new Set()) {
  return items.length ? items.slice(0, limit).map((item, index) => `
    <div class="rank-row content-row">
      <b>${index + 1}</b>
      <span>
        <strong>${escapeHtml(item.title)}</strong>
        <span><mark class="channel-tag ${isOwnedCompetitorChannel(item.channelId, ownedChannelIds) ? "own-tag" : "competitor-tag"}">${escapeHtml(item.channelTitle)}</mark><span class="content-metric">${item.views.toLocaleString()} views</span></span>
      </span>
      <em>${escapeHtml(item.format)}</em>
    </div>
  `).join("") : emptyCard("No content found in this range.");
}

function benchmarkLeaders(groups) {
  const metrics = {
    views: (group) => group.views,
    shorts: (group) => group.averageViews.shorts,
    videos: (group) => group.averageViews.videos,
    live: (group) => group.averageViews.live,
    engagement: (group) => group.engagement,
  };
  return Object.fromEntries(Object.entries(metrics).map(([key, getter]) => {
    const max = Math.max(...groups.map((group) => Number(getter(group) || 0)), 0);
    const owners = groups.filter((group) => Number(getter(group) || 0) === max).map((group) => group.name);
    return [key, { value: max, owners }];
  }));
}

function benchmarkCell(value, leader, groupName, formatter = formatInteger, tag = "span") {
  const numericValue = Number(value || 0);
  const isLeader = numericValue === leader.value && leader.owners.includes(groupName);
  const classes = ["benchmark-value"];
  if (isLeader && isOwnedGroupName(groupName)) classes.push("leader-testbook");
  if (isLeader && !isOwnedGroupName(groupName)) classes.push("leader-competitor");
  return `<${tag} class="${classes.join(" ")}">${formatter(numericValue)}</${tag}>`;
}

function isOwnedGroupName(value = "") {
  return /testbook/i.test(String(value));
}

function isOwnedCompetitorChannel(channelId, ownedChannelIds) {
  return ownedChannelIds.has(channelId);
}

function formatInteger(value) {
  return Number(value || 0).toLocaleString();
}

function formatPercentCompact(value) {
  return `${Number(value || 0).toFixed(2)}%`;
}

function applyView() {
  const isDashboard = state.activeView === "dashboard";
  const isAllInOne = state.report ? state.report.isAllInOne : (state.selectedChannelId === "all-in-one" || !state.selectedChannelId);
  document.querySelectorAll("[data-view-tab]").forEach((button) => {
    button.classList.toggle("active", button.dataset.viewTab === state.activeView);
  });
  document.querySelectorAll("[data-view]").forEach((element) => {
    const views = element.dataset.view.split(/\s+/);
    element.classList.toggle("is-hidden", !views.includes(state.activeView));
  });
  document.querySelectorAll("[data-dashboard-mode]").forEach((element) => {
    if (!isDashboard) {
      element.classList.add("is-hidden");
      return;
    }
    const mode = element.dataset.dashboardMode;
    element.classList.toggle("is-hidden", isAllInOne ? mode !== "all" : mode !== "single");
  });
  document.querySelectorAll("[data-sidebar-view]").forEach((element) => {
    element.classList.toggle("is-hidden", !isDashboard);
  });
  document.querySelector("#topbarActions")?.classList.toggle("is-hidden", !isDashboard);
  document.querySelector("#channelTitle").textContent = isDashboard
    ? (state.report?.title || "Channel Analytics")
    : state.activeView === "competitors"
      ? "Competitors"
      : state.activeView === "seo"
        ? "SEO Audit"
        : state.activeView === "ytm"
          ? "YTM Audit"
          : state.activeView === "targets"
            ? "Target Tracker"
              : state.activeView === "comments"
                ? "Comments Moderator"
                : state.activeView === "admin-reports"
                  ? (state.adminMonthlyReport?.rangeLabel ? `MoM Progress & Export (${state.adminMonthlyReport.rangeLabel})` : "MoM Progress & Export")
                  : state.activeView === "youtube-ops"
                    ? "YouTube Operations"
                    : "Research";
}

function renderResearchView() {
  if (researchKeywordInput) researchKeywordInput.value = state.researchKeyword;
  if (researchRangeSelect) researchRangeSelect.value = state.researchRange;
  renderResearchFilters();
  renderResearchSummary();
  renderResearchResults();
}

function renderResearchFilters() {
  document.querySelectorAll("[data-research-filter]").forEach((button) => {
    button.classList.toggle("active", button.dataset.researchFilter === state.researchFilter);
  });
}

function renderResearchSummary() {
  const summary = document.querySelector("#researchSummary");
  if (!summary) return;
  if (!state.researchResults.length) {
    summary.innerHTML = "";
    return;
  }
  const filtered = filteredResearchResults();
  summary.innerHTML = `
    <div class="research-stat">
      <span>Keyword</span>
      <strong>${escapeHtml(state.researchKeyword)}</strong>
    </div>
    <div class="research-stat">
      <span>Range</span>
      <strong>${researchRangeLabel(state.researchRange)}</strong>
    </div>
    <div class="research-stat">
      <span>Results</span>
      <strong>${filtered.length}</strong>
    </div>
    <div class="research-stat">
      <span>Top view count</span>
      <strong>${formatCompactNumber(filtered[0]?.views || 0)}</strong>
    </div>
  `;
}

function renderResearchResults() {
  const container = document.querySelector("#researchResults");
  if (!container) return;
  const filtered = filteredResearchResults();
  if (!state.researchResults.length) {
    container.innerHTML = emptyCard("Add a keyword and run research to see the top videos in this time frame.");
    return;
  }
  if (!filtered.length) {
    container.innerHTML = emptyCard(`No ${state.researchFilter.toLowerCase()} results found for this keyword.`);
    return;
  }
  container.innerHTML = `
    <div class="research-table">
      <div class="research-row research-head">
        <span>Rank</span>
        <span>Content</span>
        <span>Channel</span>
        <span>Format</span>
        <span>Views</span>
        <span>Link</span>
      </div>
      ${filtered.map((item, index) => `
        <div class="research-row">
          <b>${index + 1}</b>
          <div class="research-title-cell">
            <strong>${escapeHtml(item.title)}</strong>
            <small>${escapeHtml(formatPublishedAt(item.publishedAt))}</small>
          </div>
          <span class="research-channel">${escapeHtml(item.channelTitle)}</span>
          <span class="research-format">${escapeHtml(item.format)}</span>
          <strong>${formatCompactNumber(item.views)}</strong>
          <a class="link-chip" href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">YouTube</a>
        </div>
      `).join("")}
    </div>
  `;
}

async function loadResearch(options = {}) {
  const keyword = (state.researchKeyword || "").trim();
  if (!keyword) {
    document.querySelector("#researchResults").innerHTML = emptyCard("Enter a keyword first.");
    return;
  }
  state.researchKeyword = keyword;
  if (!options.force && state.researchResults.length) {
    renderResearchView();
    return;
  }
  const requestId = ++state.researchRequestId;
  document.querySelector("#researchResults").innerHTML = emptyCard("Loading top videos...");
  
  const researchSteps = [
    { time: 0, text: "Searching YouTube for videos matching keyword..." },
    { time: 3, text: "Retrieving statistics and identifying formats..." },
    { time: 7, text: "Calculating view/subscriber ratios and ranking outliers..." }
  ];
  const progressBar = startProgressBar("researchProgressBarContainer", "researchProgressBarFill", "researchProgressBarLabel", researchSteps);

  try {
    const forceQuery = options.force ? "&force=1" : "";
    const data = await api(`/api/research?keyword=${encodeURIComponent(keyword)}&range=${encodeURIComponent(state.researchRange)}${forceQuery}`);
    if (requestId !== state.researchRequestId) {
      if (progressBar) progressBar.stop(false, "Research Failed!");
      return;
    }
    state.researchResults = data.items || [];
    state.researchFilter = "All";
    renderResearchView();
    if (progressBar) progressBar.stop(true, "Research Loaded! (100%)", "Research Failed!");
  } catch (error) {
    if (requestId !== state.researchRequestId) return;
    document.querySelector("#researchResults").innerHTML = emptyCard(error.message);
    if (progressBar) progressBar.stop(false, "Research Failed!");
  }
}

function filteredResearchResults() {
  if (state.researchFilter === "All") return state.researchResults;
  return state.researchResults.filter((item) => item.format === state.researchFilter);
}

function channelNameById(channelId) {
  return state.channels.find((channel) => channel.id === channelId)?.name || "Selected channel";
}

function populateCompetitorOwners() {
  const ownerInput = document.querySelector("#competitorOwnerInput");
  ownerInput.innerHTML = state.channels.map((channel) => `
    <option value="${channel.id}" ${channel.id === state.selectedChannelId ? "selected" : ""}>${escapeHtml(channel.name)}</option>
  `).join("");
  document.querySelector("#competitorSearchResults").innerHTML = "";
  document.querySelector("#competitorChannelIdInput").value = "";
}

async function searchCompetitors() {
  const query = document.querySelector("#competitorNameInput").value.trim();
  const results = document.querySelector("#competitorSearchResults");
  if (!query) return;
  results.innerHTML = `<button type="button" class="search-result">Searching...</button>`;
  const data = await api(`/api/search-channels?q=${encodeURIComponent(query)}`);
  if (!data.channels.length) {
    results.innerHTML = `<button type="button" class="search-result">No channel found. You can still add by exact channel ID.</button>`;
    return;
  }
  results.innerHTML = data.channels.map((channel) => `
    <button type="button" class="search-result" data-channel-id="${escapeHtml(channel.id)}" data-channel-name="${escapeHtml(channel.name)}">
      ${channel.thumbnail ? `<img src="${escapeHtml(channel.thumbnail)}" alt="" />` : ""}
      <span>
        <strong>${escapeHtml(channel.name)}</strong>
        <small>${escapeHtml(channel.handle || channel.id)} · ${Number(channel.subscribers || 0).toLocaleString()} subscribers</small>
      </span>
    </button>
  `).join("");
}

document.querySelector("#competitorSearchResults").addEventListener("click", (event) => {
  const result = event.target.closest("[data-channel-id]");
  if (!result) return;
  document.querySelector("#competitorNameInput").value = result.dataset.channelName;
  document.querySelector("#competitorChannelIdInput").value = result.dataset.channelId;
  document.querySelector("#competitorSearchResults").innerHTML = `
    <button type="button" class="search-result selected">
      <span><strong>${escapeHtml(result.dataset.channelName)}</strong><small>${escapeHtml(result.dataset.channelId)}</small></span>
    </button>
  `;
});

document.querySelector("#youtubeSearchChart").addEventListener("click", async (event) => {
  const button = event.target.closest("[data-search-date]");
  if (!button) return;
  await loadSearchKeywords(button.dataset.searchDate);
});

function renderYoutubeSearch(report) {
  const useHybrid = state.viewMetric === "hybrid";
  const searchDays = dashboardVisibleSeries(report.series).map((day) => ({
    date: day.date,
    label: day.label,
    views: Number((useHybrid ? day.youtubeSearchEngagedViews : day.youtubeSearchViews) || 0),
  }));
  const maxViews = Math.max(1, ...searchDays.map((day) => day.views));
  const bestDay = searchDays.slice().sort((a, b) => b.views - a.views)[0];
  const selectedDate = state.selectedSearchDate && searchDays.some((day) => day.date === state.selectedSearchDate)
    ? state.selectedSearchDate
    : bestDay?.date;
  state.selectedSearchDate = selectedDate;
  const totalSearchVal = Number((useHybrid ? report.totals.youtubeSearchEngagedViews : report.totals.youtubeSearchViews) || 0);
  document.querySelector("#searchTotal").textContent = `${totalSearchVal.toLocaleString()} search views${useHybrid ? " (Engaged)" : ""}`;
  document.querySelector("#youtubeSearchChart").innerHTML = searchDays.map((day) => {
    const height = day.views ? Math.max(10, Math.round((day.views / maxViews) * 126)) : 4;
    return `
      <button class="search-day ${day.date === selectedDate ? "active" : ""}" type="button" data-search-date="${escapeHtml(day.date)}">
        <i style="height:${height}px"></i>
        <strong>${escapeHtml(day.label)}</strong>
        <span class="search-value">${day.views.toLocaleString()}</span>
      </button>
    `;
  }).join("");
  if (selectedDate) {
    loadSearchKeywords(selectedDate);
  } else {
    document.querySelector("#searchKeywords").innerHTML = emptyCard("No YouTube Search views in this range.");
  }
}

function dashboardVisibleSeries(series) {
  return series.filter((day) => Number(day.organicViews || 0) > 0 || Number(day.youtubeSearchViews || 0) > 0);
}

function renderDelta(selector, currentValue = 0, previousValue = 0) {
  const element = document.querySelector(selector);
  const current = Number(currentValue || 0);
  const previous = Number(previousValue || 0);
  const percent = previous ? Math.round(((current - previous) / Math.abs(previous)) * 100) : (current ? 100 : 0);
  element.textContent = `${percent > 0 ? "+" : ""}${percent}%`;
  element.className = `delta ${percent > 0 ? "up" : percent < 0 ? "down" : "neutral"}`;
}

function rangeLabel() {
  if (state.activeRange === "7" || state.activeRange === "28") {
    return ranges[state.activeRange].label;
  }
  if (state.activeRange === "selectMonth") {
    return new Date(`${state.selectedMonth}-01T00:00:00Z`).toLocaleDateString("en-IN", { month: "short", year: "numeric" });
  }
  if (state.activeRange === "custom") {
    if (state.report && state.report.dates) {
      const start = new Date(`${state.report.dates.startDate}T00:00:00Z`).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
      const end = new Date(`${state.report.dates.endDate}T00:00:00Z`).toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
      return `${start} to ${end}`;
    }
    return "Custom Date Range";
  }
  if (state.report && state.report.dates) {
    return new Date(`${state.report.dates.startDate}T00:00:00Z`).toLocaleDateString("en-IN", { month: "short", year: "numeric" });
  }
  return ranges[state.activeRange].label;
}

async function loadSearchKeywords(date) {
  state.selectedSearchDate = date;
  document.querySelectorAll("[data-search-date]").forEach((button) => {
    button.classList.toggle("active", button.dataset.searchDate === date);
  });
  document.querySelector("#searchKeywordTitle").textContent = formatDisplayDate(date);
  document.querySelector("#searchKeywords").innerHTML = emptyCard("Loading keywords...");
  try {
    const monthQuery = state.activeRange === "selectMonth" ? `&month=${encodeURIComponent(state.selectedMonth)}` : "";
    const data = await api(`/api/search-keywords?range=${state.activeRange}${monthQuery}&channelId=${state.selectedChannelId}&date=${encodeURIComponent(date)}`);
    document.querySelector("#searchKeywords").innerHTML = data.keywords.length ? data.keywords.map((row, index) => `
      <div class="keyword-row">
        <b>${index + 1}</b>
        <span>${escapeHtml(row.keyword)}</span>
        <strong>${Number(row.views || 0).toLocaleString()}</strong>
      </div>
    `).join("") : emptyCard("YouTube did not return search keyword detail for this day yet.");
  } catch (error) {
    document.querySelector("#searchKeywords").innerHTML = emptyCard(error.message);
  }
}

function showSetupOnly(status, message) {
  accessScreen.hidden = true;
  setupScreen.hidden = false;
  appShell.hidden = true;
  document.querySelector("#setupRedirectPreview").textContent = status.redirectUri || "http://localhost:4173/oauth2callback";
  document.querySelector("#setupRedirectUri").value = status.redirectUri || "http://localhost:4173/oauth2callback";
  document.querySelector("#setupGoogleClientId").placeholder = status.googleConfigured ? "Already configured" : "xxxxx.apps.googleusercontent.com";
  document.querySelector("#setupGoogleClientSecret").placeholder = status.googleConfigured ? "Already configured" : "OAuth client secret";
  document.querySelector("#setupYoutubeApiKey").placeholder = status.youtubeApiKeyConfigured ? "Already configured" : "Used for public competitor data";
  document.querySelector("#setupAnthropicApiKey").placeholder = status.claudeConfigured ? "Already configured" : "Used for AI recommendations";
  document.querySelector("#setupMessage").textContent = message;
}

function showAccessOnly(status, message) {
  setupScreen.hidden = true;
  accessScreen.hidden = false;
  appShell.hidden = true;
  document.querySelector("#accessMessage").textContent = message;
}

function setupAdminCustomFilters(isAd) {
  const rangeSelect = document.querySelector("#rangeSelect");
  const adminCustomFilters = document.querySelector("#adminCustomFilters");
  const monthPickerWrap = document.querySelector("#monthPickerWrap");

  if (!rangeSelect || !adminCustomFilters) return;

  if (isAd) {
    if (!rangeSelect.querySelector('option[value="selectMonth"]')) {
      const optMonth = document.createElement("option");
      optMonth.value = "selectMonth";
      optMonth.textContent = "Custom Month";
      rangeSelect.appendChild(optMonth);
    }

    const val = rangeSelect.value;
    if (val === "selectMonth") {
      adminCustomFilters.style.display = "flex";
      if (monthPickerWrap) monthPickerWrap.style.display = "grid";
    } else {
      adminCustomFilters.style.display = "none";
      if (monthPickerWrap) monthPickerWrap.style.display = "none";
    }
  } else {
    adminCustomFilters.style.display = "none";
    rangeSelect.querySelectorAll('option[value="selectMonth"]').forEach(opt => opt.remove());
  }
}

function showDashboard() {
  accessScreen.hidden = true;
  setupScreen.hidden = true;
  appShell.hidden = false;
  
  const isAd = (state.isAuditAdmin === undefined || state.isAuditAdmin === true);
  if (!isAd && state.activeView === "ytm") {
    state.activeView = "dashboard";
  }
  
  setupAdminCustomFilters(isAd);
  
  // SEO Audit, MoM Export, and YouTube Ops are available to everyone
  document.querySelectorAll('[data-view-tab="seo"], [data-view-tab="admin-reports"], [data-view-tab="youtube-ops"]').forEach(btn => {
    btn.style.display = "";
  });
  document.querySelectorAll('[data-view-tab="ytm"]').forEach(btn => {
    btn.style.display = isAd ? "" : "none";
  });

  document.querySelector("#manageTargetsButton")?.classList.remove("is-hidden");

  const canAdd = (state.allowedToAddChannel === undefined || state.allowedToAddChannel === true);
  document.querySelectorAll("#connectChannelButton, #keywordsConnectChannelButton").forEach(btn => {
    btn.classList.toggle("is-hidden", !canAdd);
  });

  applyView();
}

function showConnectDashboard(status, message) {
  showDashboard();
  state.channels = [];
  document.querySelector("#channelTitle").textContent = "Connect YouTube";
  document.querySelector("#slotCount").textContent = "0 connected";
  channelList.innerHTML = emptyCard("No channels connected yet.");
  ["#viewsTrend", "#subTrend", "#videoCount", "#shortCount", "#liveCount"].forEach((selector) => {
    document.querySelector(selector).textContent = "0";
  });
  document.querySelector("#viewsTrendDetail").textContent = ranges[state.activeRange].label;
  document.querySelector("#subTrendDetail").textContent = ranges[state.activeRange].label;
  document.querySelector("#uploadChart").innerHTML = `<div class="publish-row empty-row"><strong>${escapeHtml(message)}</strong></div>`;
  document.querySelector("#viewsLegend").innerHTML = "";
  document.querySelector("#subscriberContent").innerHTML = emptyCard("Top content appears after OAuth.");
  document.querySelector("#competitorBenchmark").innerHTML = emptyCard("Select a connected channel to compare against competitors.");
  document.querySelector("#averageViews").innerHTML = "";
  document.querySelector("#youtubeSearchChart").innerHTML = "";
  document.querySelector("#searchKeywords").innerHTML = "";
  applyView();
}

function renderNotConnected(status) {
  renderSetup(status, status.googleConfigured
    ? "Add a channel with Google to load your real YouTube channels."
    : "Google OAuth is not configured yet. Fill `.env`, then restart the server.");
}

function renderOfflineMode() {
  renderEmptyShell("Open this dashboard through the local server at http://localhost:4173. The live Google OAuth flow cannot run from a file:// URL.");
}

function renderError(message) {
  if (state.allConfigured || state.connected) {
    showConnectDashboard(state, message);
    return;
  }
  showSetupOnly(state, message);
}

function renderSetup(status, message) {
  state.channels = [];
  document.querySelector("#channelTitle").textContent = "Setup";
  document.querySelector("#slotCount").textContent = "0 connected";
  channelList.innerHTML = emptyCard("No channels connected.");
  ["#viewsTrend", "#subTrend", "#videoCount", "#shortCount", "#liveCount"].forEach((selector) => {
    document.querySelector(selector).textContent = "0";
  });
  document.querySelector("#uploadChart").innerHTML = `<div class="publish-row empty-row"><strong>${escapeHtml(message)}</strong></div>`;
  document.querySelector("#viewsLegend").innerHTML = "";
  document.querySelector("#subscriberContent").innerHTML = emptyCard("Top content appears after OAuth.");
  document.querySelector("#competitorBenchmark").innerHTML = emptyCard("Add competitors after selecting one connected channel.");
  document.querySelector("#averageViews").innerHTML = "";
  document.querySelector("#youtubeSearchChart").innerHTML = "";
  document.querySelector("#searchKeywords").innerHTML = "";
  applyView();
}

function renderEmptyShell(message) {
  state.channels = [];
  document.querySelector("#channelTitle").textContent = "Live YouTube dashboard";
  document.querySelector("#slotCount").textContent = "0 connected";
  channelList.innerHTML = emptyCard("No channels connected.");
  ["#viewsTrend", "#subTrend", "#videoCount", "#shortCount", "#liveCount"].forEach((selector) => {
    document.querySelector(selector).textContent = "0";
  });
  document.querySelector("#uploadChart").innerHTML = `<div class="publish-row empty-row"><strong>${escapeHtml(message)}</strong></div>`;
  document.querySelector("#viewsLegend").innerHTML = "";
  document.querySelector("#subscriberContent").innerHTML = emptyCard(message);
  document.querySelector("#competitorBenchmark").innerHTML = emptyCard(message);
  document.querySelector("#averageViews").innerHTML = "";
  document.querySelector("#youtubeSearchChart").innerHTML = "";
  document.querySelector("#searchKeywords").innerHTML = "";
  applyView();
}

function setLoading() {
  document.querySelector("#channelTitle").textContent = "Loading YouTube data";
}

async function api(url, options) {
  const response = await fetch(url, options);
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || "Request failed.");
  return data;
}

async function saveLocalConfig(options = {}) {
  const config = await api("/api/config", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      googleClientId: document.querySelector("#setupGoogleClientId")?.value || "",
      googleClientSecret: document.querySelector("#setupGoogleClientSecret")?.value || "",
      googleRedirectUri: document.querySelector("#setupRedirectUri")?.value || "",
      youtubeApiKey: document.querySelector("#setupYoutubeApiKey")?.value || "",
      anthropicApiKey: document.querySelector("#setupAnthropicApiKey")?.value || "",
    }),
  });
  state = { ...state, ...config };
  if (options.redirectAfterSave) {
    window.location.href = "/auth/google";
    return;
  }
  if (config.allConfigured) {
    showConnectDashboard(config, "All local settings are saved. Click Add channel to choose your YouTube channel or Brand Account.");
    return;
  }
  showSetupOnly(config, "Saved what you entered. Add the missing required keys before the dashboard opens.");
}

function emptyCard(message) {
  return `<article class="competitor-card"><strong>${escapeHtml(message)}</strong></article>`;
}

function formatDisplayDate(value) {
  const date = new Date(`${value}T00:00:00Z`);
  const dayMonth = date.toLocaleDateString("en-IN", { day: "numeric", month: "short" });
  const weekday = date.toLocaleDateString("en-IN", { weekday: "long" });
  return `${dayMonth}, ${weekday}`;
}

function formatPublishedAt(value) {
  const date = new Date(value);
  return date.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
}

function currentMonthValue() {
  return new Date().toISOString().slice(0, 7);
}

function researchRangeLabel(value) {
  return {
    "48h": "Last 48 hours",
    "30d": "Last 30 days",
    "90d": "Last 90 days",
    "365d": "Last 365 days",
  }[value] || value;
}

function formatCompactNumber(value) {
  return new Intl.NumberFormat("en-IN", {
    notation: "compact",
    maximumFractionDigits: value >= 100000 ? 1 : 0,
  }).format(Number(value || 0));
}

function formatOutlier(value) {
  return `${Number(value || 0).toFixed(2)}x`;
}

function formatKnownCount(value, known = true) {
  return known ? Number(value || 0).toLocaleString() : "-";
}

function ideaFormatClass(format) {
  if (format === "Shorts") return "own-tag";
  if (format === "Live") return "competitor-tag";
  return "neutral-tag";
}

function renderRankBadge(index) {
  return `<b class="rank-badge ${rankBadgeClass(index)}">${index + 1}</b>`;
}

function rankBadgeClass(index) {
  if (index === 0) return "gold";
  if (index === 1) return "silver";
  if (index === 2) return "bronze";
  return "default";
}

function truncateTitle(value, maxLength = 40) {
  const text = String(value || "");
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`;
}

function dedupeFacultyVideos(videos) {
  const seen = new Set();
  return videos.filter((video) => {
    const key = `${video.id || ""}:${video.title || ""}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  })[char]);
}

function startProgressBar(containerId, fillId, labelId, steps) {
  if (activeProgressBars[containerId]) {
    clearInterval(activeProgressBars[containerId]);
    delete activeProgressBars[containerId];
  }

  const container = document.getElementById(containerId);
  const fill = document.getElementById(fillId);
  const label = document.getElementById(labelId);
  if (!container || !fill || !label) return null;

  container.classList.remove("is-hidden");
  fill.style.width = "0%";
  
  let currentProgress = 0;
  let startTime = Date.now();
  
  const interval = setInterval(() => {
    const elapsed = (Date.now() - startTime) / 1000;
    
    let stepText = steps[0].text;
    for (const step of steps) {
      if (elapsed >= step.time) {
        stepText = step.text;
      }
    }
    
    let targetProgress = 100 * (1 - Math.exp(-elapsed / 35));
    if (targetProgress > 98) targetProgress = 98;
    
    currentProgress = targetProgress;
    fill.style.width = `${Math.round(currentProgress)}%`;
    label.textContent = `${stepText} (${Math.round(currentProgress)}%)`;
  }, 100);
  
  activeProgressBars[containerId] = interval;
  
  return {
    stop: (success = true, customSuccessText = "Audit Complete! (100%)", customFailText = "Audit Failed!") => {
      if (activeProgressBars[containerId] === interval) {
        clearInterval(interval);
        delete activeProgressBars[containerId];
      } else {
        clearInterval(interval);
        return;
      }
      fill.style.width = "100%";
      label.textContent = success ? customSuccessText : customFailText;
      setTimeout(() => {
        if (!activeProgressBars[containerId]) {
          container.classList.add("is-hidden");
          fill.style.width = "0%";
        }
      }, 800);
    }
  };
}

async function loadSeoAudit(options = {}) {
  const resultsContainer = document.querySelector("#seoResults");
  if (!resultsContainer) return;
  
  const targetSelect = document.querySelector("#seoAuditTarget");
  const candidate = targetSelect ? targetSelect.value : "All";
  
  if (!options.force && state.seoResults.length) {
    renderSeoAuditView();
    return;
  }
  
  resultsContainer.innerHTML = emptyCard("Audit in progress... Please see the progress bar above.");
  
  const seoSummary = document.querySelector("#seoSummary");
  if (seoSummary) seoSummary.innerHTML = "";
  const seoChannelFilters = document.querySelector("#seoChannelFilters");
  if (seoChannelFilters) seoChannelFilters.innerHTML = "";
  
  const seoSteps = [
    { time: 0, text: "Fetching uploads and video metadata..." },
    { time: 4, text: "Analyzing title keywords and tags..." },
    { time: 10, text: "Evaluating description structure and hashtags..." },
    { time: 20, text: "Consulting AI model for suggestions..." },
    { time: 35, text: "Finalizing optimization reports..." }
  ];
  const progressBar = startProgressBar("seoProgressBarContainer", "seoProgressBarFill", "seoProgressBarLabel", seoSteps);
  
  try {
    const candidateQuery = `candidate=${encodeURIComponent(candidate)}`;
    const forceQuery = options.force ? "&force=1" : "";
    const data = await api(`/api/seo/audit?${candidateQuery}${forceQuery}`);
    if (progressBar) progressBar.stop(true);
    state.seoResults = data.videos || [];
    state.seoFilter = candidate;
    renderSeoAuditView();
  } catch (error) {
    if (progressBar) progressBar.stop(false);
    resultsContainer.innerHTML = emptyCard(error.message);
  }
}

function renderSeoAuditView() {
  if (state.activeView !== "seo") return;
  renderSeoChannelFilters();
  renderSeoSummary();
  renderSeoAuditResults();

  const hasResults = state.seoResults && state.seoResults.length > 0;
  const csvBtn = document.querySelector("#seoExportCsvButton");
  const sheetsBtn = document.querySelector("#seoCopySheetsButton");
  if (csvBtn) csvBtn.disabled = !hasResults;
  if (sheetsBtn) sheetsBtn.disabled = !hasResults;
}

function renderSeoChannelFilters() {
  const container = document.querySelector("#seoChannelFilters");
  if (!container) return;
  if (!state.seoResults.length) {
    container.innerHTML = "";
    return;
  }
  
  const candidatesWithResults = new Set();
  state.seoResults.forEach(video => {
    const candidate = getCandidateName(video.channelTitle);
    candidatesWithResults.add(candidate);
  });
  
  const candidateList = ["Saijal", "Mohit", "Vinayak", "Aditya", "Vivek", "Other"].filter(c => candidatesWithResults.has(c));
  
  if (!candidateList.includes(state.seoFilter)) {
    state.seoFilter = candidateList[0] || "Other";
  }
  
  container.innerHTML = candidateList.map(candidate => {
    const isActive = state.seoFilter === candidate;
    return `<button class="filter-chip ${isActive ? "active" : ""}" type="button" data-seo-filter="${escapeHtml(candidate)}">${escapeHtml(candidate)}</button>`;
  }).join("");
}

function renderSeoSummary() {
  const container = document.querySelector("#seoSummary");
  if (!container) return;
  if (!state.seoResults.length) {
    container.innerHTML = "";
    return;
  }
  
  const filtered = filteredSeoResults(true);
  const count = filtered.length;
  if (!count) {
    container.innerHTML = emptyCard("No videos match the selected filter.");
    return;
  }
  
  let totalScore = 0;
  let optimizedCount = 0;
  let descIssues = 0;
  let tagsIssues = 0;
  let hashtagIssues = 0;
  
  filtered.forEach(video => {
    totalScore += video.score;
    if (video.score === 100) optimizedCount++;
    
    video.gaps.forEach(gap => {
      const lower = gap.toLowerCase();
      if (lower.includes("description")) {
        descIssues++;
      } else if (lower.includes("hashtag")) {
        hashtagIssues++;
      } else if (lower.includes("tag")) {
        tagsIssues++;
      }
    });
  });
  
  const avgScore = Math.round(totalScore / count);
  
  container.innerHTML = `
    <div class="research-stat">
      <span>Average Score</span>
      <strong>${avgScore}/100</strong>
    </div>
    <div class="research-stat">
      <span>Fully Optimized</span>
      <strong>${optimizedCount} <small>/ ${count}</small></strong>
    </div>
    <div class="research-stat">
      <span>Desc. Gaps</span>
      <strong>${descIssues}</strong>
    </div>
    <div class="research-stat">
      <span>Tags Gaps</span>
      <strong>${tagsIssues}</strong>
    </div>
    <div class="research-stat">
      <span>Hashtag Gaps</span>
      <strong>${hashtagIssues}</strong>
    </div>
  `;
}

function filteredSeoResults(includeOptimized = false) {
  let list = [...state.seoResults];
  
  if (state.seoFilter) {
    list = list.filter(video => {
      const candidate = getCandidateName(video.channelTitle);
      return candidate === state.seoFilter;
    });
  }
  
  if (!includeOptimized) {
    list = list.filter(video => video.gaps && video.gaps.length > 0);
  }
  
  return list;
}

function renderSeoAuditResults() {
  const container = document.querySelector("#seoResults");
  if (!container) return;
  if (!state.seoResults.length) {
    container.innerHTML = emptyCard("No audit results. Select an audit target and click Run audit to analyze content.");
    return;
  }
  
  const filtered = filteredSeoResults();
  if (state.seoSortLowestFirst) {
    filtered.sort((a, b) => a.score - b.score);
  } else {
    filtered.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
  }
  
  if (!filtered.length) {
    container.innerHTML = emptyCard("No videos with gaps found for this Candidate. Good job!");
    return;
  }
  
  container.innerHTML = `
    <div class="research-table seo-table">
      <div class="research-row research-head seo-row-head">
        <span>Content</span>
        <span>Views</span>
        <span>Format</span>
        <span>Score</span>
        <span>Identified Gaps</span>
        <span>Actions</span>
      </div>
      ${filtered.map(video => {
        const scoreClass = video.score === 100 ? "score-green" : video.score >= 70 ? "score-yellow" : "score-red";
        
        let gapsHtml = "";
        if (video.gaps.length === 0) {
          gapsHtml = `<span class="gap-optimized">No gaps found</span>`;
        } else {
          gapsHtml = `<ul class="gap-list">${video.gaps.map(gap => `<li>${escapeHtml(gap)}</li>`).join("")}</ul>`;
        }
        
        const isHighPerformer = video.views > video.channelAverageViews * 1.5;
        const outlierTag = isHighPerformer ? `<mark class="channel-tag own-tag outlier-badge">High Performer</mark>` : "";
        
        return `
          <div class="research-row seo-row">
            <div class="seo-title-cell-with-thumb">
              <img src="https://i.ytimg.com/vi/${video.id}/default.jpg" class="seo-video-thumb" alt="" />
              <div class="research-title-cell">
                <strong>${escapeHtml(video.title)}</strong>
                <small>${escapeHtml(video.channelTitle)} · ${escapeHtml(formatPublishedAt(video.publishedAt))}</small>
                ${outlierTag}
              </div>
            </div>
            <div class="seo-views-cell">
              <strong>${video.views.toLocaleString()}</strong>
              <small class="avg-subtext">avg: ${Math.round(video.channelAverageViews).toLocaleString()}</small>
            </div>
            <span class="research-format">${escapeHtml(video.format)}</span>
            <div class="score-cell">
              <span class="score-badge ${scoreClass}">${video.score}</span>
            </div>
            <div class="gaps-cell">
              ${gapsHtml}
            </div>
            <div class="actions-cell">
              <a class="link-chip watch-link" href="https://www.youtube.com/watch?v=${video.id}" target="_blank" rel="noreferrer">Open</a>
            </div>
          </div>
        `;
      }).join("")}
    </div>
  `;
}

async function showSeoSuggestions(videoId, channelId, title) {
  const dialog = document.querySelector("#seoSuggestDialog");
  const descArea = document.querySelector("#seoSuggestDescription");
  const tagsArea = document.querySelector("#seoSuggestTags");
  const hashInput = document.querySelector("#seoSuggestHashtags");
  const reasonText = document.querySelector("#seoSuggestReason");
  const dialogTitle = document.querySelector("#seoSuggestTitle");
  
  if (!dialog || !descArea || !tagsArea || !hashInput || !reasonText || !dialogTitle) return;
  
  dialogTitle.textContent = `Optimized Metadata - ${truncateTitle(title, 35)}`;
  descArea.value = "Loading suggestions...";
  tagsArea.value = "Loading suggestions...";
  hashInput.value = "Loading suggestions...";
  reasonText.textContent = "Asking Claude to generate optimized metadata...";
  
  dialog.showModal();
  
  try {
    const data = await api("/api/seo/suggest", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ videoId, channelId }),
    });
    
    descArea.value = data.description || "";
    tagsArea.value = data.tags || "";
    hashInput.value = data.hashtags || "";
    reasonText.textContent = data.reasoning || "";
  } catch (error) {
    descArea.value = "Failed to load suggestions.";
    tagsArea.value = "Failed to load suggestions.";
    hashInput.value = "Failed to load suggestions.";
    reasonText.textContent = error.message;
  }
}



function exportSeoToCsv() {
  const filtered = filteredSeoResults();
  if (!filtered.length) return;
  
  const headers = ["Channel Name", "Video Title", "YouTube Link", "Score", "Format", "Views", "Average Channel Views", "Gaps/Issues"];
  
  const rows = filtered.map(video => {
    const gapsText = video.gaps.length > 0 ? video.gaps.join("; ") : "No gaps found";
    const link = `https://www.youtube.com/watch?v=${video.id}`;
    return [
      video.channelTitle,
      video.title,
      link,
      video.score,
      video.format,
      video.views,
      Math.round(video.channelAverageViews),
      gapsText
    ];
  });
  
  const csvContent = [
    headers.map(h => `"${h.replace(/"/g, '""')}"`).join(","),
    ...rows.map(row => row.map(cell => {
      const val = cell === null || cell === undefined ? "" : String(cell);
      return `"${val.replace(/"/g, '""')}"`;
    }).join(","))
  ].join("\n");
  
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.setAttribute("href", url);
  link.setAttribute("download", `youtube_seo_audit_${state.seoFilter}_${new Date().toISOString().slice(0, 10)}.csv`);
  link.style.visibility = "hidden";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

async function copySeoForSheets(btn) {
  const filtered = filteredSeoResults();
  if (!filtered.length) return;
  
  const headers = ["Channel Name", "Video Title", "YouTube Link", "Score", "Format", "Views", "Average Channel Views", "Gaps/Issues"];
  
  const rows = filtered.map(video => {
    const gapsText = video.gaps.length > 0 ? video.gaps.join("; ") : "No gaps found";
    const link = `https://www.youtube.com/watch?v=${video.id}`;
    const hyperlinkFormula = `=HYPERLINK("${link}", "Watch Video")`;
    return [
      video.channelTitle,
      video.title,
      hyperlinkFormula,
      video.score,
      video.format,
      video.views,
      Math.round(video.channelAverageViews),
      gapsText
    ];
  });
  
  const tsvContent = [
    headers.join("\t"),
    ...rows.map(row => row.map(cell => {
      const val = cell === null || cell === undefined ? "" : String(cell);
      return val.replace(/\t/g, " ").replace(/\r?\n/g, " ");
    }).join("\t"))
  ].join("\n");
  
  try {
    await navigator.clipboard.writeText(tsvContent);
    const originalText = btn.textContent;
    btn.textContent = "Copied TSV!";
    btn.classList.add("copied");
    setTimeout(() => {
      btn.textContent = originalText;
      btn.classList.remove("copied");
    }, 1500);
  } catch (err) {
    alert("Failed to copy data. Please try again.");
  }
}

async function loadYtmAudit(options = {}) {
  const resultsContainer = document.querySelector("#ytmResults");
  if (!resultsContainer) return;
  
  const targetSelect = document.querySelector("#ytmAuditTarget");
  const manager = targetSelect ? targetSelect.value : "All";
  
  if (!options.force && state.ytmResults.length) {
    renderYtmAuditView();
    return;
  }
  
  resultsContainer.innerHTML = emptyCard("Audit in progress... Please see the progress bar above.");
  
  const ytmSummary = document.querySelector("#ytmSummary");
  if (ytmSummary) ytmSummary.innerHTML = "";
  const ytmChannelFilters = document.querySelector("#ytmChannelFilters");
  if (ytmChannelFilters) ytmChannelFilters.innerHTML = "";
  
  const ytmSteps = [
    { time: 0, text: "Fetching uploads and channel stats..." },
    { time: 4, text: "Auditing pinned comments & description links..." },
    { time: 10, text: "Testing redirect links..." },
    { time: 20, text: "Calculating owner comment reply rates..." },
    { time: 35, text: "Finalizing manager compliance reports..." }
  ];
  const progressBar = startProgressBar("ytmProgressBarContainer", "ytmProgressBarFill", "ytmProgressBarLabel", ytmSteps);
  
  try {
    const managerQuery = `manager=${encodeURIComponent(manager)}`;
    const forceQuery = options.force ? "&force=1" : "";
    const data = await api(`/api/ytm/audit?${managerQuery}${forceQuery}`);
    if (progressBar) progressBar.stop(true);
    state.ytmResults = data.videos || [];
    state.ytmFilter = manager;
    renderYtmAuditView();
  } catch (error) {
    if (progressBar) progressBar.stop(false);
    resultsContainer.innerHTML = emptyCard(error.message);
  }
}

function renderYtmAuditView() {
  if (state.activeView !== "ytm") return;
  renderYtmChannelFilters();
  renderYtmSummary();
  renderYtmAuditResults();
  
  const hasResults = state.ytmResults && state.ytmResults.length > 0;
  const csvBtn = document.querySelector("#ytmExportCsvButton");
  const sheetsBtn = document.querySelector("#ytmCopySheetsButton");
  if (csvBtn) csvBtn.disabled = !hasResults;
  if (sheetsBtn) sheetsBtn.disabled = !hasResults;
}

function renderYtmChannelFilters() {
  const container = document.querySelector("#ytmChannelFilters");
  if (!container) return;
  if (!state.ytmResults.length) {
    container.innerHTML = "";
    return;
  }
  
  const managersWithResults = new Set();
  state.ytmResults.forEach(video => {
    const manager = getYtmName(video.channelTitle, video.channelId);
    managersWithResults.add(manager);
  });
  
  const managerList = ["Nitin", "Shubham", "Raubnish", "Narendra/Amit", "Abhinav", "Shukendu", "Ashish Tyagi", "Lubna", "Vivek", "Govardhan", "Other"].filter(m => managersWithResults.has(m));
  
  if (!managerList.includes(state.ytmFilter)) {
    state.ytmFilter = managerList[0] || "Other";
  }
  
  container.innerHTML = managerList.map(manager => {
    const isActive = state.ytmFilter === manager;
    return `<button class="filter-chip ${isActive ? "active" : ""}" type="button" data-ytm-filter="${escapeHtml(manager)}">${escapeHtml(manager)}</button>`;
  }).join("");
}

function renderYtmSummary() {
  const container = document.querySelector("#ytmSummary");
  if (!container) return;
  if (!state.ytmResults.length) {
    container.innerHTML = "";
    return;
  }
  
  const filtered = filteredYtmResults(true);
  const count = filtered.length;
  if (!count) {
    container.innerHTML = emptyCard("No videos match the selected filter.");
    return;
  }
  
  let totalScore = 0;
  let optimizedCount = 0;
  let linkPinnedMissing = 0;
  let linkDescMissing = 0;
  let playlistIssues = 0;
  let notRepliedHearted = 0;
  
  filtered.forEach(video => {
    totalScore += video.score;
    if (video.score === 100) optimizedCount++;
    
    video.gaps.forEach(gap => {
      const lower = gap.toLowerCase();
      if (lower.includes("link missing in pinned comment")) {
        linkPinnedMissing++;
      } else if (lower.includes("link missing in description")) {
        linkDescMissing++;
      } else if (lower.includes("playlist")) {
        playlistIssues++;
      } else if (lower.includes("not replied/not hearted")) {
        notRepliedHearted++;
      }
    });
  });
  
  const avgScore = Math.round(totalScore / count);
  
  container.innerHTML = `
    <div class="research-stat">
      <span>Average Score</span>
      <strong>${avgScore}/100</strong>
    </div>
    <div class="research-stat">
      <span>Fully Audited</span>
      <strong>${optimizedCount} <small>/ ${count}</small></strong>
    </div>
    <div class="research-stat">
      <span>Link in Pinned Missing</span>
      <strong>${linkPinnedMissing}</strong>
    </div>
    <div class="research-stat">
      <span>Link in Desc Missing</span>
      <strong>${linkDescMissing}</strong>
    </div>
    <div class="research-stat">
      <span>Playlist Missing</span>
      <strong>${playlistIssues}</strong>
    </div>
    <div class="research-stat">
      <span>Not Replied/Hearted</span>
      <strong>${notRepliedHearted}</strong>
    </div>
  `;
}

function filteredYtmResults(includeOptimized = false) {
  let list = [...state.ytmResults];
  
  if (state.ytmFilter) {
    list = list.filter(video => {
      const manager = getYtmName(video.channelTitle, video.channelId);
      return manager === state.ytmFilter;
    });
  }
  
  if (!includeOptimized) {
    list = list.filter(video => video.gaps && video.gaps.length > 0);
  }
  
  return list;
}

function renderYtmAuditResults() {
  const container = document.querySelector("#ytmResults");
  if (!container) return;
  if (!state.ytmResults.length) {
    container.innerHTML = emptyCard("No audit results. Select a target YTM and click Run YTM audit to analyze content.");
    return;
  }
  
  const filtered = filteredYtmResults();
  if (state.ytmSortLowestFirst) {
    filtered.sort((a, b) => a.score - b.score);
  } else {
    filtered.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
  }
  
  if (!filtered.length) {
    container.innerHTML = emptyCard("No videos with gaps found for this YTM. All checked tasks OK!");
    return;
  }
  
  container.innerHTML = `
    <div class="research-table ytm-table">
      <div class="research-row research-head ytm-row-head">
        <span>Content</span>
        <span>Views</span>
        <span>Format</span>
        <span>Score</span>
        <span>Identified Gaps / Operational Issues</span>
        <span>Actions</span>
      </div>
      ${filtered.map(video => {
        const scoreClass = video.score === 100 ? "score-green" : video.score >= 70 ? "score-yellow" : "score-red";
        
        let gapsHtml = "";
        if (video.gaps.length === 0) {
          gapsHtml = `<span class="gap-optimized">All checked tasks OK</span>`;
        } else {
          gapsHtml = `<ul class="gap-list">${video.gaps.map(gap => {
            let escaped = escapeHtml(gap);
            escaped = escaped.replace(/(https?:\/\/link\.testbook\.com\/[^\s,()]+)/g, '<a href="$1" target="_blank" rel="noreferrer" class="gap-link">$1</a>');
            return `<li>${escaped}</li>`;
          }).join("")}</ul>`;
        }
        
        return `
          <div class="research-row ytm-row">
            <div class="seo-title-cell-with-thumb">
              <img src="https://i.ytimg.com/vi/${video.id}/default.jpg" class="seo-video-thumb" alt="" />
              <div class="research-title-cell">
                <strong>${escapeHtml(video.title)}</strong>
                <small>${escapeHtml(video.channelTitle)} · ${escapeHtml(formatPublishedAt(video.publishedAt))}</small>
              </div>
            </div>
            <div class="seo-views-cell">
              <strong>${video.views.toLocaleString()}</strong>
            </div>
            <span class="research-format">${escapeHtml(video.format)}</span>
            <div class="score-cell">
              <span class="score-badge ${scoreClass}">${video.score}</span>
            </div>
            <div class="gaps-cell">
              ${gapsHtml}
            </div>
            <div class="actions-cell" style="display: flex; gap: 8px;">
              <a class="link-chip watch-link" href="https://www.youtube.com/watch?v=${video.id}" target="_blank" rel="noreferrer">Open</a>
              ${video.unansweredComments && video.unansweredComments.length > 0 ? `
                <button class="link-chip comments-toggle-btn" data-drawer-id="drawer-${video.id}">Comments (${video.unansweredComments.length})</button>
              ` : ''}
            </div>
          </div>
          ${video.unansweredComments && video.unansweredComments.length > 0 ? `
            <div class="ytm-comments-drawer is-hidden" id="drawer-${video.id}">
              <h3>Unanswered Comments</h3>
              <div class="comments-list" style="display: flex; flex-direction: column; gap: 12px; margin-top: 10px;">
                ${video.unansweredComments.map(comment => `
                  <div class="ytm-comment-item" id="comment-${comment.id}">
                    <div class="comment-header">
                      <img src="${comment.authorProfileImage || 'https://www.gravatar.com/avatar/00000000000000000000000000000000?d=mp&f=y'}" class="comment-author-img" />
                      <strong>${escapeHtml(comment.authorName)}</strong>
                      <span class="comment-time">${escapeHtml(formatPublishedAt(comment.publishedAt))}</span>
                    </div>
                    <div class="comment-body">
                      <p>${escapeHtml(comment.text)}</p>
                    </div>
                    <div class="comment-reply-box">
                      <textarea placeholder="Write a reply..." id="reply-text-${comment.id}"></textarea>
                      <div class="comment-reply-actions" style="justify-content: flex-end;">
                        <button type="button" class="connect-button send-reply-btn" data-channel-id="${video.channelId}" data-parent-id="${comment.id}" data-comment-id="${comment.id}">Send Reply</button>
                      </div>
                    </div>
                  </div>
                `).join("")}
              </div>
            </div>
          ` : ''}
        `;
      }).join("")}
    </div>
  `;
}

function exportYtmToCsv() {
  const filtered = filteredYtmResults();
  if (!filtered.length) return;
  
  const headers = ["Channel Name", "Video Title", "YouTube Link", "Score", "Format", "Views", "Gaps/Issues"];
  
  const rows = filtered.map(video => {
    const gapsText = video.gaps.length > 0 ? video.gaps.join("; ") : "No gaps found";
    const link = `https://www.youtube.com/watch?v=${video.id}`;
    return [
      video.channelTitle,
      video.title,
      link,
      video.score,
      video.format,
      video.views,
      gapsText
    ];
  });
  
  const csvContent = [
    headers.map(h => `"${h.replace(/"/g, '""')}"`).join(","),
    ...rows.map(row => row.map(cell => {
      const val = cell === null || cell === undefined ? "" : String(cell);
      return `"${val.replace(/"/g, '""')}"`;
    }).join(","))
  ].join("\n");
  
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.setAttribute("href", url);
  link.setAttribute("download", `youtube_ytm_audit_${state.ytmFilter}_${new Date().toISOString().slice(0, 10)}.csv`);
  link.style.visibility = "hidden";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

async function copyYtmForSheets(btn) {
  const filtered = filteredYtmResults();
  if (!filtered.length) return;
  
  const headers = ["Channel Name", "Video Title", "YouTube Link", "Score", "Format", "Views", "Gaps/Issues"];
  
  const rows = filtered.map(video => {
    const gapsText = video.gaps.length > 0 ? video.gaps.join("; ") : "No gaps found";
    const link = `https://www.youtube.com/watch?v=${video.id}`;
    const hyperlinkFormula = `=HYPERLINK("${link}", "Watch Video")`;
    return [
      video.channelTitle,
      video.title,
      hyperlinkFormula,
      video.score,
      video.format,
      video.views,
      gapsText
    ];
  });
  
  const tsvContent = [
    headers.join("\t"),
    ...rows.map(row => row.map(cell => {
      const val = cell === null || cell === undefined ? "" : String(cell);
      return val.replace(/\t/g, " ").replace(/\r?\n/g, " ");
    }).join("\t"))
  ].join("\n");
  
  try {
    await navigator.clipboard.writeText(tsvContent);
    const originalText = btn.textContent;
    btn.textContent = "Copied TSV!";
    btn.classList.add("copied");
    setTimeout(() => {
      btn.textContent = originalText;
      btn.classList.remove("copied");
    }, 1500);
  } catch (err) {
    alert("Failed to copy data. Please try again.");
  }
}

// Global click event handlers for comments drawers and reply actions
document.addEventListener("click", async (event) => {
  if (state.activeView !== "ytm") return;
  const toggleBtn = event.target.closest(".comments-toggle-btn");
  if (toggleBtn) {
    const drawerId = toggleBtn.dataset.drawerId;
    const drawer = document.getElementById(drawerId);
    if (drawer) {
      drawer.classList.toggle("is-hidden");
    }
    return;
  }
  

  
  const sendBtn = event.target.closest(".send-reply-btn");
  if (sendBtn) {
    const channelId = sendBtn.dataset.channelId;
    const parentId = sendBtn.dataset.parentId;
    const commentId = sendBtn.dataset.commentId;
    const textarea = document.getElementById(`reply-text-${commentId}`);
    if (textarea) {
      const replyText = textarea.value.trim();
      if (!replyText) {
        alert("Please write a reply first.");
        return;
      }
      sendBtn.disabled = true;
      const originalText = sendBtn.innerHTML;
      sendBtn.innerHTML = "Sending...";
      try {
        await api("/api/ytm/comment/reply", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ channelId, parentId, replyText }),
        });
        
        const item = document.getElementById(`comment-${commentId}`);
        if (item) {
          const parent = item.parentElement;
          item.remove();
          
          if (parent && parent.children.length === 0) {
            const drawer = parent.closest(".ytm-comments-drawer");
            if (drawer) {
              drawer.innerHTML = "<p class='no-comments-msg' style='color: green; font-weight: 500;'>All comments replied to!</p>";
              setTimeout(() => drawer.classList.add("is-hidden"), 2000);
            }
            
            const videoId = drawer.id.replace("drawer-", "");
            const stateVideo = state.ytmResults.find(v => v.id === videoId);
            if (stateVideo) {
              stateVideo.unansweredComments = [];
              stateVideo.gaps = stateVideo.gaps.filter(g => g !== "Not Replied/Not Hearted");
              stateVideo.score = Math.max(0, 100 - stateVideo.gaps.length * 25);
              renderYtmAuditResults();
            }
          } else {
            const drawerId = parent.closest(".ytm-comments-drawer").id;
            const videoId = drawerId.replace("drawer-", "");
            const stateVideo = state.ytmResults.find(v => v.id === videoId);
            if (stateVideo) {
              stateVideo.unansweredComments = stateVideo.unansweredComments.filter(c => c.id !== commentId);
              const toggleBtnForVideo = document.querySelector(`[data-drawer-id="${drawerId}"]`);
              if (toggleBtnForVideo) {
                toggleBtnForVideo.textContent = `Comments (${stateVideo.unansweredComments.length})`;
              }
            }
          }
        }
      } catch (err) {
        alert("Failed to send reply: " + err.message);
      } finally {
        sendBtn.disabled = false;
        sendBtn.innerHTML = originalText;
      }
    }
    return;
  }
});


function truncateVideoTitle(title, limit = 60) {
  if (!title) return "";
  if (title.length <= limit) return title;
  return title.substring(0, limit) + "...";
}

// Keywords View Controllers
async function loadKeywords(options = {}) {
  const automatedContainer = document.querySelector("#automatedKeywordsTableContainer");
  const manualContainer = document.querySelector("#manualKeywordsTableContainer");
  
  const isAdminUser = (state.isAuditAdmin === undefined || state.isAuditAdmin === true);
  const ytmList = [];
  if (isAdminUser) {
    ytmList.push("Admin");
  }
  ytmList.push("Saijal", "Mohit", "Vinayak", "Aditya", "Vivek", "Govardhan");
  
  if (!state.selectedKeywordsYtm) {
    state.selectedKeywordsYtm = ytmList[0];
  }

  renderKeywordsYtmFilters(ytmList);

  const managerContainerEl = document.querySelector("#keywordsManagerContainer");
  const adminContainerEl = document.querySelector("#keywordsAdminContainer");
  const channelSelectContainer = document.querySelector("#rankTrackerChannelSelectContainer");

  if (state.selectedKeywordsYtm === "Admin") {
    if (managerContainerEl) managerContainerEl.classList.add("is-hidden");
    if (adminContainerEl) adminContainerEl.classList.remove("is-hidden");
    if (channelSelectContainer) channelSelectContainer.style.display = "none";
    await loadAdminKeywords(options);
  } else {
    if (adminContainerEl) adminContainerEl.classList.add("is-hidden");
    if (managerContainerEl) managerContainerEl.classList.remove("is-hidden");
    if (channelSelectContainer) channelSelectContainer.style.display = "flex";
    
    try {
      if (!options.force) {
        automatedContainer.innerHTML = emptyCard("Loading rankings...");
        manualContainer.innerHTML = emptyCard("Loading rankings...");
      }
      const data = await api("/api/keywords/rankings?ytm=" + encodeURIComponent(state.selectedKeywordsYtm));
      state.keywordRankings = data;

      const select = document.querySelector("#rankTrackerChannelSelect");
      if (select) {
        const channels = data.channels || [];
        select.innerHTML = channels.map(ch => `<option value="${escapeHtml(ch.id)}">${escapeHtml(ch.name)}</option>`).join("");
        
        if (channels.length > 0) {
          const hasSelected = channels.some(ch => ch.id === state.rankTrackerSelectedChannelId);
          if (!hasSelected) {
            state.rankTrackerSelectedChannelId = channels[0].id;
          }
          select.value = state.rankTrackerSelectedChannelId;
        } else {
          state.rankTrackerSelectedChannelId = null;
        }

        select.onchange = (e) => {
          state.rankTrackerSelectedChannelId = e.target.value;
          renderKeywordsView();
        };
      }

      renderKeywordsView();
    } catch (err) {
      automatedContainer.innerHTML = emptyCard(err.message || "Failed to load keyword rankings.");
      manualContainer.innerHTML = "";
    }
  }
}

function renderKeywordsYtmFilters(ytmList) {
  const container = document.querySelector("#keywordsYtmFilters");
  if (!container) return;
  
  container.innerHTML = ytmList.map(ytm => {
    const isActive = state.selectedKeywordsYtm === ytm;
    return `<button class="filter-chip ${isActive ? "active" : ""}" type="button" data-keywords-ytm="${escapeHtml(ytm)}">${escapeHtml(ytm)}</button>`;
  }).join("");

  container.querySelectorAll("[data-keywords-ytm]").forEach(btn => {
    btn.addEventListener("click", () => {
      state.selectedKeywordsYtm = btn.dataset.keywordsYtm;
      loadKeywords();
    });
  });
}

function matchRankFilter(rank, filter) {
  if (filter === "all" || !filter) return true;
  if (rank === "quota_exceeded" || rank === null || rank === undefined) return false;
  const r = Number(rank);
  if (isNaN(r)) return false;
  
  if (filter === "top3") return r >= 1 && r <= 3;
  if (filter === "page1") return r >= 4 && r <= 10;
  if (filter === "opportunity") return r >= 11 && r <= 20;
  return true;
}

function renderKeywordsView() {
  const data = state.keywordRankings || { rankings: { automated: [], manual: [] }, manualKeywords: [], lastUpdated: null, channels: [] };
  
  const lastUpdatedEl = document.querySelector("#keywordsLastUpdated");
  if (lastUpdatedEl) {
    if (data.lastUpdated) {
      lastUpdatedEl.textContent = "Last scanned: " + new Date(data.lastUpdated).toLocaleString();
    } else {
      lastUpdatedEl.textContent = "Not scanned yet";
    }
  }

  // Cooldown check for refresh rankings button (30 minutes per channel)
  const selectedChannelId = state.rankTrackerSelectedChannelId;
  const selectChan = (data.channels || []).find(c => c.id === selectedChannelId);
  const refreshBtn = document.querySelector("#keywordsRefreshButton");
  
  if (state.selectedKeywordsYtm !== "Admin" && selectChan && selectChan.lastUpdated) {
    const last = new Date(selectChan.lastUpdated).getTime();
    const elapsedMin = (Date.now() - last) / (60 * 1000);
    if (elapsedMin < 30) {
      const remaining = Math.ceil(30 - elapsedMin);
      if (refreshBtn) {
        refreshBtn.disabled = true;
        refreshBtn.textContent = `Refresh (Cooldown: ${remaining}m)`;
        refreshBtn.style.opacity = "0.6";
        refreshBtn.style.cursor = "not-allowed";
      }
    } else {
      if (refreshBtn) {
        refreshBtn.disabled = false;
        refreshBtn.textContent = "Refresh rankings";
        refreshBtn.style.opacity = "1";
        refreshBtn.style.cursor = "pointer";
      }
    }
  } else {
    if (refreshBtn) {
      refreshBtn.disabled = false;
      refreshBtn.textContent = "Refresh rankings";
      refreshBtn.style.opacity = "1";
      refreshBtn.style.cursor = "pointer";
    }
  }

  const filterVal = state.keywordsRankFilter || "all";
  const chId = state.rankTrackerSelectedChannelId;
  const autoFiltered = (data.rankings?.automated || [])
    .filter(r => r.channelId === chId)
    .filter(r => matchRankFilter(r.currentRank, filterVal));
  const manualFiltered = (data.rankings?.manual || [])
    .filter(r => r.channelId === chId)
    .filter(r => matchRankFilter(r.currentRank, filterVal));

  renderKeywordTable(
    document.querySelector("#automatedKeywordsTableContainer"),
    autoFiltered,
    "automated"
  );

  renderKeywordTable(
    document.querySelector("#manualKeywordsTableContainer"),
    manualFiltered,
    "manual"
  );

  const trackBtn = document.querySelector("#addManualKeywordButton");
  const trackInput = document.querySelector("#manualKeywordInput");
  const currentManualCount = (data.manualKeywords || []).filter(kw => kw.channelId === chId).length;
  if (trackBtn && trackInput) {
    if (currentManualCount >= 50) {
      trackBtn.disabled = true;
      trackInput.disabled = true;
      trackInput.placeholder = "Limit of 50 keywords reached.";
    } else {
      trackBtn.disabled = false;
      trackInput.disabled = false;
      trackInput.placeholder = "e.g. ssc cgl 2026 classes";
    }
  }

  // Static layout: manual keywords section is always on top of automated
  const autoSec = document.querySelector("#automatedKeywordsSection");
  const manualSec = document.querySelector("#manualKeywordsSection");
  if (autoSec && manualSec) {
    manualSec.style.order = "1";
    autoSec.style.order = "2";
    manualSec.classList.toggle("is-hidden", currentManualCount === 0);
    if (currentManualCount === 0) {
      autoSec.style.borderTop = "none";
      autoSec.style.paddingTop = "0";
    } else {
      autoSec.style.borderTop = "1px solid var(--line)";
      autoSec.style.paddingTop = "24px";
    }
  }
}

function renderKeywordTable(container, list, type) {
  if (!container) return;
  if (!list || !list.length) {
    container.innerHTML = emptyCard(
      type === "automated" 
        ? "No search keywords found. Run a refresh to scan your channels' traffic data."
        : "No custom keywords tracked yet. Select a target channel, type a keyword above, and click Track Keyword."
    );
    return;
  }

  const isAutomated = type === "automated";
  
  container.innerHTML = `
    <div class="keywords-table">
      <div class="keywords-row keywords-head ${type}-row">
        <span>Rank & Trend</span>
        <span>Keyword</span>
        <span>Channel</span>
        ${isAutomated ? "<span>Views (7d)</span>" : ""}
        <span>Best Ranking Video</span>
        <span>Search Page</span>
        ${!isAutomated ? "<span>Action</span>" : ""}
      </div>
      ${list.map(row => {
        const trendHtml = getTrendBadgeHtml(row.currentRank, row.previousRank);
        const searchUrl = "https://www.youtube.com/results?search_query=" + encodeURIComponent(row.keyword);
        
        let videoHtml = `<span style="color: var(--muted); font-size: 12px;">Not in Top 50</span>`;
        if (row.videoId) {
          const videoUrl = "https://www.youtube.com/watch?v=" + row.videoId;
          videoHtml = `
            <div class="keywords-video-cell">
              <strong title="${escapeHtml(row.videoTitle)}">${escapeHtml(truncateVideoTitle(row.videoTitle))}</strong>
              <a href="${videoUrl}" target="_blank" rel="noreferrer" style="font-size: 11px; color: var(--accent); text-decoration: none;">Watch video</a>
            </div>
          `;
        }

        const isQuota = row.currentRank === "quota_exceeded";
        const rankText = isQuota ? "Quota Reached" : (row.currentRank ? `#${row.currentRank}` : "50+");
        const rankFontSize = isQuota ? "11px" : "14px";
        return `
          <div class="keywords-row ${type}-row">
            <div class="rank-badge-container rank-badge-clickable" style="cursor: pointer;" data-keyword="${escapeHtml(row.keyword)}" data-history-json="${escapeHtml(JSON.stringify(row.history || []))}">
              <strong style="font-size: ${rankFontSize}; white-space: nowrap;">${rankText}</strong>
              ${trendHtml}
            </div>
            <strong>${escapeHtml(row.keyword)}</strong>
            <strong style="color: var(--accent);">${escapeHtml(row.channelName || "")}</strong>
            ${isAutomated ? `<strong style="font-weight: 700;">${Number(row.views || 0).toLocaleString()}</strong>` : ""}
            ${videoHtml}
            <a class="link-chip" href="${searchUrl}" target="_blank" rel="noreferrer" style="text-align: center; border-radius: 6px;">YouTube</a>
            ${!isAutomated ? `
              <button class="keywords-delete-btn" type="button" data-delete-keyword="${escapeHtml(row.keyword)}" data-channel-id="${escapeHtml(row.channelId)}">Delete</button>
            ` : ""}
          </div>
        `;
      }).join("")}
    </div>
  `;

  attachRankBadgeClickListeners(container);

  if (!isAutomated) {
    container.querySelectorAll("[data-delete-keyword]").forEach(btn => {
      btn.addEventListener("click", async () => {
        const kw = btn.dataset.deleteKeyword;
        if (confirm(`Remove keyword "${kw}" from manual tracking?`)) {
          try {
            btn.disabled = true;
            btn.textContent = "Deleting...";
            await api("/api/keywords/manual", {
              method: "DELETE",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                ytm: state.selectedKeywordsYtm,
                channelId: btn.dataset.channelId,
                keyword: kw
              })
            });
            // Reload rankings for current YTM
            const data = await api("/api/keywords/rankings?ytm=" + encodeURIComponent(state.selectedKeywordsYtm));
            state.keywordRankings = data;
            renderKeywordsView();
          } catch (err) {
            alert(err.message || "Failed to delete keyword.");
            btn.disabled = false;
            btn.textContent = "Delete";
          }
        }
      });
    });
  }
}

function getTrendBadgeHtml(curr, prev) {
  if (curr === "quota_exceeded" || prev === "quota_exceeded") {
    return `<span class="trend-badge trend-flat">-</span>`;
  }
  if (prev === null || prev === undefined) {
    return `<span class="trend-badge trend-flat">New</span>`;
  }
  if (curr === null || curr === undefined) {
    return `<span class="trend-badge trend-down">Lost</span>`;
  }
  const delta = prev - curr;
  if (delta > 0) {
    return `<span class="trend-badge trend-up">▲ ${delta}</span>`;
  }
  if (delta < 0) {
    return `<span class="trend-badge trend-down">▼ ${Math.abs(delta)}</span>`;
  }
  return `<span class="trend-badge trend-flat">=</span>`;
}

async function addManualKeyword() {
  const input = document.querySelector("#manualKeywordInput");
  const keyword = (input?.value || "").trim();

  if (!keyword) return;

  const btn = document.querySelector("#addManualKeywordButton");
  try {
    if (btn) btn.disabled = true;
    if (input) input.disabled = true;
    
    await api("/api/keywords/manual", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ytm: state.selectedKeywordsYtm,
        channelId: state.rankTrackerSelectedChannelId,
        keyword: keyword
      })
    });
    
    // Reload rankings
    const data = await api("/api/keywords/rankings?ytm=" + encodeURIComponent(state.selectedKeywordsYtm));
    state.keywordRankings = data;
    renderKeywordsView();
    if (input) input.value = "";
  } catch (err) {
    alert(err.message || "Failed to add manual keyword.");
  } finally {
    if (btn) btn.disabled = false;
    if (input) input.disabled = false;
    if (input) input.focus();
  }
}

async function refreshKeywordRankings() {
  const refreshBtn = document.querySelector("#keywordsRefreshButton");
  const progressBarContainer = document.querySelector("#keywordsProgressBarContainer");
  const progressBarFill = document.querySelector("#keywordsProgressBarFill");
  const progressBarLabel = document.querySelector("#keywordsProgressBarLabel");

  try {
    if (refreshBtn) refreshBtn.disabled = true;
    if (progressBarContainer) progressBarContainer.classList.remove("is-hidden");
    
    let progress = 0;
    const interval = setInterval(() => {
      progress += 10;
      if (progress > 90) progress = 90;
      if (progressBarFill) progressBarFill.style.width = progress + "%";
      if (progressBarLabel) progressBarLabel.textContent = `Scanning target channel's keyword rankings... (${progress}%)`;
    }, 1000);

    const updated = await api("/api/keywords/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ytm: state.selectedKeywordsYtm,
        channelId: state.rankTrackerSelectedChannelId
      })
    });
    
    clearInterval(interval);
    if (progressBarFill) progressBarFill.style.width = "100%";
    if (progressBarLabel) progressBarLabel.textContent = "Scan complete! (100%)";
    
    state.keywordRankings = updated;
    renderKeywordsView();
    
    setTimeout(() => {
      if (progressBarContainer) progressBarContainer.classList.add("is-hidden");
    }, 1500);
  } catch (err) {
    alert(err.message || "Failed to refresh keyword rankings.");
    if (progressBarContainer) progressBarContainer.classList.add("is-hidden");
  } finally {
    if (refreshBtn) refreshBtn.disabled = false;
  }
}

function attachRankBadgeClickListeners(container) {
  if (!container) return;
  container.querySelectorAll(".rank-badge-clickable").forEach(badge => {
    badge.addEventListener("click", () => {
      const kw = badge.dataset.keyword;
      const history = JSON.parse(badge.dataset.historyJson || "[]");
      showRankHistoryDialog(kw, history);
    });
  });
}

function showRankHistoryDialog(keyword, history = []) {
  document.querySelector("#rankHistoryDialog")?.remove();

  const dialog = document.createElement("dialog");
  dialog.id = "rankHistoryDialog";
  dialog.style.padding = "24px";
  dialog.style.borderRadius = "12px";
  dialog.style.border = "1px solid var(--line)";
  dialog.style.background = "#fff";
  dialog.style.boxShadow = "0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04)";
  dialog.style.maxWidth = "450px";
  dialog.style.width = "90%";
  dialog.style.outline = "none";

  const titleHtml = `<h3 style="margin-top: 0; margin-bottom: 16px; font-size: 16px; font-weight: 700;">Ranking History: <span style="color: var(--accent);">${escapeHtml(keyword)}</span></h3>`;
  
  let contentHtml = "";
  if (!history || !history.length) {
    contentHtml = `<p style="color: var(--muted); font-size: 13px; margin-bottom: 20px;">No historical ranking data available for this keyword.</p>`;
  } else {
    const sortedHistory = [...history].sort((a, b) => new Date(b.date) - new Date(a.date));
    contentHtml = `
      <div style="max-height: 250px; overflow-y: auto; margin-bottom: 20px; border: 1px solid var(--line); border-radius: 6px;">
        <table style="width: 100%; border-collapse: collapse; font-size: 13px; text-align: left;">
          <thead>
            <tr style="background: var(--surface); border-bottom: 1px solid var(--line);">
              <th style="padding: 10px 12px; font-weight: 600; color: var(--muted);">Date</th>
              <th style="padding: 10px 12px; font-weight: 600; color: var(--muted);">Position</th>
            </tr>
          </thead>
          <tbody>
            ${sortedHistory.map(h => {
              const rankVal = h.rank === "quota_exceeded" ? "Quota Reached" : (h.rank ? `#${h.rank}` : "50+");
              return `
                <tr style="border-bottom: 1px solid var(--line);">
                  <td style="padding: 10px 12px;">${escapeHtml(h.date)}</td>
                  <td style="padding: 10px 12px; font-weight: 700;">${rankVal}</td>
                </tr>
              `;
            }).join("")}
          </tbody>
        </table>
      </div>
    `;
  }

  dialog.innerHTML = `
    ${titleHtml}
    ${contentHtml}
    <div style="display: flex; justify-content: flex-end; gap: 8px;">
      <button class="connect-button" type="button" id="closeRankHistoryBtn" style="margin: 0; padding: 8px 16px; font-size: 13px; background: var(--line); color: var(--ink); border: none;">Close</button>
    </div>
  `;

  document.body.appendChild(dialog);
  dialog.showModal();

  dialog.querySelector("#closeRankHistoryBtn")?.addEventListener("click", () => {
    dialog.close();
    dialog.remove();
  });
  
  dialog.addEventListener("click", (e) => {
    const rect = dialog.getBoundingClientRect();
    const isInDialog = (rect.top <= e.clientY && e.clientY <= rect.top + rect.height &&
      rect.left <= e.clientX && e.clientX <= rect.left + rect.width);
    if (!isInDialog) {
      dialog.close();
      dialog.remove();
    }
  });
}

function exportKeywordsToCsv() {
  let list = [];
  let filename = "youtube_rankings.csv";
  
  const filterVal = state.keywordsRankFilter || "all";

  if (state.selectedKeywordsYtm === "Admin") {
    filename = `admin_cross_channel_rankings_${filterVal}.csv`;
    const data = state.adminKeywordRankings || { rankings: [] };
    list = (data.rankings || []).filter(r => matchRankFilter(r.currentRank, filterVal));
  } else {
    filename = `${state.selectedKeywordsYtm}_rankings_${filterVal}.csv`;
    const data = state.keywordRankings || { rankings: { automated: [], manual: [] } };
    const autoFiltered = (data.rankings?.automated || []).filter(r => matchRankFilter(r.currentRank, filterVal));
    const manualFiltered = (data.rankings?.manual || []).filter(r => matchRankFilter(r.currentRank, filterVal));
    list = [...manualFiltered, ...autoFiltered];
  }

  if (!list.length) {
    alert("No keywords available to export with the current filters.");
    return;
  }

  const csvHeaders = ["Keyword", "Channel", "Current Rank", "Previous Rank", "Views (7d)", "Video Title", "Video URL"];
  const csvRows = list.map(r => {
    const rankText = r.currentRank === "quota_exceeded" ? "Quota Reached" : (r.currentRank || "50+");
    const prevRankText = r.previousRank === "quota_exceeded" ? "Quota Reached" : (r.previousRank || "-");
    const viewsVal = r.views || "-";
    const videoUrl = r.videoId ? `https://www.youtube.com/watch?v=${r.videoId}` : "";
    
    return [
      r.keyword,
      r.channelName || "",
      rankText,
      prevRankText,
      viewsVal,
      r.videoTitle || "",
      videoUrl
    ].map(val => `"${String(val || "").replace(/"/g, '""')}"`).join(",");
  });

  const csvContent = [csvHeaders.join(","), ...csvRows].join("\n");
  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  
  const link = document.createElement("a");
  link.setAttribute("href", url);
  link.setAttribute("download", filename);
  link.style.visibility = "hidden";
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}

function attachKeywordsRankFilterListeners() {
  document.querySelectorAll("[data-rank-filter]").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("[data-rank-filter]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      
      state.keywordsRankFilter = btn.dataset.rankFilter;
      
      if (state.selectedKeywordsYtm === "Admin") {
        renderAdminKeywordsView();
      } else {
        renderKeywordsView();
      }
    });
  });
  
  document.querySelector("#keywordsExportCsvButton")?.addEventListener("click", () => {
    exportKeywordsToCsv();
  });
}

// Run rank filter setup once
attachKeywordsRankFilterListeners();

// Attach event listeners for Keywords panel
document.querySelector("#keywordsRefreshButton")?.addEventListener("click", () => {
  if (state.selectedKeywordsYtm === "Admin") {
    refreshAdminKeywordRankings();
  } else {
    refreshKeywordRankings();
  }
});

document.querySelector("#addManualKeywordButton")?.addEventListener("click", () => {
  addManualKeyword();
});

document.querySelector("#manualKeywordInput")?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    addManualKeyword();
  }
});

// Target Tracker View Controllers

function formatQuarterLabel(quarterKey) {
  if (!quarterKey) return "";
  const parts = quarterKey.split("_");
  const period = parts[0] || "";
  const year = parts[1] || "";
  let monthDesc = "";
  if (period === "AMJ") monthDesc = "(Apr - Jun)";
  else if (period === "JAS") monthDesc = "(Jul - Sep)";
  else if (period === "OND") monthDesc = "(Oct - Dec)";
  else if (period === "JFM") monthDesc = "(Jan - Mar)";
  return `${period} ${year} ${monthDesc}`.trim();
}

function updateQuarterSelectOptions(allQuarters = [], activeQuarter = "") {
  const defaultQuarters = ["AMJ_2026", "JAS_2026"];
  const quarters = Array.from(new Set([...defaultQuarters, ...allQuarters])).filter(Boolean);
  
  // Update main quarterSelect
  const mainSelect = document.querySelector("#quarterSelect");
  if (mainSelect) {
    const currentVal = activeQuarter || mainSelect.value || "JAS_2026";
    mainSelect.innerHTML = quarters.map(q => `
      <option value="${escapeHtml(q)}" ${q === currentVal ? "selected" : ""}>${escapeHtml(formatQuarterLabel(q))}</option>
    `).join("");
  }

  // Update editorQuarterSelect inside Manage Targets Modal
  const editorSelect = document.querySelector("#editorQuarterSelect");
  if (editorSelect) {
    const currentEditorVal = editorActiveQuarter || activeQuarter || "JAS_2026";
    editorSelect.innerHTML = quarters.map(q => `
      <option value="${escapeHtml(q)}" ${q === currentEditorVal ? "selected" : ""}>${escapeHtml(formatQuarterLabel(q))}</option>
    `).join("");
  }

  // Update editorCopyFromSelect inside Manage Targets Modal
  const editorCopySelect = document.querySelector("#editorCopyFromSelect");
  if (editorCopySelect) {
    editorCopySelect.innerHTML = quarters.map(q => `
      <option value="${escapeHtml(q)}">${escapeHtml(formatQuarterLabel(q))}</option>
    `).join("");
  }

  // Update newQuarterCopyFrom inside Add Quarter Modal
  const newQuarterCopySelect = document.querySelector("#newQuarterCopyFrom");
  if (newQuarterCopySelect) {
    newQuarterCopySelect.innerHTML = `
      <option value="">-- Start with Empty Targets --</option>
      ${quarters.map(q => `<option value="${escapeHtml(q)}">${escapeHtml(formatQuarterLabel(q))}</option>`).join("")}
    `;
  }
}

async function loadTargets(options = {}) {
  const ytmBody = document.querySelector("#ytmTargetsTableBody");
  const seoBody = document.querySelector("#seoTargetsTableBody");
  const dateRangeEl = document.querySelector("#targetsDateRange");

  state.activeQuarter = state.activeQuarter || "JAS_2026";
  state.activeTargetSubTab = state.activeTargetSubTab || "ytm";

  // Render initial tab UI state
  updateTargetSubTabUI();

  try {
    if (ytmBody) ytmBody.innerHTML = `<tr><td colspan="8" style="text-align: center; padding: 20px; color: var(--muted);">Loading target stats...</td></tr>`;
    if (seoBody) seoBody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 20px; color: var(--muted);">Loading target stats...</td></tr>`;
    if (dateRangeEl) dateRangeEl.textContent = "Loading date range...";

    const res = await api(`/api/targets?quarter=${encodeURIComponent(state.activeQuarter)}${options.force ? "&force=1" : ""}`);
    state.targetsData = res;

    // Update quarter dropdowns dynamically
    updateQuarterSelectOptions(res.allQuarters || [], state.activeQuarter);

    // Update date range info
    if (dateRangeEl) {
      if (res.hasStarted) {
        const daysText = res.elapsedDays !== undefined && res.totalDays !== undefined
          ? ` (${res.elapsedDays} of ${res.totalDays} days elapsed, pro-rated target calculation)`
          : "";
        dateRangeEl.textContent = `Quarter performance: ${formatDateText(res.startDate)} to ${formatDateText(res.endDate)}${daysText}`;
      } else {
        dateRangeEl.textContent = `Quarter has not started yet (starts ${formatDateText(res.startDate)})`;
      }
    }

    renderTargetsTable();
  } catch (err) {
    console.error("Failed to load targets:", err);
    if (ytmBody) ytmBody.innerHTML = `<tr><td colspan="8" style="text-align: center; padding: 20px; color: var(--red);">Error: ${err.message || "Failed to load targets"}</td></tr>`;
    if (seoBody) seoBody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 20px; color: var(--red);">Error: ${err.message || "Failed to load targets"}</td></tr>`;
  }
}

function formatDateText(isoString) {
  if (!isoString) return "";
  try {
    const d = new Date(`${isoString}T00:00:00Z`);
    const day = d.getUTCDate();
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    const month = months[d.getUTCMonth()];
    const year = d.getUTCFullYear();
    
    let suffix = "th";
    if (day === 1 || day === 21 || day === 31) suffix = "st";
    else if (day === 2 || day === 22) suffix = "nd";
    else if (day === 3 || day === 23) suffix = "rd";
    
    return `${month} ${day}${suffix}, ${year}`;
  } catch {
    return isoString;
  }
}

function formatPercentText(val) {
  if (!val) return "0%";
  if (val >= 100) return `${Math.round(val)}%`;
  return `${val.toFixed(1)}%`;
}

function renderTargetsTable() {
  const ytmBody = document.querySelector("#ytmTargetsTableBody");
  const seoBody = document.querySelector("#seoTargetsTableBody");
  if (!state.targetsData) return;

  const { ytm, seo } = state.targetsData;
  const useHybrid = state.viewMetric === "hybrid";

  // Update table header names dynamically to clarify which metric is being viewed
  const ytmHeaderEl = document.querySelector("#ytmTargetsTableSection th:nth-child(4)");
  if (ytmHeaderEl) {
    ytmHeaderEl.textContent = useHybrid ? "Actual Organic (Hybrid)" : "Actual Organic (Standard)";
  }
  const seoHeaderEl = document.querySelector("#seoTargetsTableSection th:nth-child(4)");
  if (seoHeaderEl) {
    seoHeaderEl.textContent = useHybrid ? "Actual Search (Hybrid)" : "Actual Search (Standard)";
  }

  // Render YTM Targets Table
  if (ytmBody) {
    if (!ytm || !ytm.length) {
      ytmBody.innerHTML = `<tr><td colspan="8" style="text-align: center; padding: 20px; color: var(--muted);">No YTM targets configured for this quarter. Click "Manage Targets" to add.</td></tr>`;
    } else {
      ytmBody.innerHTML = ytm.map(t => {
        const actualViewsVal = useHybrid ? t.actualViews : t.actualStandardViews;
        const percentVal = useHybrid ? t.viewsPercent : t.standardViewsPercent;
        const proRataVal = useHybrid ? t.viewsProRataPercent : t.standardViewsProRataPercent;
        
        const viewsBadgeClass = getProgressBadgeClass(proRataVal);
        const subsBadgeClass = getProgressBadgeClass(t.subsProRataPercent);

        return `
          <tr style="border-bottom: 1px solid var(--line);">
            <td style="padding: 12px 8px; font-weight: 500; color: var(--text);">${escapeHtml(t.employee)}</td>
            <td style="padding: 12px 8px; color: var(--muted);">${escapeHtml(t.channelName)}</td>
            <td style="padding: 12px 8px; text-align: right; font-variant-numeric: tabular-nums;">${formatInteger(t.viewsTarget)}</td>
            <td style="padding: 12px 8px; text-align: right; font-variant-numeric: tabular-nums;">${formatInteger(actualViewsVal)}</td>
            <td style="padding: 12px 8px; text-align: right;"><span class="${viewsBadgeClass}">${formatPercentText(percentVal)} reached</span></td>
            <td style="padding: 12px 8px; text-align: right; font-variant-numeric: tabular-nums;">${formatInteger(t.subsTarget)}</td>
            <td style="padding: 12px 8px; text-align: right; font-variant-numeric: tabular-nums;">${formatInteger(t.actualSubs)}</td>
            <td style="padding: 12px 8px; text-align: right;"><span class="${subsBadgeClass}">${formatPercentText(t.subsPercent)} reached</span></td>
          </tr>
        `;
      }).join("");
    }
  }

  // Render SEO Targets Table
  if (seoBody) {
    if (!seo || !seo.length) {
      seoBody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 20px; color: var(--muted);">No SEO targets configured for this quarter. Click "Manage Targets" to add.</td></tr>`;
    } else {
      seoBody.innerHTML = seo.map(t => {
        const actualSearchViewsVal = useHybrid ? t.actualSearchViews : t.actualStandardSearchViews;
        const percentVal = useHybrid ? t.searchPercent : t.standardSearchPercent;
        const proRataVal = useHybrid ? t.searchProRataPercent : t.standardSearchProRataPercent;
        
        const searchBadgeClass = getProgressBadgeClass(proRataVal);

        return `
          <tr style="border-bottom: 1px solid var(--line);">
            <td style="padding: 12px 8px; font-weight: 500; color: var(--text);">${escapeHtml(t.employee)}</td>
            <td style="padding: 12px 8px; color: var(--muted);">${escapeHtml(t.channelName)}</td>
            <td style="padding: 12px 8px; text-align: right; font-variant-numeric: tabular-nums;">${formatInteger(t.searchViewsTarget)}</td>
            <td style="padding: 12px 8px; text-align: right; font-variant-numeric: tabular-nums;">${formatInteger(actualSearchViewsVal)}</td>
            <td style="padding: 12px 8px; text-align: right;"><span class="${searchBadgeClass}">${formatPercentText(percentVal)} reached</span></td>
          </tr>
        `;
      }).join("");
    }
  }
}

function getProgressBadgeClass(percent) {
  if (percent >= 100) return "badge-reached-good";
  if (percent >= 80) return "badge-reached-warn";
  return "badge-reached-danger";
}

function updateTargetSubTabUI() {
  const subTabYtm = document.querySelector("#subTabYtm");
  const subTabSeo = document.querySelector("#subTabSeo");
  const ytmSection = document.querySelector("#ytmTargetsTableSection");
  const seoSection = document.querySelector("#seoTargetsTableSection");

  if (state.activeTargetSubTab === "ytm") {
    subTabYtm?.classList.add("active-sub-tab");
    subTabSeo?.classList.remove("active-sub-tab");
    ytmSection?.classList.remove("is-hidden");
    seoSection?.classList.add("is-hidden");
  } else {
    subTabYtm?.classList.remove("active-sub-tab");
    subTabSeo?.classList.add("active-sub-tab");
    ytmSection?.classList.add("is-hidden");
    seoSection?.classList.remove("is-hidden");
  }
}

// Modal Target Editor Controllers

let editorActiveQuarter = "";
let tempYtmTargets = [];
let tempSeoTargets = [];

function loadEditorTargetsForQuarter(quarterKey) {
  if (state.targetsData?.rawTargets && state.targetsData.rawTargets[quarterKey]) {
    const raw = state.targetsData.rawTargets[quarterKey];
    tempYtmTargets = JSON.parse(JSON.stringify(raw.ytm || []));
    tempSeoTargets = JSON.parse(JSON.stringify(raw.seo || []));
  } else if (quarterKey === state.activeQuarter && state.targetsData) {
    tempYtmTargets = JSON.parse(JSON.stringify(state.targetsData.ytm || []));
    tempSeoTargets = JSON.parse(JSON.stringify(state.targetsData.seo || []));
  } else {
    tempYtmTargets = [];
    tempSeoTargets = [];
  }
}

function openTargetsEditor(quarterToEdit = "") {
  const dialog = document.querySelector("#targetsDialog");
  const channelSelect = document.querySelector("#targetChannelInput");
  const editorQuarterLabel = document.querySelector("#targetsEditorQuarter");

  if (!dialog) return;

  editorActiveQuarter = quarterToEdit || state.activeQuarter || "JAS_2026";
  if (editorQuarterLabel) {
    editorQuarterLabel.textContent = `Quarter Targets for ${editorActiveQuarter.replace("_", " ")}`;
  }

  // Update dropdown options
  if (state.targetsData?.allQuarters) {
    updateQuarterSelectOptions(state.targetsData.allQuarters, state.activeQuarter);
  }

  const editorSelect = document.querySelector("#editorQuarterSelect");
  if (editorSelect) {
    editorSelect.value = editorActiveQuarter;
  }

  // Populate Channel dropdown select options
  if (channelSelect && state.channels) {
    const publicChs = state.channels.filter(c => c.id !== "all-in-one");
    channelSelect.innerHTML = publicChs.map(c => `
      <option value="${escapeHtml(c.id)}">${escapeHtml(c.name)}</option>
    `).join("");
  }

  // Load targets into temporary lists
  loadEditorTargetsForQuarter(editorActiveQuarter);

  // Reset Add form inputs
  document.querySelector("#targetEmployeeInput").value = "";
  document.querySelector("#targetViewsInput").value = "";
  document.querySelector("#targetSubsInput").value = "";
  document.querySelector("#targetSearchViewsInput").value = "";

  renderEditorTargets();
  dialog.showModal();
}

function renderEditorTargets() {
  const ytmBody = document.querySelector("#editorYtmTableBody");
  const seoBody = document.querySelector("#editorSeoTableBody");

  if (ytmBody) {
    if (!tempYtmTargets.length) {
      ytmBody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 12px; color: var(--muted);">No YTM targets added yet. Use form above or Copy from another quarter.</td></tr>`;
    } else {
      ytmBody.innerHTML = tempYtmTargets.map((t, idx) => `
        <tr style="border-bottom: 1px solid var(--line);">
          <td style="padding: 8px 4px; font-weight: 500;">${escapeHtml(t.employee)}</td>
          <td style="padding: 8px 4px; color: var(--muted);">${escapeHtml(t.channelName)}</td>
          <td style="padding: 8px 4px; text-align: right;">${formatInteger(t.viewsTarget)}</td>
          <td style="padding: 8px 4px; text-align: right;">${formatInteger(t.subsTarget)}</td>
          <td style="padding: 8px 4px; text-align: center;">
            <button class="keywords-delete-btn" type="button" onclick="deleteEditorTargetRow('ytm', ${idx})">Delete</button>
          </td>
        </tr>
      `).join("");
    }
  }

  if (seoBody) {
    if (!tempSeoTargets.length) {
      seoBody.innerHTML = `<tr><td colspan="4" style="text-align: center; padding: 12px; color: var(--muted);">No SEO targets added yet. Use form above or Copy from another quarter.</td></tr>`;
    } else {
      seoBody.innerHTML = tempSeoTargets.map((t, idx) => `
        <tr style="border-bottom: 1px solid var(--line);">
          <td style="padding: 8px 4px; font-weight: 500;">${escapeHtml(t.employee)}</td>
          <td style="padding: 8px 4px; color: var(--muted);">${escapeHtml(t.channelName)}</td>
          <td style="padding: 8px 4px; text-align: right;">${formatInteger(t.searchViewsTarget)}</td>
          <td style="padding: 8px 4px; text-align: center;">
            <button class="keywords-delete-btn" type="button" onclick="deleteEditorTargetRow('seo', ${idx})">Delete</button>
          </td>
        </tr>
      `).join("");
    }
  }
}

// Expose deleteEditorTargetRow globally for inline onclick attribute
window.deleteEditorTargetRow = function(category, idx) {
  if (category === "ytm") {
    tempYtmTargets.splice(idx, 1);
  } else {
    tempSeoTargets.splice(idx, 1);
  }
  renderEditorTargets();
};

function addEditorTargetRow() {
  const employee = document.querySelector("#targetEmployeeInput").value.trim();
  const category = document.querySelector("#targetCategoryInput").value;
  const channelSelect = document.querySelector("#targetChannelInput");
  const channelId = channelSelect.value;
  const channelName = channelSelect.options[channelSelect.selectedIndex]?.text || "";

  if (!employee) {
    alert("Please enter Employee Name.");
    return;
  }
  if (!channelId) {
    alert("No channel selected.");
    return;
  }

  if (category === "ytm") {
    const viewsVal = parseInt(document.querySelector("#targetViewsInput").value, 10);
    const subsVal = parseInt(document.querySelector("#targetSubsInput").value, 10);
    if (isNaN(viewsVal) || viewsVal < 0 || isNaN(subsVal) || subsVal < 0) {
      alert("Please enter valid Views and Subscribers targets.");
      return;
    }

    tempYtmTargets.push({
      id: "t_" + Date.now() + "_" + Math.random().toString(36).substr(2, 5),
      employee,
      channelId,
      channelName,
      viewsTarget: viewsVal,
      subsTarget: subsVal
    });
  } else {
    const searchVal = parseInt(document.querySelector("#targetSearchViewsInput").value, 10);
    if (isNaN(searchVal) || searchVal < 0) {
      alert("Please enter a valid YT Search Views target.");
      return;
    }

    tempSeoTargets.push({
      id: "t_" + Date.now() + "_" + Math.random().toString(36).substr(2, 5),
      employee,
      channelId,
      channelName,
      searchViewsTarget: searchVal
    });
  }

  // Clear numeric inputs
  document.querySelector("#targetViewsInput").value = "";
  document.querySelector("#targetSubsInput").value = "";
  document.querySelector("#targetSearchViewsInput").value = "";

  renderEditorTargets();
}

async function saveEditorTargets() {
  try {
    const targetQuarter = editorActiveQuarter || state.activeQuarter;
    const body = {
      quarter: targetQuarter,
      ytm: tempYtmTargets.map(t => ({
        id: t.id,
        employee: t.employee,
        channelId: t.channelId,
        channelName: t.channelName,
        viewsTarget: t.viewsTarget,
        subsTarget: t.subsTarget
      })),
      seo: tempSeoTargets.map(t => ({
        id: t.id,
        employee: t.employee,
        channelId: t.channelId,
        channelName: t.channelName,
        searchViewsTarget: t.searchViewsTarget
      }))
    };

    const dialog = document.querySelector("#targetsDialog");
    const saveBtn = document.querySelector("#saveTargetsEditorBtn");
    if (saveBtn) saveBtn.disabled = true;

    await api("/api/targets/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });

    state.activeQuarter = targetQuarter;
    if (dialog) dialog.close();
    await loadTargets({ force: true });
  } catch (err) {
    alert("Failed to save changes: " + err.message);
  } finally {
    const saveBtn = document.querySelector("#saveTargetsEditorBtn");
    if (saveBtn) saveBtn.disabled = false;
  }
}

// Attach Event Listeners for targets panel elements
document.querySelector("#quarterSelect")?.addEventListener("change", (e) => {
  state.activeQuarter = e.target.value;
  loadTargets();
});

document.querySelector("#manageTargetsButton")?.addEventListener("click", () => {
  openTargetsEditor();
});

document.querySelector("#addQuarterButton")?.addEventListener("click", () => {
  const newQuarterDialog = document.querySelector("#newQuarterDialog");
  if (!newQuarterDialog) return;

  const yearInput = document.querySelector("#newQuarterYear");
  if (yearInput) {
    const now = new Date();
    yearInput.value = now.getFullYear();
  }

  if (state.targetsData?.allQuarters) {
    updateQuarterSelectOptions(state.targetsData.allQuarters, state.activeQuarter);
  }

  newQuarterDialog.showModal();
});

document.querySelector("#submitCreateQuarterBtn")?.addEventListener("click", async () => {
  const period = document.querySelector("#newQuarterPeriod")?.value || "OND";
  const year = parseInt(document.querySelector("#newQuarterYear")?.value, 10);
  const copyFrom = document.querySelector("#newQuarterCopyFrom")?.value || "";

  if (!year || year < 2020 || year > 2050) {
    alert("Please enter a valid year.");
    return;
  }

  const newQuarterKey = `${period}_${year}`;
  const submitBtn = document.querySelector("#submitCreateQuarterBtn");
  if (submitBtn) submitBtn.disabled = true;

  try {
    await api("/api/targets/quarter", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ quarter: newQuarterKey, copyFrom: copyFrom || undefined })
    });

    const newQuarterDialog = document.querySelector("#newQuarterDialog");
    if (newQuarterDialog) newQuarterDialog.close();

    state.activeQuarter = newQuarterKey;
    await loadTargets({ force: true });

    // Open target editor immediately for user convenience
    openTargetsEditor(newQuarterKey);
  } catch (err) {
    alert("Failed to create quarter: " + err.message);
  } finally {
    if (submitBtn) submitBtn.disabled = false;
  }
});

document.querySelector("#editorQuarterSelect")?.addEventListener("change", (e) => {
  editorActiveQuarter = e.target.value;
  const editorQuarterLabel = document.querySelector("#targetsEditorQuarter");
  if (editorQuarterLabel) {
    editorQuarterLabel.textContent = `Quarter Targets for ${editorActiveQuarter.replace("_", " ")}`;
  }
  loadEditorTargetsForQuarter(editorActiveQuarter);
  renderEditorTargets();
});

document.querySelector("#editorCopyFromBtn")?.addEventListener("click", () => {
  const copyFromSelect = document.querySelector("#editorCopyFromSelect");
  const copyFromQuarter = copyFromSelect?.value;
  if (!copyFromQuarter) return;

  const raw = state.targetsData?.rawTargets?.[copyFromQuarter];
  if (!raw || (!raw.ytm?.length && !raw.seo?.length)) {
    alert(`No targets found in ${formatQuarterLabel(copyFromQuarter)} to copy.`);
    return;
  }

  if (confirm(`Copy targets from ${formatQuarterLabel(copyFromQuarter)} to ${formatQuarterLabel(editorActiveQuarter)}? This will replace the unsaved targets in this editor.`)) {
    tempYtmTargets = (raw.ytm || []).map(t => ({
      ...t,
      id: "t_" + Date.now() + "_" + Math.random().toString(36).substr(2, 5)
    }));
    tempSeoTargets = (raw.seo || []).map(t => ({
      ...t,
      id: "t_" + Date.now() + "_" + Math.random().toString(36).substr(2, 5)
    }));
    renderEditorTargets();
  }
});

document.querySelector("#subTabYtm")?.addEventListener("click", () => {
  state.activeTargetSubTab = "ytm";
  updateTargetSubTabUI();
});

document.querySelector("#subTabSeo")?.addEventListener("click", () => {
  state.activeTargetSubTab = "seo";
  updateTargetSubTabUI();
});

document.querySelector("#targetCategoryInput")?.addEventListener("change", (e) => {
  const ytmFields = document.querySelector("#targetYtmFields");
  const seoFields = document.querySelector("#targetSeoFields");
  if (e.target.value === "ytm") {
    ytmFields?.classList.remove("is-hidden");
    seoFields?.classList.add("is-hidden");
  } else {
    ytmFields?.classList.add("is-hidden");
    seoFields?.classList.remove("is-hidden");
  }
});

document.querySelector("#addNewTargetRowBtn")?.addEventListener("click", () => {
  addEditorTargetRow();
});

document.querySelector("#saveTargetsEditorBtn")?.addEventListener("click", () => {
  saveEditorTargets();
});

// ==========================================
// Admin 6-Month MoM Report & Export Controller
// ==========================================

state.adminMonthlyReport = null;
state.adminMonthlyReportKey = "";
state.adminActiveTab = "views"; // "views" | "subs" | "combined"
state.adminChannelSearch = "";
state.adminRange = "1y"; // "1y" | "2y" | "3m" | "custom"
state.adminStartDate = "2025-08-01";
state.adminEndDate = "2026-08-31";

async function loadAdminMonthlyReport(options = {}) {
  const container = document.querySelector("#adminMonthlyTableContainer");
  if (!container) return;

  const activeRange = state.adminRange || "1y";
  const rangeKey = activeRange === "custom"
    ? `custom:${state.adminStartDate || ""}:${state.adminEndDate || ""}`
    : activeRange;

  if (state.adminMonthlyReport && state.adminMonthlyReportKey === rangeKey && !options.force) {
    renderAdminMonthlyReport();
    return;
  }

  const rangeLabelDesc = activeRange === "2y"
    ? "2-year"
    : activeRange === "3m"
      ? "last 3 months"
      : activeRange === "custom"
        ? `${state.adminStartDate || ""} to ${state.adminEndDate || ""}`
        : "1-year";

  container.innerHTML = `
    <div style="padding: 48px; text-align: center; color: var(--muted); display: flex; flex-direction: column; align-items: center; gap: 12px;">
      <div style="width: 32px; height: 32px; border: 3px solid var(--line); border-top-color: var(--blue, #3c6ee8); border-radius: 50%; animation: spin 0.8s linear infinite;"></div>
      <span style="font-size: 14px; font-weight: 500;">Aggregating ${escapeHtml(rangeLabelDesc)} analytics across all attached channels...</span>
      <span style="font-size: 12px; color: var(--muted);">Calculating Organic Views, Hybrid Views, and Net Subscribers...</span>
    </div>
  `;

  try {
    const params = new URLSearchParams();
    params.set("range", activeRange);
    if (activeRange === "custom") {
      if (state.adminStartDate) params.set("startDate", state.adminStartDate);
      if (state.adminEndDate) params.set("endDate", state.adminEndDate);
    }
    if (options.force) params.set("force", "1");

    const data = await api(`/api/admin/monthly-report?${params.toString()}`);
    state.adminMonthlyReport = data;
    state.adminMonthlyReportKey = rangeKey;
    renderAdminMonthlyReport();
  } catch (err) {
    container.innerHTML = `
      <div style="padding: 32px; text-align: center; color: #dc2626; font-size: 14px;">
        <strong>Failed to load report:</strong> ${escapeHtml(err.message || String(err))}
      </div>
    `;
  }
}

function renderAdminMonthlyReport() {
  const data = state.adminMonthlyReport;
  if (!data) return;

  renderAdminMonthlyKPIs(data);
  renderAdminMonthlyTable();
}

function renderAdminMonthlyKPIs(data) {
  const useHybrid = state.viewMetric === "hybrid";
  const totals = data.networkTotals || {};
  const totalViewsVal = useHybrid ? (totals.totalOrganicHybridViews || 0) : (totals.totalOrganicViews || 0);
  const totalSubsVal = totals.totalSubscribers || 0;

  // Dynamic header title and KPI labels
  const headerTitleEl = document.querySelector("#adminMomHeaderTitle");
  if (headerTitleEl) {
    headerTitleEl.textContent = `MoM Progress & Export (${data.rangeLabel || ""})`;
  }

  const rangeShort = data.rangeShortLabel || data.rangeLabel || "";
  const totalViewsLabelEl = document.querySelector("#adminTotalViewsCardLabel");
  if (totalViewsLabelEl) {
    totalViewsLabelEl.textContent = `Total Network Organic Views (${rangeShort})`;
  }

  const totalSubsLabelEl = document.querySelector("#adminTotalSubsCardLabel");
  if (totalSubsLabelEl) {
    totalSubsLabelEl.textContent = `Total Network Subscribers (${rangeShort})`;
  }

  const totalViewsEl = document.querySelector("#admin6mTotalViews");
  if (totalViewsEl) totalViewsEl.textContent = totalViewsVal.toLocaleString();

  const latestM = data.latestCompletedMonth;
  const priorM = data.priorMonth;
  const momGrowthVal = useHybrid ? totals.momHybridViewsGrowth : totals.momViewsGrowth;

  const viewsDetailEl = document.querySelector("#admin6mViewsDetail");
  if (viewsDetailEl && latestM) {
    const latestViews = totals.months?.[latestM.key] ? (useHybrid ? totals.months[latestM.key].organicHybridViews : totals.months[latestM.key].organicViews) : 0;
    viewsDetailEl.textContent = `${latestM.label}: ${latestViews.toLocaleString()} (${momGrowthVal >= 0 ? "+" : ""}${momGrowthVal}% MoM)`;
  }

  const totalSubsEl = document.querySelector("#admin6mTotalSubs");
  if (totalSubsEl) totalSubsEl.textContent = (totalSubsVal >= 0 ? "+" : "") + totalSubsVal.toLocaleString();

  const subsDetailEl = document.querySelector("#admin6mSubsDetail");
  if (subsDetailEl && latestM) {
    const latestSubs = totals.months?.[latestM.key]?.subscribers || 0;
    const momSubsVal = totals.momSubsGrowth || 0;
    subsDetailEl.textContent = `${latestM.label}: ${(latestSubs >= 0 ? "+" : "")}${latestSubs.toLocaleString()} (${momSubsVal >= 0 ? "+" : ""}${momSubsVal}% MoM)`;
  }

  let topGrowingCh = null;
  let maxGrowth = -Infinity;
  for (const ch of (data.channels || [])) {
    const growth = useHybrid ? ch.momHybridViewsGrowth : ch.momViewsGrowth;
    const views = ch.months?.[latestM?.key] ? (useHybrid ? ch.months[latestM.key].organicHybridViews : ch.months[latestM.key].organicViews) : 0;
    if (views >= 5000 && growth > maxGrowth) {
      maxGrowth = growth;
      topGrowingCh = ch;
    }
  }

  const topChannelEl = document.querySelector("#admin6mTopChannel");
  const topChannelDetailEl = document.querySelector("#admin6mTopChannelDetail");
  if (topChannelEl) {
    if (topGrowingCh) {
      topChannelEl.textContent = topGrowingCh.name;
      if (topChannelDetailEl) topChannelDetailEl.textContent = `+${maxGrowth}% MoM growth in ${latestM?.label}`;
    } else {
      topChannelEl.textContent = "-";
      if (topChannelDetailEl) topChannelDetailEl.textContent = "No data yet";
    }
  }

  const momTrendEl = document.querySelector("#admin6mMomTrend");
  const momTrendDetailEl = document.querySelector("#admin6mMomTrendDetail");
  const momTrendLabelEl = document.querySelector("#adminMomTrendCardLabel");
  if (momTrendLabelEl) {
    momTrendLabelEl.textContent = latestM && priorM
      ? `Network MoM Views Trend (${latestM.shortLabel} vs ${priorM.shortLabel})`
      : "Network MoM Views Trend";
  }

  if (momTrendEl) {
    const isUp = momGrowthVal >= 0;
    momTrendEl.textContent = `${isUp ? "+" : ""}${momGrowthVal}%`;
    momTrendEl.style.color = isUp ? "#16a34a" : "#dc2626";
    if (momTrendDetailEl) {
      if (latestM && priorM) {
        momTrendDetailEl.textContent = `${latestM.label} vs ${priorM.label}`;
      } else {
        momTrendDetailEl.textContent = "-";
      }
    }
  }

  const badge = document.querySelector("#adminChannelCountBadge");
  if (badge) {
    badge.textContent = `${data.channels?.length || 0} channels connected`;
  }
}

function renderAdminMonthlyTable() {
  const data = state.adminMonthlyReport;
  const container = document.querySelector("#adminMonthlyTableContainer");
  if (!data || !container) return;

  const useHybrid = state.viewMetric === "hybrid";
  const activeTab = state.adminActiveTab || "views";
  const search = (state.adminChannelSearch || "").toLowerCase().trim();

  const filteredChannels = (data.channels || []).filter(ch => {
    if (!search) return true;
    return ch.name.toLowerCase().includes(search) || ch.id.toLowerCase().includes(search);
  });

  const months = data.months || [];
  const network = data.networkTotals || {};

  let theadHtml = "";
  if (activeTab === "views") {
    theadHtml = `
      <tr style="border-bottom: 2px solid var(--line); background: var(--bg-alt, #f8fafc); color: var(--muted); font-size: 12px; text-transform: uppercase; font-weight: 600;">
        <th style="padding: 12px 14px; text-align: left; position: sticky; left: 0; background: var(--bg-alt, #f8fafc); z-index: 2;">Channel Name</th>
        ${months.map(m => `<th style="padding: 12px 10px; text-align: right; white-space: nowrap;">${escapeHtml(m.label)}</th>`).join("")}
        <th style="padding: 12px 12px; text-align: right; white-space: nowrap;">Total Views</th>
        <th style="padding: 12px 14px; text-align: right; white-space: nowrap;">MoM Growth</th>
      </tr>
    `;
  } else if (activeTab === "subs") {
    theadHtml = `
      <tr style="border-bottom: 2px solid var(--line); background: var(--bg-alt, #f8fafc); color: var(--muted); font-size: 12px; text-transform: uppercase; font-weight: 600;">
        <th style="padding: 12px 14px; text-align: left; position: sticky; left: 0; background: var(--bg-alt, #f8fafc); z-index: 2;">Channel Name</th>
        ${months.map(m => `<th style="padding: 12px 10px; text-align: right; white-space: nowrap;">${escapeHtml(m.label)}</th>`).join("")}
        <th style="padding: 12px 12px; text-align: right; white-space: nowrap;">Total Net Subs</th>
        <th style="padding: 12px 14px; text-align: right; white-space: nowrap;">MoM Growth</th>
      </tr>
    `;
  } else {
    theadHtml = `
      <tr style="border-bottom: 1px solid var(--line); background: var(--bg-alt, #f8fafc); color: var(--muted); font-size: 11px; text-transform: uppercase; font-weight: 600;">
        <th rowspan="2" style="padding: 12px 14px; text-align: left; vertical-align: middle; position: sticky; left: 0; background: var(--bg-alt, #f8fafc); z-index: 2; border-right: 1px solid var(--line);">Channel Name</th>
        ${months.map(m => `<th colspan="2" style="padding: 8px 10px; text-align: center; border-right: 1px solid var(--line);">${escapeHtml(m.label)}</th>`).join("")}
        <th colspan="2" style="padding: 8px 12px; text-align: center;">Total (${escapeHtml(data.rangeShortLabel || data.rangeLabel || "Period")})</th>
      </tr>
      <tr style="border-bottom: 2px solid var(--line); background: var(--bg-alt, #f8fafc); color: var(--muted); font-size: 11px; text-transform: uppercase; font-weight: 600;">
        ${months.map(() => `<th style="padding: 6px 8px; text-align: right; font-size: 10px;">Views</th><th style="padding: 6px 8px; text-align: right; font-size: 10px; border-right: 1px solid var(--line);">Subs</th>`).join("")}
        <th style="padding: 6px 8px; text-align: right; font-size: 10px;">Views</th>
        <th style="padding: 6px 8px; text-align: right; font-size: 10px;">Subs</th>
      </tr>
    `;
  }

  let networkRowHtml = "";
  if (activeTab === "views") {
    const totalVal = useHybrid ? network.totalOrganicHybridViews : network.totalOrganicViews;
    const momVal = useHybrid ? network.momHybridViewsGrowth : network.momViewsGrowth;
    const momBadgeClass = momVal >= 0 ? "badge-reached-good" : "badge-reached-danger";
    networkRowHtml = `
      <tr style="background: rgba(99, 102, 241, 0.08); font-weight: 700; border-bottom: 2px solid var(--line);">
        <td style="padding: 12px 14px; position: sticky; left: 0; background: #e0e7ff; z-index: 1; color: #3730a3;">${network.name || "🌐 Network Total"}</td>
        ${months.map(m => {
          const val = network.months?.[m.key] ? (useHybrid ? network.months[m.key].organicHybridViews : network.months[m.key].organicViews) : 0;
          return `<td style="padding: 12px 10px; text-align: right; font-variant-numeric: tabular-nums;">${val.toLocaleString()}</td>`;
        }).join("")}
        <td style="padding: 12px 12px; text-align: right; font-variant-numeric: tabular-nums; color: #3730a3;">${totalVal.toLocaleString()}</td>
        <td style="padding: 12px 14px; text-align: right;"><span class="${momBadgeClass}">${momVal >= 0 ? "+" : ""}${momVal}%</span></td>
      </tr>
    `;
  } else if (activeTab === "subs") {
    const totalVal = network.totalSubscribers || 0;
    const momVal = network.momSubsGrowth || 0;
    const momBadgeClass = momVal >= 0 ? "badge-reached-good" : "badge-reached-danger";
    networkRowHtml = `
      <tr style="background: rgba(99, 102, 241, 0.08); font-weight: 700; border-bottom: 2px solid var(--line);">
        <td style="padding: 12px 14px; position: sticky; left: 0; background: #e0e7ff; z-index: 1; color: #3730a3;">${network.name || "🌐 Network Total"}</td>
        ${months.map(m => {
          const val = network.months?.[m.key]?.subscribers || 0;
          return `<td style="padding: 12px 10px; text-align: right; font-variant-numeric: tabular-nums;">${(val >= 0 ? "+" : "") + val.toLocaleString()}</td>`;
        }).join("")}
        <td style="padding: 12px 12px; text-align: right; font-variant-numeric: tabular-nums; color: #3730a3;">${(totalVal >= 0 ? "+" : "") + totalVal.toLocaleString()}</td>
        <td style="padding: 12px 14px; text-align: right;"><span class="${momBadgeClass}">${momVal >= 0 ? "+" : ""}${momVal}%</span></td>
      </tr>
    `;
  } else {
    const totalViewsVal = useHybrid ? network.totalOrganicHybridViews : network.totalOrganicViews;
    const totalSubsVal = network.totalSubscribers || 0;
    networkRowHtml = `
      <tr style="background: rgba(99, 102, 241, 0.08); font-weight: 700; border-bottom: 2px solid var(--line);">
        <td style="padding: 10px 14px; position: sticky; left: 0; background: #e0e7ff; z-index: 1; color: #3730a3; border-right: 1px solid var(--line);">${network.name || "🌐 Network Total"}</td>
        ${months.map(m => {
          const v = network.months?.[m.key] ? (useHybrid ? network.months[m.key].organicHybridViews : network.months[m.key].organicViews) : 0;
          const s = network.months?.[m.key]?.subscribers || 0;
          return `<td style="padding: 10px 8px; text-align: right; font-variant-numeric: tabular-nums;">${v.toLocaleString()}</td><td style="padding: 10px 8px; text-align: right; font-variant-numeric: tabular-nums; border-right: 1px solid var(--line);">${(s >= 0 ? "+" : "") + s.toLocaleString()}</td>`;
        }).join("")}
        <td style="padding: 10px 8px; text-align: right; font-variant-numeric: tabular-nums; color: #3730a3;">${totalViewsVal.toLocaleString()}</td>
        <td style="padding: 10px 8px; text-align: right; font-variant-numeric: tabular-nums; color: #3730a3;">${(totalSubsVal >= 0 ? "+" : "") + totalSubsVal.toLocaleString()}</td>
      </tr>
    `;
  }

  let bodyRowsHtml = "";
  if (!filteredChannels.length) {
    const colSpan = activeTab === "combined" ? (months.length * 2 + 3) : (months.length + 3);
    bodyRowsHtml = `<tr><td colspan="${colSpan}" style="padding: 30px; text-align: center; color: var(--muted);">No matching channels found.</td></tr>`;
  } else {
    bodyRowsHtml = filteredChannels.map(ch => {
      if (activeTab === "views") {
        const totalVal = useHybrid ? ch.totalOrganicHybridViews : ch.totalOrganicViews;
        const momVal = useHybrid ? ch.momHybridViewsGrowth : ch.momViewsGrowth;
        const momBadgeClass = momVal >= 0 ? "badge-reached-good" : "badge-reached-danger";
        return `
          <tr style="border-bottom: 1px solid var(--line);">
            <td style="padding: 10px 14px; font-weight: 500; color: var(--ink); position: sticky; left: 0; background: var(--surface); z-index: 1; white-space: nowrap;">${escapeHtml(ch.name)}</td>
            ${months.map(m => {
              const val = ch.months?.[m.key] ? (useHybrid ? ch.months[m.key].organicHybridViews : ch.months[m.key].organicViews) : 0;
              return `<td style="padding: 10px 10px; text-align: right; font-variant-numeric: tabular-nums;">${val.toLocaleString()}</td>`;
            }).join("")}
            <td style="padding: 10px 12px; text-align: right; font-weight: 600; font-variant-numeric: tabular-nums;">${totalVal.toLocaleString()}</td>
            <td style="padding: 10px 14px; text-align: right;"><span class="${momBadgeClass}">${momVal >= 0 ? "+" : ""}${momVal}%</span></td>
          </tr>
        `;
      } else if (activeTab === "subs") {
        const totalVal = ch.totalSubscribers || 0;
        const momVal = ch.momSubsGrowth || 0;
        const momBadgeClass = momVal >= 0 ? "badge-reached-good" : "badge-reached-danger";
        return `
          <tr style="border-bottom: 1px solid var(--line);">
            <td style="padding: 10px 14px; font-weight: 500; color: var(--ink); position: sticky; left: 0; background: var(--surface); z-index: 1; white-space: nowrap;">${escapeHtml(ch.name)}</td>
            ${months.map(m => {
              const val = ch.months?.[m.key]?.subscribers || 0;
              return `<td style="padding: 10px 10px; text-align: right; font-variant-numeric: tabular-nums;">${(val >= 0 ? "+" : "") + val.toLocaleString()}</td>`;
            }).join("")}
            <td style="padding: 10px 12px; text-align: right; font-weight: 600; font-variant-numeric: tabular-nums;">${(totalVal >= 0 ? "+" : "") + totalVal.toLocaleString()}</td>
            <td style="padding: 10px 14px; text-align: right;"><span class="${momBadgeClass}">${momVal >= 0 ? "+" : ""}${momVal}%</span></td>
          </tr>
        `;
      } else {
        const totalViewsVal = useHybrid ? ch.totalOrganicHybridViews : ch.totalOrganicViews;
        const totalSubsVal = ch.totalSubscribers || 0;
        return `
          <tr style="border-bottom: 1px solid var(--line);">
            <td style="padding: 10px 14px; font-weight: 500; color: var(--ink); position: sticky; left: 0; background: var(--surface); z-index: 1; white-space: nowrap; border-right: 1px solid var(--line);">${escapeHtml(ch.name)}</td>
            ${months.map(m => {
              const v = ch.months?.[m.key] ? (useHybrid ? ch.months[m.key].organicHybridViews : ch.months[m.key].organicViews) : 0;
              const s = ch.months?.[m.key]?.subscribers || 0;
              return `<td style="padding: 10px 8px; text-align: right; font-variant-numeric: tabular-nums;">${v.toLocaleString()}</td><td style="padding: 10px 8px; text-align: right; font-variant-numeric: tabular-nums; border-right: 1px solid var(--line);">${(s >= 0 ? "+" : "") + s.toLocaleString()}</td>`;
            }).join("")}
            <td style="padding: 10px 8px; text-align: right; font-weight: 600; font-variant-numeric: tabular-nums;">${totalViewsVal.toLocaleString()}</td>
            <td style="padding: 10px 8px; text-align: right; font-weight: 600; font-variant-numeric: tabular-nums;">${(totalSubsVal >= 0 ? "+" : "") + totalSubsVal.toLocaleString()}</td>
          </tr>
        `;
      }
    }).join("");
  }

  container.innerHTML = `
    <table class="data-table" style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px;">
      <thead>${theadHtml}</thead>
      <tbody>
        ${networkRowHtml}
        ${bodyRowsHtml}
      </tbody>
    </table>
  `;
}

function updateAdminSubTabsUI() {
  const tabs = [
    { id: "#adminSubTabViews", name: "views" },
    { id: "#adminSubTabSubs", name: "subs" },
    { id: "#adminSubTabCombined", name: "combined" },
  ];
  for (const t of tabs) {
    const el = document.querySelector(t.id);
    if (el) {
      if (state.adminActiveTab === t.name) {
        el.classList.add("active-sub-tab");
      } else {
        el.classList.remove("active-sub-tab");
      }
    }
  }
}

function exportAdminMonthlyCsv() {
  const data = state.adminMonthlyReport;
  if (!data) return;

  const useHybrid = state.viewMetric === "hybrid";
  const activeTab = state.adminActiveTab || "views";
  const months = data.months || [];
  const network = data.networkTotals || {};
  const channels = data.channels || [];

  let csvRows = [];

  if (activeTab === "views") {
    const headers = ["Channel Name", ...months.map(m => m.label), "Total Views", "MoM Growth %"];
    csvRows.push(headers);

    const netTotal = useHybrid ? network.totalOrganicHybridViews : network.totalOrganicViews;
    const netGrowth = useHybrid ? network.momHybridViewsGrowth : network.momViewsGrowth;
    csvRows.push([
      "Network Total (All Channels)",
      ...months.map(m => network.months?.[m.key] ? (useHybrid ? network.months[m.key].organicHybridViews : network.months[m.key].organicViews) : 0),
      netTotal,
      `${netGrowth}%`
    ]);

    for (const ch of channels) {
      const chTotal = useHybrid ? ch.totalOrganicHybridViews : ch.totalOrganicViews;
      const chGrowth = useHybrid ? ch.momHybridViewsGrowth : ch.momViewsGrowth;
      csvRows.push([
        ch.name,
        ...months.map(m => ch.months?.[m.key] ? (useHybrid ? ch.months[m.key].organicHybridViews : ch.months[m.key].organicViews) : 0),
        chTotal,
        `${chGrowth}%`
      ]);
    }
  } else if (activeTab === "subs") {
    const headers = ["Channel Name", ...months.map(m => m.label), "Total Net Subs", "MoM Growth %"];
    csvRows.push(headers);

    const netTotal = network.totalSubscribers || 0;
    const netGrowth = network.momSubsGrowth || 0;
    csvRows.push([
      "Network Total (All Channels)",
      ...months.map(m => network.months?.[m.key]?.subscribers || 0),
      netTotal,
      `${netGrowth}%`
    ]);

    for (const ch of channels) {
      const chTotal = ch.totalSubscribers || 0;
      const chGrowth = ch.momSubsGrowth || 0;
      csvRows.push([
        ch.name,
        ...months.map(m => ch.months?.[m.key]?.subscribers || 0),
        chTotal,
        `${chGrowth}%`
      ]);
    }
  } else {
    const headers = ["Channel Name"];
    for (const m of months) {
      headers.push(`${m.label} Views`, `${m.label} Subs`);
    }
    headers.push(`Total Views (${data.rangeLabel || "Total"})`, `Total Net Subs (${data.rangeLabel || "Total"})`);
    csvRows.push(headers);

    const netRow = ["Network Total (All Channels)"];
    for (const m of months) {
      const v = network.months?.[m.key] ? (useHybrid ? network.months[m.key].organicHybridViews : network.months[m.key].organicViews) : 0;
      const s = network.months?.[m.key]?.subscribers || 0;
      netRow.push(v, s);
    }
    netRow.push(useHybrid ? network.totalOrganicHybridViews : network.totalOrganicViews, network.totalSubscribers || 0);
    csvRows.push(netRow);

    for (const ch of channels) {
      const row = [ch.name];
      for (const m of months) {
        const v = ch.months?.[m.key] ? (useHybrid ? ch.months[m.key].organicHybridViews : ch.months[m.key].organicViews) : 0;
        const s = ch.months?.[m.key]?.subscribers || 0;
        row.push(v, s);
      }
      row.push(useHybrid ? ch.totalOrganicHybridViews : ch.totalOrganicViews, ch.totalSubscribers || 0);
      csvRows.push(row);
    }
  }

  const csvContent = csvRows.map(row => row.map(cell => {
    const val = cell === null || cell === undefined ? "" : String(cell);
    return `"${val.replace(/"/g, '""')}"`;
  }).join(",")).join("\n");

  const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  const periodSlug = (data.rangeLabel || state.adminRange || "report").toLowerCase().replace(/[^a-z0-9]+/g, "_");
  a.download = `testbook_mom_export_${activeTab}_${periodSlug}_${new Date().toISOString().slice(0, 10)}.csv`;
  a.style.visibility = "hidden";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

async function copyAdminMonthlyForSheets(btn) {
  const data = state.adminMonthlyReport;
  if (!data) return;

  const useHybrid = state.viewMetric === "hybrid";
  const activeTab = state.adminActiveTab || "views";
  const months = data.months || [];
  const network = data.networkTotals || {};
  const channels = data.channels || [];

  let rows = [];

  if (activeTab === "views") {
    rows.push(["Channel Name", ...months.map(m => m.label), "Total Views", "MoM Growth %"]);
    const netTotal = useHybrid ? network.totalOrganicHybridViews : network.totalOrganicViews;
    const netGrowth = useHybrid ? network.momHybridViewsGrowth : network.momViewsGrowth;
    rows.push([
      "Network Total (All Channels)",
      ...months.map(m => network.months?.[m.key] ? (useHybrid ? network.months[m.key].organicHybridViews : network.months[m.key].organicViews) : 0),
      netTotal,
      `${netGrowth}%`
    ]);
    for (const ch of channels) {
      const chTotal = useHybrid ? ch.totalOrganicHybridViews : ch.totalOrganicViews;
      const chGrowth = useHybrid ? ch.momHybridViewsGrowth : ch.momViewsGrowth;
      rows.push([
        ch.name,
        ...months.map(m => ch.months?.[m.key] ? (useHybrid ? ch.months[m.key].organicHybridViews : ch.months[m.key].organicViews) : 0),
        chTotal,
        `${chGrowth}%`
      ]);
    }
  } else if (activeTab === "subs") {
    rows.push(["Channel Name", ...months.map(m => m.label), "Total Net Subs", "MoM Growth %"]);
    const netTotal = network.totalSubscribers || 0;
    const netGrowth = network.momSubsGrowth || 0;
    rows.push([
      "Network Total (All Channels)",
      ...months.map(m => network.months?.[m.key]?.subscribers || 0),
      netTotal,
      `${netGrowth}%`
    ]);
    for (const ch of channels) {
      const chTotal = ch.totalSubscribers || 0;
      const chGrowth = ch.momSubsGrowth || 0;
      rows.push([
        ch.name,
        ...months.map(m => ch.months?.[m.key]?.subscribers || 0),
        chTotal,
        `${chGrowth}%`
      ]);
    }
  } else {
    const headers = ["Channel Name"];
    for (const m of months) {
      headers.push(`${m.label} Views`, `${m.label} Subs`);
    }
    headers.push(`Total Views (${data.rangeLabel || "Total"})`, `Total Net Subs (${data.rangeLabel || "Total"})`);
    rows.push(headers);

    const netRow = ["Network Total (All Channels)"];
    for (const m of months) {
      const v = network.months?.[m.key] ? (useHybrid ? network.months[m.key].organicHybridViews : network.months[m.key].organicViews) : 0;
      const s = network.months?.[m.key]?.subscribers || 0;
      netRow.push(v, s);
    }
    netRow.push(useHybrid ? network.totalOrganicHybridViews : network.totalOrganicViews, network.totalSubscribers || 0);
    rows.push(netRow);

    for (const ch of channels) {
      const row = [ch.name];
      for (const m of months) {
        const v = ch.months?.[m.key] ? (useHybrid ? ch.months[m.key].organicHybridViews : ch.months[m.key].organicViews) : 0;
        const s = ch.months?.[m.key]?.subscribers || 0;
        row.push(v, s);
      }
      row.push(useHybrid ? ch.totalOrganicHybridViews : ch.totalOrganicViews, ch.totalSubscribers || 0);
      rows.push(row);
    }
  }

  const tsvContent = rows.map(row => row.map(cell => {
    const val = cell === null || cell === undefined ? "" : String(cell);
    return val.replace(/\t/g, " ").replace(/\r?\n/g, " ");
  }).join("\t")).join("\n");

  try {
    await navigator.clipboard.writeText(tsvContent);
    const orig = btn.innerHTML;
    btn.innerHTML = `<span>✅</span> Copied TSV for Sheets!`;
    setTimeout(() => {
      btn.innerHTML = orig;
    }, 1800);
  } catch (err) {
    alert("Could not copy to clipboard: " + err.message);
  }
}

// Event Listeners for Admin MoM Monthly Report
document.querySelectorAll("[data-admin-range]").forEach((btn) => {
  btn.addEventListener("click", () => {
    const selectedRange = btn.dataset.adminRange;
    document.querySelectorAll("[data-admin-range]").forEach((b) => b.classList.toggle("active", b === btn));
    const customControls = document.querySelector("#adminCustomDateControls");
    if (selectedRange === "custom") {
      if (customControls) customControls.style.display = "flex";
      return;
    } else {
      if (customControls) customControls.style.display = "none";
    }
    state.adminRange = selectedRange;
    loadAdminMonthlyReport();
  });
});

document.querySelector("#adminApplyCustomDateBtn")?.addEventListener("click", () => {
  const startInput = document.querySelector("#adminCustomStartDate");
  const endInput = document.querySelector("#adminCustomEndDate");
  const startDate = startInput?.value;
  const endDate = endInput?.value;
  if (!startDate || !endDate) {
    alert("Please select both From and To dates.");
    return;
  }
  if (startDate > endDate) {
    alert("From date cannot be after To date.");
    return;
  }
  state.adminRange = "custom";
  state.adminStartDate = startDate;
  state.adminEndDate = endDate;
  loadAdminMonthlyReport();
});

document.querySelector("#adminSubTabViews")?.addEventListener("click", () => {
  state.adminActiveTab = "views";
  updateAdminSubTabsUI();
  renderAdminMonthlyTable();
});

document.querySelector("#adminSubTabSubs")?.addEventListener("click", () => {
  state.adminActiveTab = "subs";
  updateAdminSubTabsUI();
  renderAdminMonthlyTable();
});

document.querySelector("#adminSubTabCombined")?.addEventListener("click", () => {
  state.adminActiveTab = "combined";
  updateAdminSubTabsUI();
  renderAdminMonthlyTable();
});

document.querySelector("#adminChannelSearchInput")?.addEventListener("input", (e) => {
  state.adminChannelSearch = e.target.value;
  renderAdminMonthlyTable();
});

document.querySelector("#adminRefresh6mBtn")?.addEventListener("click", () => {
  loadAdminMonthlyReport({ force: true });
});

document.querySelector("#adminExportCsvBtn")?.addEventListener("click", () => {
  exportAdminMonthlyCsv();
});

document.querySelector("#adminCopySheetsBtn")?.addEventListener("click", (e) => {
  copyAdminMonthlyForSheets(e.target.closest("button") || e.target);
});

// Admin Cross-Channel Keywords Controller
async function loadAdminKeywords(options = {}) {
  const container = document.querySelector("#adminKeywordsTableContainer");
  if (!container) return;

  try {
    if (!options.force && !state.adminKeywordRankings) {
      container.innerHTML = emptyCard("Loading admin rankings...");
    }
    
    if (options.force || !state.adminKeywordRankings) {
      const data = await api("/api/admin/keywords/rankings");
      state.adminKeywordRankings = data;
    }
    
    renderAdminKeywordsView();
  } catch (err) {
    console.error("Failed to load admin keywords:", err);
    container.innerHTML = emptyCard("Error: " + err.message);
  }
}

function renderAdminKeywordsView() {
  const data = state.adminKeywordRankings || { rankings: [], lastUpdated: null };
  const lastUpdatedEl = document.querySelector("#keywordsLastUpdated");
  if (lastUpdatedEl) {
    if (data.lastUpdated) {
      lastUpdatedEl.textContent = "Last scanned: " + new Date(data.lastUpdated).toLocaleString();
    } else {
      lastUpdatedEl.textContent = "Not scanned yet";
    }
  }

  const container = document.querySelector("#adminKeywordsTableContainer");
  if (!container) return;

  const filterVal = state.keywordsRankFilter || "all";
  const list = (data.rankings || []).filter(r => matchRankFilter(r.currentRank, filterVal));
  
  if (!list.length) {
    container.innerHTML = emptyCard("No admin keywords match the current filter. Enter a keyword above to start tracking.");
    return;
  }

  container.innerHTML = `
    <div class="keywords-table">
      <div class="keywords-row keywords-head manual-row">
        <span>Rank & Trend</span>
        <span>Keyword</span>
        <span>Winning Channel</span>
        <span>Best Ranking Video</span>
        <span>Search Page</span>
        <span>Action</span>
      </div>
      ${list.map(row => {
        const trendHtml = getTrendBadgeHtml(row.currentRank, row.previousRank);
        const searchUrl = "https://www.youtube.com/results?search_query=" + encodeURIComponent(row.keyword);
        
        let videoHtml = `<span style="color: var(--muted); font-size: 12px;">Not in Top 50</span>`;
        if (row.videoId) {
          const videoUrl = "https://www.youtube.com/watch?v=" + row.videoId;
          videoHtml = `
            <div class="keywords-video-cell">
              <strong title="${escapeHtml(row.videoTitle)}">${escapeHtml(truncateVideoTitle(row.videoTitle))}</strong>
              <a href="${videoUrl}" target="_blank" rel="noreferrer" style="font-size: 11px; color: var(--accent); text-decoration: none;">Watch video</a>
            </div>
          `;
        }

        const isQuota = row.currentRank === "quota_exceeded";
        const rankText = isQuota ? "Quota Reached" : (row.currentRank ? `#${row.currentRank}` : "50+");
        const rankFontSize = isQuota ? "11px" : "14px";
        
        const channelNameText = row.channelName ? escapeHtml(row.channelName) : `<span style="color: var(--muted);">-</span>`;

        return `
          <div class="keywords-row manual-row">
            <div class="rank-badge-container rank-badge-clickable" style="cursor: pointer;" data-keyword="${escapeHtml(row.keyword)}" data-history-json="${escapeHtml(JSON.stringify(row.history || []))}">
              <strong style="font-size: ${rankFontSize}; white-space: nowrap;">${rankText}</strong>
              ${trendHtml}
            </div>
            <strong>${escapeHtml(row.keyword)}</strong>
            <strong style="color: var(--accent);">${channelNameText}</strong>
            ${videoHtml}
            <a class="link-chip" href="${searchUrl}" target="_blank" rel="noreferrer" style="text-align: center; border-radius: 6px;">YouTube</a>
            <button class="keywords-delete-btn" type="button" data-delete-admin-keyword="${escapeHtml(row.keyword)}">Delete</button>
          </div>
        `;
      }).join("")}
    </div>
  `;

  attachRankBadgeClickListeners(container);

  // Attach delete button listeners
  container.querySelectorAll("[data-delete-admin-keyword]").forEach(btn => {
    btn.addEventListener("click", async () => {
      const kw = btn.dataset.deleteAdminKeyword;
      if (confirm(`Remove admin keyword "${kw}" from tracking?`)) {
        try {
          btn.disabled = true;
          btn.textContent = "Deleting...";
          const updated = await api("/api/admin/keywords/track", {
            method: "DELETE",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ keyword: kw })
          });
          state.adminKeywordRankings = updated;
          renderAdminKeywordsView();
        } catch (err) {
          alert(err.message || "Failed to delete admin keyword.");
        } finally {
          btn.disabled = false;
          btn.textContent = "Delete";
        }
      }
    });
  });
  
  // Track input disabled check
  const trackBtn = document.querySelector("#addAdminKeywordButton");
  const trackInput = document.querySelector("#adminKeywordInput");
  if (trackBtn && trackInput) {
    if (list.length >= 30) {
      trackBtn.disabled = true;
      trackInput.disabled = true;
      trackInput.placeholder = "Limit of 30 admin keywords reached.";
    } else {
      trackBtn.disabled = false;
      trackInput.disabled = false;
      trackInput.placeholder = "e.g. current affairs today";
    }
  }
}

async function addAdminKeyword() {
  const input = document.querySelector("#adminKeywordInput");
  const keyword = (input?.value || "").trim();
  if (!keyword) return;

  const btn = document.querySelector("#addAdminKeywordButton");
  const progressBarContainer = document.querySelector("#keywordsProgressBarContainer");
  const progressBarFill = document.querySelector("#keywordsProgressBarFill");
  const progressBarLabel = document.querySelector("#keywordsProgressBarLabel");

  try {
    if (btn) btn.disabled = true;
    if (input) input.disabled = true;
    if (progressBarContainer) progressBarContainer.classList.remove("is-hidden");
    if (progressBarFill) progressBarFill.style.width = "40%";
    if (progressBarLabel) progressBarLabel.textContent = "Scanning keyword rank across channels... (40%)";

    const updated = await api("/api/admin/keywords/track", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ keyword })
    });
    
    if (progressBarFill) progressBarFill.style.width = "100%";
    if (progressBarLabel) progressBarLabel.textContent = "Scan complete! (100%)";
    
    state.adminKeywordRankings = updated;
    renderAdminKeywordsView();
    if (input) input.value = "";
    
    setTimeout(() => {
      if (progressBarContainer) progressBarContainer.classList.add("is-hidden");
    }, 1500);
  } catch (err) {
    alert(err.message || "Failed to add admin keyword.");
    if (progressBarContainer) progressBarContainer.classList.add("is-hidden");
  } finally {
    if (btn) btn.disabled = false;
    if (input) input.disabled = false;
    if (input) input.focus();
  }
}

async function refreshAdminKeywordRankings() {
  const refreshBtn = document.querySelector("#keywordsRefreshButton");
  const progressBarContainer = document.querySelector("#keywordsProgressBarContainer");
  const progressBarFill = document.querySelector("#keywordsProgressBarFill");
  const progressBarLabel = document.querySelector("#keywordsProgressBarLabel");

  try {
    if (refreshBtn) refreshBtn.disabled = true;
    if (progressBarContainer) progressBarContainer.classList.remove("is-hidden");
    
    let progress = 0;
    const interval = setInterval(() => {
      progress += 10;
      if (progress > 90) progress = 90;
      if (progressBarFill) progressBarFill.style.width = progress + "%";
      if (progressBarLabel) progressBarLabel.textContent = `Scanning admin keyword rankings... (${progress}%)`;
    }, 1000);

    const updated = await api("/api/admin/keywords/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" }
    });
    
    clearInterval(interval);
    if (progressBarFill) progressBarFill.style.width = "100%";
    if (progressBarLabel) progressBarLabel.textContent = "Scan complete! (100%)";
    
    state.adminKeywordRankings = updated;
    renderAdminKeywordsView();
    
    setTimeout(() => {
      if (progressBarContainer) progressBarContainer.classList.add("is-hidden");
    }, 1500);
  } catch (err) {
    alert(err.message || "Failed to refresh admin keyword rankings.");
    if (progressBarContainer) progressBarContainer.classList.add("is-hidden");
  } finally {
    if (refreshBtn) refreshBtn.disabled = false;
  }
}

// Attach event listeners for Admin Keywords panel
document.querySelector("#addAdminKeywordButton")?.addEventListener("click", () => {
  addAdminKeyword();
});

document.querySelector("#adminKeywordInput")?.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    addAdminKeyword();
  }
});

// Comments Moderator Controllers
async function loadComments(options = {}) {
  const container = document.querySelector("#commentsTableContainer");
  if (!container) return;

  try {
    if (!options.force && !state.comments) {
      container.innerHTML = emptyCard("Loading comments...");
    }

    const res = await api("/api/comments");
    state.comments = res.comments || [];
    state.commentSettings = res.settings || { customBlockedWords: [], autoDeleteNegative: false, totalDeletedCount: 0 };

    renderCommentsView();
  } catch (err) {
    console.error("Failed to load comments:", err);
    container.innerHTML = emptyCard("Error loading comments: " + err.message);
  }
}

function renderCommentsView() {
  const container = document.querySelector("#commentsTableContainer");
  if (!container) return;

  const comments = state.comments || [];
  const settings = state.commentSettings || { customBlockedWords: [], autoDeleteNegative: false, totalDeletedCount: 0 };
  const filter = state.commentsSentimentFilter || "all";
  const manager = state.commentsManagerFilter || "All";

  // Filter by manager first to show manager-specific stats
  const managerComments = comments.filter(c => {
    if (manager === "All") return true;
    return getYtmName(c.channelName, c.channelId) === manager;
  });

  // Update summary stats based on selected manager
  const negativeCount = managerComments.filter(c => c.sentiment === "negative").length;
  document.querySelector("#commentsTotalFetched").textContent = managerComments.length;
  document.querySelector("#commentsNegativeCount").textContent = negativeCount;

  // Render blocked words tags
  const tagsContainer = document.querySelector("#blockedWordsTagsContainer");
  if (tagsContainer) {
    const words = settings.customBlockedWords || [];
    if (!words.length) {
      tagsContainer.innerHTML = `<span style="font-size: 11px; color: var(--muted);">No custom blocked words added.</span>`;
    } else {
      tagsContainer.innerHTML = words.map(w => `
        <span class="filter-chip" style="padding: 2px 8px; font-size: 12px; display: inline-flex; align-items: center; gap: 6px; cursor: default; background: var(--surface);">
          ${escapeHtml(w)}
          <span class="remove-word-btn" data-word="${escapeHtml(w)}" style="color: var(--muted); cursor: pointer; font-weight: 700; font-size: 10px;">×</span>
        </span>
      `).join("");
      
      // Bind tag remove listeners
      tagsContainer.querySelectorAll(".remove-word-btn").forEach(btn => {
        btn.addEventListener("click", () => {
          removeBlockedWord(btn.dataset.word);
        });
      });
    }
  }

  // Filter by sentiment/content rules for display
  const filteredComments = managerComments.filter(c => {
    if (filter === "negative") return c.sentiment === "negative";
    if (filter === "char100") return (c.text || "").length > 100;
    if (filter === "link") {
      const text = (c.text || "").toLowerCase();
      return text.includes("link");
    }
    if (filter === "voice") {
      const text = (c.text || "").toLowerCase();
      const voiceWords = ["awaz", "awaaz", "voice", "awaj", "sound"];
      return voiceWords.some(w => text.includes(w));
    }
    return true;
  });

  // Helper to highlight 'link' word (yellow) and voice issue words (pink)
  function highlightCommentContent(text) {
    let escaped = escapeHtml(text || "");
    
    // Highlight the word "link" (Yellow highlight)
    const linkRegex = /\b(link)\b/gi;
    escaped = escaped.replace(linkRegex, (match) => {
      return `<mark style="background: #fef08a; padding: 2px 4px; border-radius: 4px; color: #854d0e; font-weight: 600;">${match}</mark>`;
    });
    
    // Highlight Voice keywords (Pink highlight)
    const voiceWords = ["awaz", "awaaz", "voice", "awaj", "sound"];
    const voiceRegex = new RegExp(`\\b(${voiceWords.join("|")})\\b`, "gi");
    escaped = escaped.replace(voiceRegex, (match) => {
      return `<mark style="background: #fbcfe8; color: #9d174d; padding: 2px 4px; border-radius: 4px; font-weight: 600;">${match}</mark>`;
    });
    
    return escaped;
  }

  // Toggle batch delete button
  const batchBtn = document.querySelector("#deleteBatchNegativeCommentsBtn");
  if (batchBtn) {
    const hasNegatives = comments.some(c => c.sentiment === "negative");
    batchBtn.style.display = hasNegatives ? "inline-flex" : "none";
  }

  if (!filteredComments.length) {
    let emptyMsg = "No comments found on your channels.";
    if (filter === "negative") emptyMsg = "No negative sentiment comments found.";
    else if (filter === "char100") emptyMsg = "No comments found with more than 100 characters.";
    else if (filter === "link") emptyMsg = "No comments found containing links.";
    else if (filter === "voice") emptyMsg = "No comments found with voice/sound issues.";
    container.innerHTML = emptyCard(emptyMsg);
    return;
  }

  // Render table
  container.innerHTML = `
    <div class="keywords-table">
      <div class="keywords-row keywords-head" style="grid-template-columns: 50px 140px 1fr 130px 130px 150px;">
        <span>User</span>
        <span>Author</span>
        <span>Comment Text</span>
        <span>Sentiment</span>
        <span>Channel</span>
        <span>Action</span>
      </div>
      ${filteredComments.map(row => {
        const dateStr = new Date(row.publishedAt).toLocaleDateString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        const videoUrl = "https://www.youtube.com/watch?v=" + row.videoId;
        
        let selectColor = "#16a34a"; // positive
        if (row.sentiment === "negative") selectColor = "#dc2626";
        else if (row.sentiment === "neutral") selectColor = "#4b5563";

        return `
          <div class="comment-item-wrapper" style="display: flex; flex-direction: column; border-bottom: 1px solid var(--line);">
            <div class="keywords-row" style="grid-template-columns: 50px 140px 1fr 130px 130px 150px; align-items: center; padding: 12px 16px; border-bottom: none;">
              <img src="${escapeHtml(row.authorAvatar)}" style="width: 32px; height: 32px; border-radius: 50%; border: 1px solid var(--line);" alt="" />
              <div style="display: flex; flex-direction: column; gap: 2px; min-width: 0;">
                <strong style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis; max-width: 120px; display: block;" title="${escapeHtml(row.authorName)}">${escapeHtml(row.authorName)}</strong>
                <span style="font-size: 10px; color: var(--muted);">${escapeHtml(dateStr)}</span>
              </div>
              <div style="font-size: 13px; line-height: 1.4; color: var(--ink); word-break: break-word;">
                ${highlightCommentContent(row.text)}
                <div style="margin-top: 4px;">
                  <a href="${videoUrl}" target="_blank" rel="noreferrer" style="font-size: 11px; color: var(--accent); text-decoration: none;">Watch video</a>
                </div>
              </div>
              <div>
                <select class="sentiment-select" data-comment-id="${escapeHtml(row.id)}" style="background: #fff; color: ${selectColor}; border: 1px solid var(--line); padding: 4px 8px; border-radius: 6px; font-size: 12px; font-weight: 700; outline: none; width: 110px; cursor: pointer; text-transform: uppercase;">
                  <option value="positive" ${row.sentiment === "positive" ? "selected" : ""} style="color: #16a34a; font-weight: 700;">Positive</option>
                  <option value="neutral" ${row.sentiment === "neutral" ? "selected" : ""} style="color: #4b5563; font-weight: 700;">Neutral</option>
                  <option value="negative" ${row.sentiment === "negative" ? "selected" : ""} style="color: #dc2626; font-weight: 700;">Negative</option>
                </select>
              </div>
              <strong style="font-size: 12px; color: var(--muted);">${escapeHtml(row.channelName)}</strong>
              <div style="display: flex; gap: 8px; align-items: center;">
                <button class="keywords-delete-btn delete-comment-btn" type="button" data-comment-id="${escapeHtml(row.id)}" data-channel-id="${escapeHtml(row.channelId)}" style="margin: 0; padding: 6px 10px; font-size: 11px; background: #dc2626; color: #fff; border: none; height: auto;">Delete</button>
                <button class="connect-button reply-comment-btn" type="button" data-comment-id="${escapeHtml(row.id)}" data-channel-id="${escapeHtml(row.channelId)}" data-author-name="${escapeHtml(row.authorName)}" data-comment-text="${escapeHtml(row.text)}" style="margin: 0; padding: 6px 10px; font-size: 11px; height: auto; background: var(--surface); color: var(--ink); border: 1px solid var(--line);">Reply</button>
              </div>
            </div>
            
            <!-- Inline Reply Box -->
            <div class="reply-assistant-box" id="reply-box-${escapeHtml(row.id)}" style="display: none; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 12px; margin: 0 16px 12px 16px; flex-direction: column; gap: 10px;">
              <div style="font-size: 11px; text-transform: uppercase; color: var(--muted); font-weight: 600;">Reply to Comment</div>
              <textarea class="reply-text-input" id="reply-text-${escapeHtml(row.id)}" style="width: 100%; height: 80px; background: #fff; color: var(--ink); border: 1px solid var(--line); padding: 8px 12px; border-radius: 6px; font-size: 13px; outline: none; box-sizing: border-box; resize: vertical; line-height: 1.4;" placeholder="Write a reply..."></textarea>
              <div style="display: flex; gap: 8px; justify-content: flex-end; align-items: center; width: 100%;">
                <button class="cancel-reply-btn" type="button" data-comment-id="${escapeHtml(row.id)}" style="background: #fff; color: var(--ink); border: 1px solid var(--line); border-radius: 6px; padding: 6px 12px; font-size: 12px; font-weight: 600; cursor: pointer; transition: all 0.2s;">Cancel</button>
                <button class="send-reply-btn" type="button" data-comment-id="${escapeHtml(row.id)}" data-channel-id="${escapeHtml(row.channelId)}" style="background: var(--blue, #3c6ee8); color: #fff; border: none; border-radius: 6px; padding: 6px 16px; font-size: 12px; font-weight: 600; cursor: pointer; transition: all 0.2s;">Post Reply</button>
              </div>
            </div>
          </div>
        `;
      }).join("")}
    </div>
  `;

  // Bind individual delete listeners
  container.querySelectorAll(".delete-comment-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      deleteComment(btn.dataset.commentId, btn.dataset.channelId);
    });
  });

  // Bind sentiment override selection dropdowns
  container.querySelectorAll(".sentiment-select").forEach(select => {
    select.addEventListener("change", async (e) => {
      const commentId = select.dataset.commentId;
      const sentiment = e.target.value;
      try {
        await api(`/api/comments/${commentId}/sentiment`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ sentiment })
        });
        const c = state.comments.find(item => item.id === commentId);
        if (c) c.sentiment = sentiment;
        renderCommentsView();
      } catch (err) {
        alert("Failed to update sentiment: " + err.message);
      }
    });
  });

  // Bind Reply Toggle
  container.querySelectorAll(".reply-comment-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const commentId = btn.dataset.commentId;
      const box = document.getElementById(`reply-box-${commentId}`);
      const textInput = document.getElementById(`reply-text-${commentId}`);
      
      if (!box || !textInput) return;
      
      if (box.style.display === "flex") {
        box.style.display = "none";
        return;
      }
      
      box.style.display = "flex";
      textInput.value = "";
      textInput.placeholder = "Write a reply...";
      textInput.focus();
    });
  });

  // Bind Cancel Reply Box Button
  container.querySelectorAll(".cancel-reply-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      const commentId = btn.dataset.commentId;
      const box = document.getElementById(`reply-box-${commentId}`);
      if (box) box.style.display = "none";
    });
  });

  // Bind Send/Post Comment Reply Button
  container.querySelectorAll(".send-reply-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      const commentId = btn.dataset.commentId;
      const channelId = btn.dataset.channelId;
      const textInput = document.getElementById(`reply-text-${commentId}`);
      const box = document.getElementById(`reply-box-${commentId}`);
      if (!textInput || !textInput.value.trim()) {
        alert("Please enter reply text.");
        return;
      }
      
      const replyText = textInput.value.trim();
      const originalText = btn.textContent;
      try {
        btn.disabled = true;
        btn.textContent = "Posting...";
        await api("/api/ytm/comment/reply", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ channelId, parentId: commentId, replyText })
        });
        alert("Reply posted successfully!");
        if (box) box.style.display = "none";
      } catch (err) {
        alert("Failed to post reply: " + err.message);
      } finally {
        btn.disabled = false;
        btn.textContent = originalText;
      }
    });
  });
}

async function deleteComment(commentId, channelId) {
  if (!confirm("Are you sure you want to permanently delete this comment from YouTube?")) {
    return;
  }
  try {
    const btn = document.querySelector(`[data-comment-id="${commentId}"]`);
    if (btn) {
      btn.disabled = true;
      btn.textContent = "Deleting...";
    }
    await api(`/api/comments/${commentId}?channelId=${channelId}`, { method: "DELETE" });
    state.comments = (state.comments || []).filter(c => c.id !== commentId);
    renderCommentsView();
  } catch (err) {
    alert("Failed to delete comment: " + err.message);
    loadComments({ force: true });
  }
}

async function deleteBatchNegativeComments() {
  const negatives = (state.comments || []).filter(c => c.sentiment === "negative");
  if (!negatives.length) return;

  if (!confirm(`Are you sure you want to permanently delete all ${negatives.length} negative comments from YouTube?`)) {
    return;
  }

  const btn = document.querySelector("#deleteBatchNegativeCommentsBtn");
  const originalText = btn.textContent;
  try {
    btn.disabled = true;
    btn.textContent = "Deleting all...";
    const commentIds = negatives.map(c => c.id);
    const res = await api("/api/comments/delete-batch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ commentIds })
    });
    alert(`Successfully deleted ${res.count} comments.`);
    loadComments({ force: true });
  } catch (err) {
    alert("Batch delete failed: " + err.message);
  } finally {
    btn.disabled = false;
    btn.textContent = originalText;
  }
}

async function addBlockedWord() {
  const input = document.querySelector("#customBlockedWordInput");
  const word = input?.value?.trim();
  if (!word) return;

  const settings = state.commentSettings || { customBlockedWords: [] };
  const words = settings.customBlockedWords || [];
  if (words.some(w => w.toLowerCase() === word.toLowerCase())) {
    alert("Word is already blocked.");
    return;
  }

  words.push(word);
  try {
    const updated = await api("/api/comments/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ customBlockedWords: words })
    });
    state.commentSettings = updated;
    input.value = "";
    if (state.comments) {
      state.comments.forEach(c => {
        if (c.text.toLowerCase().includes(word.toLowerCase())) {
          c.sentiment = "negative";
        }
      });
    }
    renderCommentsView();
  } catch (err) {
    alert("Failed to add word: " + err.message);
  }
}

async function removeBlockedWord(word) {
  const settings = state.commentSettings || { customBlockedWords: [] };
  const words = (settings.customBlockedWords || []).filter(w => w !== word);
  try {
    const updated = await api("/api/comments/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ customBlockedWords: words })
    });
    state.commentSettings = updated;
    loadComments({ force: true });
  } catch (err) {
    alert("Failed to remove word: " + err.message);
  }
}

function setupCommentsViewListeners() {
  document.querySelector("#addBlockedWordButton")?.addEventListener("click", () => {
    addBlockedWord();
  });

  document.querySelector("#customBlockedWordInput")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      addBlockedWord();
    }
  });

  document.querySelector("#deleteBatchNegativeCommentsBtn")?.addEventListener("click", () => {
    deleteBatchNegativeComments();
  });

  document.querySelector("#commentsManagerSelect")?.addEventListener("change", (e) => {
    state.commentsManagerFilter = e.target.value;
    renderCommentsView();
  });

  document.querySelectorAll("[data-sentiment-filter]").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll("[data-sentiment-filter]").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      state.commentsSentimentFilter = btn.dataset.sentimentFilter;
      renderCommentsView();
    });
  });
}

setTimeout(setupCommentsViewListeners, 1000);

state.liveAutomatorSelectedChannel = "";
state.liveAutomatorStreams = [];
state.liveAutomatorTasks = [];

function populateLiveAutomatorChannels() {
  const select = document.querySelector("#liveAutomatorChannelSelect");
  if (select && state.channels) {
    const publicChs = state.channels.filter(c => c.id !== "all-in-one");
    select.innerHTML = publicChs.map(c => `
      <option value="${escapeHtml(c.id)}">${escapeHtml(c.name)}</option>
    `).join("");
    
    if (!state.liveAutomatorSelectedChannel && publicChs.length) {
      state.liveAutomatorSelectedChannel = publicChs[0].id;
    }
    
    if (state.liveAutomatorSelectedChannel) {
      select.value = state.liveAutomatorSelectedChannel;
    }
  }
}

async function loadLiveAutomator(options = {}) {
  populateLiveAutomatorChannels();
  
  const select = document.querySelector("#liveAutomatorChannelSelect");
  const channelId = select ? select.value : state.liveAutomatorSelectedChannel;
  if (!channelId) {
    document.querySelector("#liveAutomatorStreamsList").innerHTML = emptyCard("Please connect a channel first.");
    document.querySelector("#liveAutomatorTasksList").innerHTML = emptyCard("Please connect a channel first.");
    if (document.querySelector("#liveAutomatorRulesList")) {
      document.querySelector("#liveAutomatorRulesList").innerHTML = emptyCard("Please connect a channel first.");
    }
    return;
  }

  state.liveAutomatorSelectedChannel = channelId;

  if (!options.force && state.liveAutomatorStreams.length && state.liveAutomatorStreamsChannelId === channelId) {
    renderLiveAutomatorView();
    return;
  }

  document.querySelector("#liveAutomatorStreamsList").innerHTML = emptyCard("Fetching streams...");
  document.querySelector("#liveAutomatorTasksList").innerHTML = emptyCard("Fetching automation tasks...");
  if (document.querySelector("#liveAutomatorRulesList")) {
    document.querySelector("#liveAutomatorRulesList").innerHTML = emptyCard("Fetching rules...");
  }

  const steps = [
    { time: 0, text: "Searching channel for live/upcoming broadcasts..." },
    { time: 2, text: "Retrieving active tasks and rules..." },
    { time: 4, text: "Rendering live automator dashboard..." }
  ];
  const progressBar = startProgressBar("liveAutomatorProgressBarContainer", "liveAutomatorProgressBarFill", "liveAutomatorProgressBarLabel", steps);

  try {
    const [streamsRes, tasksRes, rulesRes] = await Promise.all([
      api(`/api/live-automator/streams?channelId=${encodeURIComponent(channelId)}`),
      api(`/api/live-automator/tasks?channelId=${encodeURIComponent(channelId)}`),
      api(`/api/live-automator/rules?channelId=${encodeURIComponent(channelId)}`)
    ]);

    if (progressBar) progressBar.stop(true, "Dashboard Loaded!");
    state.liveAutomatorStreams = streamsRes.streams || [];
    state.liveAutomatorTasks = tasksRes.tasks || [];
    state.liveAutomatorRules = rulesRes.rules || [];
    state.liveAutomatorStreamsChannelId = channelId;
    renderLiveAutomatorView();
  } catch (error) {
    if (progressBar) progressBar.stop(false, "Load Failed!");
    document.querySelector("#liveAutomatorStreamsList").innerHTML = emptyCard(error.message);
    document.querySelector("#liveAutomatorTasksList").innerHTML = emptyCard(error.message);
    if (document.querySelector("#liveAutomatorRulesList")) {
      document.querySelector("#liveAutomatorRulesList").innerHTML = emptyCard(error.message);
    }
  }
}

function renderLiveAutomatorView() {
  renderLiveAutomatorRules();
  renderLiveAutomatorStreams();
  renderLiveAutomatorTasks();
}

function renderLiveAutomatorStreams() {
  const container = document.querySelector("#liveAutomatorStreamsList");
  if (!container) return;

  if (!state.liveAutomatorStreams || !state.liveAutomatorStreams.length) {
    container.innerHTML = emptyCard("No upcoming live streams found.");
    return;
  }

  container.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 12px;">
      ${state.liveAutomatorStreams.map(stream => {
        const task = state.liveAutomatorTasks.find(t => t.videoId === stream.id && t.status === "pending");
        const hasTask = Boolean(task);
        const statusBadge = `<span style="background: #2563eb; color: #fff; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 700; text-transform: uppercase;">Upcoming</span>`;
        const autoCommentBadge = task && task.autoCreated 
          ? `<span style="background: #059669; color: #fff; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: 700; text-transform: uppercase;">Auto Comment Set</span>`
          : "";
        const dateToUse = stream.scheduledStartTime || stream.publishedAt;
        const dateStr = new Date(dateToUse).toLocaleDateString(undefined, { 
          month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' 
        });
        return `
          <div style="display: flex; gap: 12px; padding: 12px; border: 1px solid var(--line); border-radius: 6px; align-items: center;">
            <img src="${escapeHtml(stream.thumbnail)}" style="width: 80px; height: 60px; border-radius: 4px; object-fit: cover;" alt="" />
            <div style="flex: 1; min-width: 0;">
              <div style="font-size: 13px; font-weight: 600; color: var(--ink); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="${escapeHtml(stream.title)}">
                ${escapeHtml(stream.title)}
              </div>
              <div style="margin-top: 4px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap;">
                ${statusBadge}
                ${autoCommentBadge}
                <span style="font-size: 11px; color: var(--muted);" title="Scheduled Start Time">Start: ${escapeHtml(dateStr)}</span>
              </div>
            </div>
            <div>
              <button class="connect-button automator-opt-btn" type="button" data-video-id="${escapeHtml(stream.id)}" data-video-title="${escapeHtml(stream.title)}" data-scheduled-start-time="${escapeHtml(stream.scheduledStartTime || "")}" style="margin: 0; padding: 6px 12px; font-size: 12px; height: auto; width: auto; background: ${hasTask ? "#059669" : ""}">
                ${hasTask ? "Edit Comment" : "Add Comment"}
              </button>
            </div>
          </div>
        `;
      }).join("")}
    </div>
  `;

  // Bind edit/add listeners
  container.querySelectorAll(".automator-opt-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      openLiveAutomatorDialog(btn.dataset.videoId, btn.dataset.videoTitle, btn.dataset.scheduledStartTime);
    });
  });
}

function renderLiveAutomatorTasks() {
  const container = document.querySelector("#liveAutomatorTasksList");
  if (!container) return;

  if (!state.liveAutomatorTasks || !state.liveAutomatorTasks.length) {
    container.innerHTML = emptyCard("No comment automations saved for this channel.");
    return;
  }

  container.innerHTML = `
    <div style="display: flex; flex-direction: column; gap: 12px;">
      ${state.liveAutomatorTasks.map(task => {
        let statusColor = "#d97706"; // pending: amber
        if (task.status === "posted") statusColor = "#059669"; // green
        if (task.status === "failed") statusColor = "#dc2626"; // red
        
        return `
          <div style="display: flex; flex-direction: column; gap: 8px; padding: 12px; border: 1px solid var(--line); border-radius: 6px;">
            <div style="display: flex; justify-content: space-between; align-items: center;">
              <strong style="font-size: 12px; text-transform: uppercase; color: ${statusColor}; font-weight: 700;">${task.status}</strong>
              <div style="display: flex; gap: 6px; align-items: center;">
                <a href="https://www.youtube.com/watch?v=${escapeHtml(task.videoId)}" target="_blank" class="ghost-button" style="margin: 0; padding: 2px 6px; font-size: 11px; height: auto; width: auto; color: var(--ink); border: 1px solid var(--line); background: var(--surface); text-decoration: none; display: inline-flex; align-items: center; justify-content: center;">View Video</a>
                <button class="ghost-button automator-delete-btn" type="button" data-video-id="${escapeHtml(task.videoId)}" style="margin: 0; padding: 2px 6px; font-size: 11px; height: auto; width: auto; color: #dc2626; border: 1px solid #fecaca; background: #fef2f2;">Delete</button>
              </div>
            </div>
            <div style="font-size: 13px; font-weight: 600; color: var(--ink); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">
              ${escapeHtml(task.videoTitle)}
            </div>
            <div style="font-size: 12px; background: var(--surface); border: 1px solid var(--line); border-radius: 4px; padding: 8px; font-family: monospace; white-space: pre-wrap; word-break: break-all;">${escapeHtml(task.commentText)}</div>
            ${task.error ? `<div style="font-size: 11px; color: #dc2626; font-weight: 600;">Error: ${escapeHtml(task.error)}</div>` : ""}
            ${task.postedAt ? `<div style="font-size: 11px; color: var(--muted);">Posted at: ${new Date(task.postedAt).toLocaleString()}</div>` : ""}
          </div>
        `;
      }).join("")}
    </div>
  `;

  // Bind delete listeners
  container.querySelectorAll(".automator-delete-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      deleteLiveAutomatorTask(btn.dataset.videoId);
    });
  });
}

let activeAutomatorVideoId = "";
let activeAutomatorVideoTitle = "";
let activeAutomatorScheduledStartTime = "";

function openLiveAutomatorDialog(videoId, videoTitle, scheduledStartTime) {
  activeAutomatorVideoId = videoId;
  activeAutomatorVideoTitle = videoTitle;
  activeAutomatorScheduledStartTime = scheduledStartTime || "";

  const dialog = document.getElementById("liveAutomatorDialog");
  const titleEl = document.getElementById("liveAutomatorDialogStreamTitle");
  const commentArea = document.getElementById("liveAutomatorCommentText");

  if (!dialog || !titleEl || !commentArea) return;

  titleEl.textContent = `Automate Comment: ${truncateTitle(videoTitle, 35)}`;
  
  // Fill existing text if any
  const existing = state.liveAutomatorTasks.find(t => t.videoId === videoId);
  commentArea.value = existing ? existing.commentText : "";

  dialog.showModal();
}

async function saveLiveAutomatorTask() {
  const commentText = document.getElementById("liveAutomatorCommentText").value.trim();
  if (!commentText) {
    alert("Please enter comment text.");
    return;
  }

  const btn = document.getElementById("liveAutomatorSaveBtn");
  const origText = btn.textContent;
  btn.textContent = "Saving...";
  btn.disabled = true;

  try {
    await api("/api/live-automator/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        channelId: state.liveAutomatorSelectedChannel,
        videoId: activeAutomatorVideoId,
        videoTitle: activeAutomatorVideoTitle,
        commentText,
        scheduledStartTime: activeAutomatorScheduledStartTime
      })
    });

    document.getElementById("liveAutomatorDialog").close();
    await loadLiveAutomator({ force: true });
  } catch (err) {
    alert("Failed to save automation: " + err.message);
  } finally {
    btn.textContent = origText;
    btn.disabled = false;
  }
}

async function deleteLiveAutomatorTask(videoId) {
  if (!confirm("Are you sure you want to delete this comment automation?")) return;

  try {
    await api("/api/live-automator/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ videoId })
    });
    await loadLiveAutomator({ force: true });
  } catch (err) {
    alert("Failed to delete automation: " + err.message);
  }
}

function renderLiveAutomatorRules() {
  const container = document.querySelector("#liveAutomatorRulesList");
  if (!container) return;

  if (!state.liveAutomatorRules || !state.liveAutomatorRules.length) {
    container.innerHTML = `<div style="font-size: 12px; color: var(--muted); text-align: center; padding: 16px;">No keyword rules saved.</div>`;
    return;
  }

  container.innerHTML = state.liveAutomatorRules.map(rule => `
    <div style="display: flex; flex-direction: column; gap: 6px; padding: 10px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface);">
      <div style="display: flex; justify-content: space-between; align-items: center;">
        <span style="background: var(--line); color: var(--ink); padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: 700; font-family: monospace;">${escapeHtml(rule.keyword)}</span>
        <div style="display: flex; gap: 6px; align-items: center;">
          <button class="ghost-button automator-rule-edit-btn" type="button" data-keyword="${escapeHtml(rule.keyword)}" data-comment-text="${escapeHtml(rule.commentText)}" style="margin: 0; padding: 2px 6px; font-size: 11px; height: auto; width: auto; color: var(--ink); border: 1px solid var(--line); background: #fff;">Edit</button>
          <button class="ghost-button automator-rule-delete-btn" type="button" data-keyword="${escapeHtml(rule.keyword)}" style="margin: 0; padding: 2px 6px; font-size: 11px; height: auto; width: auto; color: #dc2626; border: 1px solid #fecaca; background: #fef2f2;">Delete</button>
        </div>
      </div>
      <div style="font-size: 11px; color: var(--muted); font-family: monospace; white-space: pre-wrap; max-height: 80px; overflow-y: auto; border: 1px solid transparent; padding: 2px 0;">${escapeHtml(rule.commentText)}</div>
    </div>
  `).join("");

  // Bind edit & delete click listeners
  container.querySelectorAll(".automator-rule-edit-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.getElementById("liveAutomatorRuleKeyword").value = btn.dataset.keyword;
      document.getElementById("liveAutomatorRuleCommentText").value = btn.dataset.commentText;
    });
  });

  container.querySelectorAll(".automator-rule-delete-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      deleteLiveAutomatorRule(btn.dataset.keyword);
    });
  });
}

async function saveLiveAutomatorRule() {
  const keyword = document.getElementById("liveAutomatorRuleKeyword").value.trim();
  const commentText = document.getElementById("liveAutomatorRuleCommentText").value.trim();

  if (!keyword || !commentText) {
    alert("Please fill in both Keyword and Comment Text.");
    return;
  }

  const btn = document.getElementById("liveAutomatorRuleSaveBtn");
  const origText = btn.textContent;
  btn.textContent = "Saving...";
  btn.disabled = true;

  try {
    await api("/api/live-automator/rules/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        channelId: state.liveAutomatorSelectedChannel,
        keyword,
        commentText
      })
    });

    document.getElementById("liveAutomatorRuleKeyword").value = "";
    document.getElementById("liveAutomatorRuleCommentText").value = "";
    await loadLiveAutomator({ force: true });
  } catch (err) {
    alert("Failed to save rule: " + err.message);
  } finally {
    btn.textContent = origText;
    btn.disabled = false;
  }
}

async function deleteLiveAutomatorRule(keyword) {
  if (!confirm(`Are you sure you want to delete the auto-rule for "${keyword}"?`)) return;

  try {
    await api("/api/live-automator/rules/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        channelId: state.liveAutomatorSelectedChannel,
        keyword
      })
    });
    await loadLiveAutomator({ force: true });
  } catch (err) {
    alert("Failed to delete rule: " + err.message);
  }
}

function renderFacultyPerformance(data) {
  const container = document.querySelector("#facultyPerformanceTable");
  if (!container) return;

  if (!data || !data.length) {
    container.innerHTML = `<div style="font-size: 13px; color: var(--muted); text-align: center; padding: 24px;">No faculty performance data found for the configured keywords. Click "Manage Faculty" above to add names.</div>`;
    return;
  }

  container.innerHTML = `
    <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px;">
      <thead>
        <tr style="background: #f8fafc; border-bottom: 1px solid var(--line); font-weight: 600; color: var(--ink);">
          <th style="padding: 12px 16px;">Faculty Name</th>
          <th style="padding: 12px 16px; text-align: right;">Views Gained</th>
          <th style="padding: 12px 16px; text-align: right;">Subs Gained</th>
          <th style="padding: 12px 16px; text-align: right;">Videos</th>
          <th style="padding: 12px 16px; text-align: right;">Live Streams</th>
          <th style="padding: 12px 16px; text-align: right;">Like Ratio</th>
          <th style="padding: 12px 16px; text-align: right;">Total Duration</th>
        </tr>
      </thead>
      <tbody>
        ${data.map(row => `
          <tr style="border-bottom: 1px solid var(--line); color: var(--ink);">
            <td style="padding: 12px 16px; font-weight: 600;">${escapeHtml(row.facultyName)}</td>
            <td style="padding: 12px 16px; text-align: right; font-weight: 500;">${row.views.toLocaleString()}</td>
            <td style="padding: 12px 16px; text-align: right; color: #059669; font-weight: 500;">+${row.subscribers.toLocaleString()}</td>
            <td style="padding: 12px 16px; text-align: right;">${row.noOfVideos}</td>
            <td style="padding: 12px 16px; text-align: right;">${row.noOfLive}</td>
            <td style="padding: 12px 16px; text-align: right; font-weight: 500;">${row.likeRatio || "-"}</td>
            <td style="padding: 12px 16px; text-align: right;">${row.durationHours}h</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

async function openFacultyKeywordsManager() {
  const channelId = state.selectedChannelId;
  if (!channelId || channelId === "all-in-one") return;

  const dialog = document.getElementById("facultyKeywordsDialog");
  const textarea = document.getElementById("facultyKeywordsText");
  textarea.value = "Loading...";
  dialog.showModal();

  try {
    const res = await api(`/api/faculty-keywords?channelId=${encodeURIComponent(channelId)}`);
    textarea.value = (res.keywords || []).join(", ");
  } catch (err) {
    textarea.value = "";
    alert("Failed to load faculty keywords: " + err.message);
  }
}

async function saveFacultyKeywordsManager() {
  const channelId = state.selectedChannelId;
  if (!channelId || channelId === "all-in-one") return;

  const text = document.getElementById("facultyKeywordsText").value;
  const keywords = text.split(",").map(k => k.trim()).filter(Boolean);

  const btn = document.getElementById("facultyKeywordsSaveBtn");
  const origText = btn.textContent;
  btn.textContent = "Saving...";
  btn.disabled = true;

  try {
    await api("/api/faculty-keywords/save", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ channelId, keywords })
    });
    document.getElementById("facultyKeywordsDialog").close();
    await loadDashboard({ force: true });
  } catch (err) {
    alert("Failed to save faculty keywords: " + err.message);
  } finally {
    btn.textContent = origText;
    btn.disabled = false;
  }
}

function setupFacultyManagerListeners() {
  // Faculty keyword manager listeners
  document.getElementById("manageFacultyBtn")?.addEventListener("click", () => {
    openFacultyKeywordsManager();
  });

  document.getElementById("unmatchedVideosBtn")?.addEventListener("click", () => {
    window.openUnmatchedVideosManager();
  });

  document.getElementById("facultyKeywordsCancelBtn")?.addEventListener("click", () => {
    document.getElementById("facultyKeywordsDialog")?.close();
  });

  document.getElementById("facultyKeywordsSaveBtn")?.addEventListener("click", () => {
    saveFacultyKeywordsManager();
  });
}

window.openUnmatchedVideosManager = function() {
  const dialog = document.getElementById("unmatchedVideosDialog");
  const container = document.getElementById("unmatchedVideosList");
  if (!dialog || !container) return;

  const unmatched = state.report?.unmatchedVideos || [];
  if (!unmatched.length) {
    container.innerHTML = `
      <div style="text-align: center; padding: 32px; color: var(--muted); font-size: 14px;">
        🎉 All videos in this range have matched faculty keywords!
      </div>
    `;
    dialog.showModal();
    return;
  }

  container.innerHTML = unmatched.map(video => {
    const thumbnailUrl = `https://i.ytimg.com/vi/${video.id}/default.jpg`;
    return `
      <div style="display: flex; gap: 16px; border-bottom: 1px solid var(--line); padding-bottom: 16px; align-items: center; min-width: 0;">
        <img src="${thumbnailUrl}" alt="Thumbnail" style="width: 120px; aspect-ratio: 16/9; object-fit: cover; border-radius: 4px; background: #e2e8f0; border: 1px solid var(--line); flex-shrink: 0;" />
        <div style="flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 6px;">
          <div style="font-weight: 600; font-size: 14px; color: var(--ink); line-height: 1.4;">
            ${escapeHtml(video.title)}
          </div>
          <div style="font-size: 11px; color: var(--muted); display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
            <span>Published: ${new Date(video.publishedAt).toLocaleString()}</span>
            <span>|</span>
            <span>Format: ${video.format}</span>
            <span>|</span>
            <a href="https://www.youtube.com/watch?v=${video.id}" target="_blank" rel="noreferrer" style="color: #2563eb; text-decoration: none; font-weight: 600; display: inline-flex; align-items: center; gap: 4px;">
              📺 Watch Video
            </a>
          </div>
          <div style="display: flex; gap: 8px; align-items: center; width: 100%; margin-top: 4px;">
            <input type="text" id="input-title-${video.id}" maxlength="100" oninput="document.getElementById('char-count-${video.id}').textContent = this.value.length + '/100'" value="${escapeHtml(video.title)}" style="flex: 1; border: 1px solid var(--line); border-radius: 4px; padding: 6px 12px; font-size: 13px; height: 32px; box-sizing: border-box; outline: none;" />
            <span id="char-count-${video.id}" style="font-size: 11px; color: var(--muted); white-space: nowrap; margin-right: 4px;">${video.title.length}/100</span>
            <button onclick="window.updateVideoTitleDirectly('${video.id}')" id="btn-title-${video.id}" class="connect-button" style="height: 32px; padding: 0 12px; font-size: 12px; margin: 0; width: auto; background: #2563eb; color: #fff;">
              Update Title
            </button>
          </div>
        </div>
      </div>
    `;
  }).join("");

  dialog.showModal();
};

window.updateVideoTitleDirectly = async function(videoId) {
  const input = document.getElementById(`input-title-${videoId}`);
  const btn = document.getElementById(`btn-title-${videoId}`);
  const channelId = state.selectedChannelId;
  if (!input || !btn || !channelId) return;

  const newTitle = input.value.trim();
  if (!newTitle) {
    alert("Title cannot be empty!");
    return;
  }
  if (newTitle.length > 100) {
    alert(`YouTube video titles cannot exceed 100 characters. Your title is currently ${newTitle.length} characters long. Please shorten it before updating.`);
    return;
  }

  const oldText = btn.textContent;
  btn.textContent = "Updating...";
  btn.disabled = true;

  try {
    const res = await fetch("/api/update-video-title", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ videoId, channelId, newTitle })
    });
    const data = await res.json();
    if (!res.ok || data.error) {
      throw new Error(data.error || "Failed to update title");
    }
    
    alert("Title updated successfully on YouTube!");
    
    const refreshBtn = document.querySelector(".dashboard-header .refresh-button") || document.getElementById("refreshBtn");
    if (refreshBtn) {
      refreshBtn.click();
    } else {
      loadDashboard({ force: true });
    }
    
    document.getElementById("unmatchedVideosDialog")?.close();

  } catch (err) {
    alert("Error: " + err.message);
    btn.textContent = oldText;
    btn.disabled = false;
  }
};

// Initialize listeners
setTimeout(setupFacultyManagerListeners, 1000);

// ====================================================
// YOUTUBE OPS WORKSPACE CONTROLLER
// ====================================================

const opsState = {
  activeMode: "hub", // "hub" | "event" | "video" | "shorts"
  eventSubmode: "new", // "new" | "reuse"
  cachedDefaults: {},
  cachedPlaylists: {},
  cachedChannelEvents: {},
  facultyList: [],
  selectedFaculty: null,
  // Video flow
  videoFile: null,
  videoThumbBase64: null,
  videoDuration: 0,
  videoStep: 1,
  // Shorts flow
  shortsFile: null,
  shortsThumbBase64: null,
  shortsDuration: 0,
  shortsStep: 1,
  // Event flow
  eventThumbBase64: null,
  initialized: false,
};

function formatDuration(seconds) {
  const sec = Math.floor(seconds || 0);
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function formatBytes(bytes, decimals = 1) {
  if (!bytes || bytes === 0) return "0 Bytes";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + " " + sizes[i];
}

function fileToBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = (err) => reject(err);
    reader.readAsDataURL(file);
  });
}

function loadYouTubeOps() {
  initYouTubeOps();
  populateOpsChannels();
  if (opsState.activeMode === "hub") {
    switchOpsMode("hub");
  }
}

function initYouTubeOps() {
  if (opsState.initialized) return;
  opsState.initialized = true;

  // Back to Hub Button
  const backBtn = document.getElementById("opsBackToHubBtn");
  if (backBtn) {
    backBtn.addEventListener("click", () => switchOpsMode("hub"));
  }

  // Hub Cards Click
  document.querySelectorAll(".ops-hub-card").forEach((card) => {
    card.addEventListener("click", () => {
      const mode = card.dataset.opsMode;
      switchOpsMode(mode);
    });
  });

  // Channel Select Changes
  const eventChSelect = document.getElementById("opsEventChannelSelect");
  if (eventChSelect) {
    eventChSelect.addEventListener("change", () => onOpsChannelChange("event", eventChSelect.value));
  }

  const videoChSelect = document.getElementById("opsVideoChannelSelect");
  if (videoChSelect) {
    videoChSelect.addEventListener("change", () => {
      onOpsChannelChange("video", videoChSelect.value);
      validateVideoStep1();
    });
  }

  const shortsChSelect = document.getElementById("opsShortsChannelSelect");
  if (shortsChSelect) {
    shortsChSelect.addEventListener("change", () => {
      onOpsChannelChange("shorts", shortsChSelect.value);
      validateShortsStep1();
    });
  }

  // Title character counters & live mockup sync
  setupTitleCounterAndMockup("opsEventTitleInput", "opsEventTitleCount");
  setupTitleCounterAndMockup("opsVideoTitleInput", "opsVideoTitleCount", "opsVideoMockupTitle", "Video Title Preview");
  setupTitleCounterAndMockup("opsShortsTitleInput", "opsShortsTitleCount", "opsShortsMockupTitle", "Short Title #Shorts");

  // Shorts quick #Shorts button
  const shortsAddTagBtn = document.getElementById("opsShortsAddTagBtn");
  if (shortsAddTagBtn) {
    shortsAddTagBtn.addEventListener("click", () => {
      const input = document.getElementById("opsShortsTitleInput");
      if (input) {
        if (!input.value.toLowerCase().includes("#shorts")) {
          input.value = (input.value.trim() + " #Shorts").trim();
          input.dispatchEvent(new Event("input"));
        }
      }
    });
  }

  // Description defaults buttons (Reset / Save)
  setupDescDefaultButtons("event");
  setupDescDefaultButtons("video");
  setupDescDefaultButtons("shorts");

  // Video Dropzone & File Picker
  setupVideoFilePicker();

  // Shorts Dropzone & File Picker
  setupShortsFilePicker();

  // Thumbnails
  setupThumbPicker("event");
  setupThumbPicker("video");
  setupThumbPicker("shorts");

  // Video Wizard Steps Navigation
  setupVideoWizardNavigation();

  // Shorts Wizard Steps Navigation
  setupShortsWizardNavigation();

  // Event Form Submit
  const eventForm = document.getElementById("opsCreateEventForm");
  if (eventForm) {
    eventForm.addEventListener("submit", handleCreateLiveEvent);
  }

  // Event Subtabs (New Event vs Reuse Existing)
  const subtabNew = document.getElementById("opsEventSubtabNew");
  const subtabReuse = document.getElementById("opsEventSubtabReuse");
  const reusePickerWrap = document.getElementById("opsReusePickerWrap");
  const eventModeBadge = document.getElementById("opsEventModeBadge");
  const eventHeaderTitle = document.getElementById("opsEventHeaderTitle");
  const eventSubmitBtn = document.getElementById("opsCreateEventSubmitBtn");

  function setEventSubmode(submode) {
    opsState.eventSubmode = submode;
    const isReuse = submode === "reuse";

    if (subtabNew) {
      subtabNew.classList.toggle("active", !isReuse);
      subtabNew.classList.remove("active-sub-tab");
    }
    if (subtabReuse) {
      subtabReuse.classList.toggle("active", isReuse);
      subtabReuse.classList.remove("active-sub-tab");
    }
    if (reusePickerWrap) reusePickerWrap.classList.toggle("is-hidden", !isReuse);

    const descEl = document.getElementById("opsEventHeaderDesc");

    if (isReuse) {
      if (eventModeBadge) eventModeBadge.innerHTML = "↻ Reuse Stream Mode";
      if (eventHeaderTitle) eventHeaderTitle.textContent = "Reuse Existing Event";
      if (descEl) descEl.textContent = "Select an existing broadcast to auto-fill metadata, tags, thumbnail, playlist, and reuse the original OBS stream key.";
      if (eventSubmitBtn) {
        eventSubmitBtn.innerHTML = '🔁 Reuse Live Broadcast & Launch Stream <span class="ops-kbd-badge-bold">CTRL+ENTER</span>';
        eventSubmitBtn.style.background = "#0f172a";
        eventSubmitBtn.style.borderColor = "#0f172a";
      }
      const chSel = document.getElementById("opsEventChannelSelect");
      if (chSel && chSel.value) {
        loadExistingEventsForReuse(chSel.value);
      }
    } else {
      if (eventModeBadge) eventModeBadge.innerHTML = "🔴 Live Stream Mode";
      if (eventHeaderTitle) eventHeaderTitle.textContent = "Create New Event";
      if (descEl) descEl.textContent = "Schedule a fresh YouTube event with faculty assignment, metadata, thumbnail, and stream key generation.";
      if (eventSubmitBtn) {
        eventSubmitBtn.innerHTML = '🚀 Create Live Event & Generate Stream Key <span class="ops-kbd-badge-bold">CTRL+ENTER</span>';
        eventSubmitBtn.style.background = "#0f172a";
        eventSubmitBtn.style.borderColor = "#0f172a";
      }
    }
  }

  if (subtabNew) {
    subtabNew.addEventListener("click", () => setEventSubmode("new"));
  }
  if (subtabReuse) {
    subtabReuse.addEventListener("click", () => setEventSubmode("reuse"));
  }

  const refreshEventsBtn = document.getElementById("opsFetchExistingEventsBtn");
  if (refreshEventsBtn) {
    refreshEventsBtn.addEventListener("click", () => {
      const chSel = document.getElementById("opsEventChannelSelect");
      if (!chSel || !chSel.value) {
        alert("Please select a target channel first.");
        return;
      }
      loadExistingEventsForReuse(chSel.value, true);
    });
  }

  const reuseSelect = document.getElementById("opsReuseEventSelect");
  if (reuseSelect) {
    reuseSelect.addEventListener("change", () => onReuseEventSelected(reuseSelect.value));
  }

  // Clear Form button
  const clearFormBtn = document.getElementById("opsEventClearBtn");
  if (clearFormBtn) {
    clearFormBtn.addEventListener("click", clearOpsEventForm);
  }

  // Topbar connect & refresh buttons
  document.getElementById("opsConnectChannelBtn")?.addEventListener("click", () => {
    document.getElementById("connectChannelButton")?.click();
  });

  document.getElementById("opsRefreshChannelsBtn")?.addEventListener("click", async () => {
    await populateOpsChannels();
    const chSel = document.getElementById("opsEventChannelSelect");
    if (chSel && chSel.value) {
      onOpsChannelChange("event", chSel.value);
    }
  });

  // Global Keyboard Shortcuts for Event Mode
  document.addEventListener("keydown", (e) => {
    const eventView = document.getElementById("opsEventView");
    if (!eventView || eventView.classList.contains("is-hidden")) return;

    // Type '/' to focus faculty search input (when not actively inside an input or textarea)
    if (e.key === "/" && !["INPUT", "TEXTAREA", "SELECT"].includes(document.activeElement?.tagName)) {
      e.preventDefault();
      const facInput = document.getElementById("opsFacultySearchInput");
      if (facInput) {
        facInput.focus();
        facInput.select();
      }
      return;
    }

    // Ctrl+Enter or Cmd+Enter to submit
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      const activeForm = document.getElementById("opsCreateEventForm");
      if (activeForm) {
        e.preventDefault();
        const submitBtn = document.getElementById("opsCreateEventSubmitBtn");
        submitBtn?.click();
      }
      return;
    }

    // Esc to blur active element or close dropdown
    if (e.key === "Escape") {
      const dropdown = document.getElementById("opsFacultyDropdown");
      if (dropdown && !dropdown.classList.contains("is-hidden")) {
        dropdown.classList.add("is-hidden");
        return;
      }
      if (["INPUT", "TEXTAREA"].includes(document.activeElement?.tagName)) {
        document.activeElement.blur();
      }
    }
  });

  // Faculty Email ID Selector Setup
  setupFacultyPicker();

  // Initialize YouTube Studio Native Schedule Pickers
  initYouTubeSchedulePickers();

  // Initial date/times for Live Event (Start: +30m)
  setDefaultEventTimes();
}

function clearOpsEventForm() {
  const titleInput = document.getElementById("opsEventTitleInput");
  const descInput = document.getElementById("opsEventDescInput");
  const tagsInput = document.getElementById("opsEventTagsInput");
  const plSelect = document.getElementById("opsEventPlaylistSelect");
  const privSelect = document.getElementById("opsEventPrivacySelect");
  const typeSelect = document.getElementById("opsEventTypeSelect");
  const thumbRemoveBtn = document.getElementById("opsEventThumbRemoveBtn");

  if (titleInput) {
    titleInput.value = "";
    titleInput.dispatchEvent(new Event("input"));
  }
  if (descInput) descInput.value = "";
  if (tagsInput) tagsInput.value = "";
  if (plSelect) plSelect.value = "";
  if (privSelect) privSelect.value = "public";
  if (typeSelect) typeSelect.value = "Live Class";
  if (thumbRemoveBtn) thumbRemoveBtn.click();

  opsState.eventExistingThumbUrl = null;
  opsState.eventThumbBase64 = null;
  const thumbText = document.querySelector("#opsEventThumbDropzone .ops-clean-thumb-text");
  if (thumbText) thumbText.textContent = "No file chosen";

  clearFacultySelection();
  setDefaultEventTimes();

  const reuseSelect = document.getElementById("opsReuseEventSelect");
  if (reuseSelect) reuseSelect.value = "";

  const resultBox = document.getElementById("opsEventResultBox");
  if (resultBox) resultBox.classList.add("is-hidden");
}

function setupFacultyPicker() {
  const searchInput = document.getElementById("opsFacultySearchInput");
  const clearBtn = document.getElementById("opsFacultyClearBtn");
  const quickSelect = document.getElementById("opsFacultyQuickSelect");
  const dropdown = document.getElementById("opsFacultyDropdown");
  const chipRemove = document.getElementById("opsFacultyChipRemove");

  // Load faculty directory from server
  loadFacultyDirectory();

  if (searchInput) {
    searchInput.addEventListener("input", () => {
      const q = searchInput.value.trim();
      if (clearBtn) clearBtn.classList.toggle("is-hidden", !q);
      renderFacultyDropdown(q);
    });

    searchInput.addEventListener("focus", () => {
      renderFacultyDropdown(searchInput.value.trim());
    });

    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        const val = searchInput.value.trim();
        if (val) {
          if (val.includes("@")) {
            selectFaculty({ email: val, facultyName: val.split("@")[0] });
          } else if (opsState.facultyList.length > 0) {
            const match = opsState.facultyList.find(f => 
              (f.facultyName && f.facultyName.toLowerCase().includes(val.toLowerCase())) || 
              (f.email && f.email.toLowerCase().includes(val.toLowerCase()))
            );
            if (match) selectFaculty(match);
          }
        }
      } else if (e.key === "Escape") {
        if (dropdown) dropdown.classList.add("is-hidden");
      }
    });
  }

  if (clearBtn) {
    clearBtn.addEventListener("click", () => {
      clearFacultySelection();
      searchInput?.focus();
    });
  }

  if (chipRemove) {
    chipRemove.addEventListener("click", () => {
      clearFacultySelection();
      searchInput?.focus();
    });
  }

  if (quickSelect) {
    quickSelect.addEventListener("change", () => {
      const email = quickSelect.value;
      if (!email) {
        clearFacultySelection();
        return;
      }
      const match = opsState.facultyList.find(f => f.email.toLowerCase() === email.toLowerCase());
      if (match) {
        selectFaculty(match);
      }
    });
  }

  // Close dropdown on outside click
  document.addEventListener("click", (e) => {
    if (!e.target.closest(".ops-faculty-group")) {
      if (dropdown) dropdown.classList.add("is-hidden");
    }
  });
}

async function loadFacultyDirectory() {
  try {
    const res = await fetch("/api/youtube-ops/faculty");
    const data = await res.json();
    opsState.facultyList = data.faculty || [];
    populateFacultyQuickSelect();
  } catch (err) {
    console.warn("Failed to load faculty directory:", err);
  }
}

function populateFacultyQuickSelect() {
  const quickSelect = document.getElementById("opsFacultyQuickSelect");
  if (!quickSelect) return;
  const faculties = [...opsState.facultyList].sort((a, b) => (a.facultyName || "").localeCompare(b.facultyName || ""));
  
  let html = `<option value="">-- Quick Pick Faculty --</option>`;
  faculties.forEach(f => {
    html += `<option value="${escapeHtml(f.email)}">${escapeHtml(f.facultyName || f.email)}</option>`;
  });
  quickSelect.innerHTML = html;
}

function renderFacultyDropdown(query) {
  const dropdown = document.getElementById("opsFacultyDropdown");
  if (!dropdown) return;

  const q = (query || "").toLowerCase();
  const filtered = opsState.facultyList.filter(f => 
    !q || 
    (f.facultyName && f.facultyName.toLowerCase().includes(q)) || 
    (f.email && f.email.toLowerCase().includes(q))
  );

  let html = "";

  // If query looks like an email and not already in list, offer direct use
  if (q && q.includes("@") && !opsState.facultyList.some(f => f.email.toLowerCase() === q)) {
    html += `
      <div class="ops-faculty-item" style="background: #ecfdf5; border-left: 3px solid #10b981;" onclick="selectFaculty({ email: '${escapeHtml(query.trim())}', facultyName: '${escapeHtml(query.trim().split('@')[0])}' })">
        <div>
          <strong style="color: #047857;">+ Use "${escapeHtml(query.trim())}"</strong>
          <div style="font-size: 11px; color: #059669;">Assign this email directly to event</div>
        </div>
        <span>Custom</span>
      </div>
    `;
  }

  if (filtered.length === 0 && !html) {
    html = `<div style="padding: 12px; color: #64748b; font-size: 13px; text-align: center;">No faculty found matching "${escapeHtml(query)}". Type an email to assign directly.</div>`;
  } else {
    filtered.slice(0, 10).forEach(f => {
      const isSelected = opsState.selectedFaculty?.email?.toLowerCase() === f.email.toLowerCase();
      html += `
        <div class="ops-faculty-item ${isSelected ? "selected" : ""}" onclick='selectFaculty(${JSON.stringify(f)})'>
          <strong>${escapeHtml(f.facultyName || "Faculty")}</strong>
          <span>${escapeHtml(f.email)}</span>
        </div>
      `;
    });
  }

  dropdown.innerHTML = html;
  dropdown.classList.remove("is-hidden");
}

window.selectFaculty = function(faculty) {
  if (!faculty || !faculty.email) return;
  opsState.selectedFaculty = faculty;

  const emailInput = document.getElementById("opsFacultyEmail");
  const nameInput = document.getElementById("opsFacultyName");
  const idInput = document.getElementById("opsFacultyId");
  const searchInput = document.getElementById("opsFacultySearchInput");
  const quickSelect = document.getElementById("opsFacultyQuickSelect");
  const chip = document.getElementById("opsFacultySelectedBadge");
  const chipName = document.getElementById("opsFacultyChipName");
  const chipEmail = document.getElementById("opsFacultyChipEmail");
  const status = document.getElementById("opsFacultySelectedStatus");
  const dropdown = document.getElementById("opsFacultyDropdown");
  const clearBtn = document.getElementById("opsFacultyClearBtn");

  if (emailInput) emailInput.value = faculty.email;
  if (nameInput) nameInput.value = faculty.facultyName || "";
  if (idInput) idInput.value = faculty.facultyCBSid || faculty.facultyId || "";

  if (chipName) chipName.textContent = faculty.facultyName || faculty.email;
  if (chipEmail) chipEmail.textContent = faculty.email;
  if (chip) chip.classList.remove("is-hidden");
  if (status) status.style.display = "inline";

  if (quickSelect) quickSelect.value = faculty.email;
  if (searchInput) searchInput.value = "";
  if (dropdown) dropdown.classList.add("is-hidden");
  if (clearBtn) clearBtn.classList.remove("is-hidden");
};

window.clearFacultySelection = function() {
  opsState.selectedFaculty = null;
  const emailInput = document.getElementById("opsFacultyEmail");
  const nameInput = document.getElementById("opsFacultyName");
  const idInput = document.getElementById("opsFacultyId");
  const searchInput = document.getElementById("opsFacultySearchInput");
  const quickSelect = document.getElementById("opsFacultyQuickSelect");
  const chip = document.getElementById("opsFacultySelectedBadge");
  const status = document.getElementById("opsFacultySelectedStatus");
  const dropdown = document.getElementById("opsFacultyDropdown");
  const clearBtn = document.getElementById("opsFacultyClearBtn");

  if (emailInput) emailInput.value = "";
  if (nameInput) nameInput.value = "";
  if (idInput) idInput.value = "";
  if (chip) chip.classList.add("is-hidden");
  if (status) status.style.display = "none";
  if (quickSelect) quickSelect.value = "";
  if (searchInput) searchInput.value = "";
  if (dropdown) dropdown.classList.add("is-hidden");
  if (clearBtn) clearBtn.classList.add("is-hidden");
};

// ============================================================================
// Section: YouTube Studio Native Schedule Picker (Calendar & Time Dropdown)
// ============================================================================
class YouTubeSchedulePicker {
  constructor(blockEl) {
    this.blockEl = blockEl;
    const targetInputId = blockEl.dataset.ytScheduleTarget;
    this.targetInput = targetInputId ? document.getElementById(targetInputId) : blockEl.querySelector("input[type='hidden']");
    this.dateAnchor = blockEl.querySelector(".yt-date-picker-anchor");
    this.dateBox = blockEl.querySelector(".yt-date-box");
    this.dateLabel = blockEl.querySelector(".yt-date-label");
    this.calendarDropdown = blockEl.querySelector(".yt-calendar-dropdown");

    this.timeAnchor = blockEl.querySelector(".yt-time-picker-anchor");
    this.timeBox = blockEl.querySelector(".yt-time-box");
    this.timeInput = blockEl.querySelector(".yt-time-input");
    this.timeDropdown = blockEl.querySelector(".yt-time-dropdown");

    this.relativeNote = blockEl.querySelector(".yt-relative-schedule-text");
    this.tzBadge = blockEl.querySelector(".yt-tz-badge");

    const initialDate = this.parseDateFromInput() || new Date(Date.now() + 30 * 60 * 1000);
    this.selectedDate = new Date(initialDate);
    this.viewingYear = this.selectedDate.getFullYear();
    this.viewingMonth = this.selectedDate.getMonth();

    this.initTzBadge();
    this.renderTimeDropdownItems();
    this.syncUI();
    this.bindEvents();
  }

  initTzBadge() {
    if (!this.tzBadge) return;
    try {
      const now = new Date();
      const offsetMin = -now.getTimezoneOffset();
      const sign = offsetMin >= 0 ? "+" : "-";
      const h = Math.floor(Math.abs(offsetMin) / 60);
      const m = Math.abs(offsetMin) % 60;
      const offsetStr = `GMT${sign}${h}${m > 0 ? `:${String(m).padStart(2, "0")}` : ""}`;
      const tzName = Intl.DateTimeFormat().resolvedOptions().timeZone || "Asia/Kolkata";
      const friendlyName = tzName.includes("Calcutta") || tzName.includes("Kolkata") ? "India Standard Time" : tzName.replace(/_/g, " ");
      this.tzBadge.textContent = `🌐 ${friendlyName} (${offsetStr})`;
    } catch (e) {
      // retain default
    }
  }

  parseDateFromInput() {
    if (!this.targetInput || !this.targetInput.value) return null;
    const val = this.targetInput.value.trim();
    const m = val.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
    if (m) {
      return new Date(
        parseInt(m[1], 10),
        parseInt(m[2], 10) - 1,
        parseInt(m[3], 10),
        parseInt(m[4], 10),
        parseInt(m[5], 10)
      );
    }
    const d = new Date(val);
    return isNaN(d.getTime()) ? null : d;
  }

  formatDateLabel(d) {
    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    return `${months[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()}`;
  }

  formatTimeLabel(d) {
    let h = d.getHours();
    const m = String(d.getMinutes()).padStart(2, "0");
    const ampm = h >= 12 ? "PM" : "AM";
    h = h % 12;
    h = h ? h : 12;
    return `${h}:${m} ${ampm}`;
  }

  formatLocalISO(d) {
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  updateRelativeText() {
    if (!this.relativeNote) return;
    const diffMs = this.selectedDate.getTime() - Date.now();
    const diffMin = Math.round(diffMs / (60 * 1000));
    if (diffMin < 0) {
      this.relativeNote.textContent = "Scheduled time is in the past";
      this.relativeNote.style.color = "#dc2626";
    } else if (diffMin < 60) {
      this.relativeNote.textContent = `Starts in ${diffMin} minute${diffMin === 1 ? "" : "s"}`;
      this.relativeNote.style.color = "#2563eb";
    } else {
      const diffHrs = Math.floor(diffMin / 60);
      const remMin = diffMin % 60;
      const todayStr = new Date().toDateString();
      const selStr = this.selectedDate.toDateString();
      const timeStr = this.formatTimeLabel(this.selectedDate);
      if (todayStr === selStr) {
        this.relativeNote.textContent = `Starts today at ${timeStr} (in ${diffHrs}h ${remMin}m)`;
      } else {
        const tomorrow = new Date(Date.now() + 86400000);
        if (tomorrow.toDateString() === selStr) {
          this.relativeNote.textContent = `Starts tomorrow at ${timeStr}`;
        } else {
          this.relativeNote.textContent = `Starts on ${this.formatDateLabel(this.selectedDate)} at ${timeStr}`;
        }
      }
      this.relativeNote.style.color = "#2563eb";
    }
  }

  syncUI() {
    if (this.dateLabel) {
      this.dateLabel.textContent = this.formatDateLabel(this.selectedDate);
    }
    if (this.timeInput) {
      this.timeInput.value = this.formatTimeLabel(this.selectedDate);
    }
    if (this.targetInput) {
      this.targetInput.value = this.formatLocalISO(this.selectedDate);
    }
    this.updateRelativeText();
  }

  setDate(newDate, triggerEvent = true) {
    if (!(newDate instanceof Date) || isNaN(newDate.getTime())) return;
    this.selectedDate = new Date(newDate);
    this.viewingYear = this.selectedDate.getFullYear();
    this.viewingMonth = this.selectedDate.getMonth();
    this.syncUI();
    if (triggerEvent && this.targetInput) {
      this.targetInput.dispatchEvent(new Event("change", { bubbles: true }));
      this.targetInput.dispatchEvent(new Event("input", { bubbles: true }));
    }
  }

  syncFromTargetInput() {
    const parsed = this.parseDateFromInput();
    if (parsed && (!this.selectedDate || parsed.getTime() !== this.selectedDate.getTime())) {
      this.selectedDate = parsed;
      this.viewingYear = this.selectedDate.getFullYear();
      this.viewingMonth = this.selectedDate.getMonth();
      this.syncUI();
    }
  }

  renderCalendar() {
    if (!this.calendarDropdown) return;
    const headerTitle = this.formatDateLabel(this.selectedDate);

    // Month 1
    const y1 = this.viewingYear;
    const m1 = this.viewingMonth;
    const m1Date = new Date(y1, m1, 1);
    const monthNames = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"];
    const m1Badge = `${monthNames[m1]} ${y1}`;
    const m1DaysInMonth = new Date(y1, m1 + 1, 0).getDate();
    const m1FirstDay = m1Date.getDay();

    // Month 2 (next month preview)
    let y2 = y1;
    let m2 = m1 + 1;
    if (m2 > 11) {
      m2 = 0;
      y2++;
    }
    const m2Badge = `${monthNames[m2]} ${y2}`;
    const m2DaysInMonth = new Date(y2, m2 + 1, 0).getDate();
    const m2FirstDay = new Date(y2, m2, 1).getDay();

    const today = new Date();
    const isToday = (y, m, d) => today.getFullYear() === y && today.getMonth() === m && today.getDate() === d;
    const isSelected = (y, m, d) => this.selectedDate.getFullYear() === y && this.selectedDate.getMonth() === m && this.selectedDate.getDate() === d;

    const buildDaysGrid = (year, month, firstDay, daysCount) => {
      let html = '<div class="yt-cal-days-grid">';
      for (let i = 0; i < firstDay; i++) {
        html += '<div class="yt-cal-day empty"></div>';
      }
      for (let d = 1; d <= daysCount; d++) {
        const selClass = isSelected(year, month, d) ? " selected" : "";
        const todClass = isToday(year, month, d) ? " today" : "";
        html += `<div class="yt-cal-day${selClass}${todClass}" data-cal-year="${year}" data-cal-month="${month}" data-cal-day="${d}">${d}</div>`;
      }
      html += "</div>";
      return html;
    };

    this.calendarDropdown.innerHTML = `
      <div class="yt-cal-top-bar">
        <div class="yt-cal-header-title">${headerTitle}</div>
        <div class="yt-cal-arrows">
          <button type="button" class="yt-cal-arrow-btn yt-cal-prev-btn" title="Previous month">‹</button>
          <button type="button" class="yt-cal-arrow-btn yt-cal-next-btn" title="Next month">›</button>
        </div>
      </div>
      <div class="yt-cal-weekdays">
        <span>S</span><span>M</span><span>T</span><span>W</span><span>T</span><span>F</span><span>S</span>
      </div>
      <div class="yt-cal-month-badge">${m1Badge}</div>
      ${buildDaysGrid(y1, m1, m1FirstDay, m1DaysInMonth)}
      <div class="yt-cal-month-badge" style="margin-top: 14px;">${m2Badge}</div>
      ${buildDaysGrid(y2, m2, m2FirstDay, m2DaysInMonth)}
    `;

    // Hook arrows
    this.calendarDropdown.querySelector(".yt-cal-prev-btn")?.addEventListener("click", (e) => {
      e.stopPropagation();
      this.viewingMonth--;
      if (this.viewingMonth < 0) {
        this.viewingMonth = 11;
        this.viewingYear--;
      }
      this.renderCalendar();
    });

    this.calendarDropdown.querySelector(".yt-cal-next-btn")?.addEventListener("click", (e) => {
      e.stopPropagation();
      this.viewingMonth++;
      if (this.viewingMonth > 11) {
        this.viewingMonth = 0;
        this.viewingYear++;
      }
      this.renderCalendar();
    });

    // Hook day clicks
    this.calendarDropdown.querySelectorAll(".yt-cal-day:not(.empty)").forEach((dayEl) => {
      dayEl.addEventListener("click", (e) => {
        e.stopPropagation();
        const y = parseInt(dayEl.dataset.calYear, 10);
        const m = parseInt(dayEl.dataset.calMonth, 10);
        const d = parseInt(dayEl.dataset.calDay, 10);
        this.selectedDate.setFullYear(y, m, d);
        this.syncUI();
        if (this.targetInput) {
          this.targetInput.dispatchEvent(new Event("change", { bubbles: true }));
          this.targetInput.dispatchEvent(new Event("input", { bubbles: true }));
        }
        this.closeCalendar();
      });
    });
  }

  renderTimeDropdownItems() {
    if (!this.timeDropdown) return;
    let html = "";
    for (let h = 0; h < 24; h++) {
      for (let m = 0; m < 60; m += 15) {
        const dummyDate = new Date(2026, 0, 1, h, m);
        const label = this.formatTimeLabel(dummyDate);
        html += `<div class="yt-time-item" data-hour="${h}" data-minute="${m}">${label}</div>`;
      }
    }
    this.timeDropdown.innerHTML = html;

    this.timeDropdown.querySelectorAll(".yt-time-item").forEach((item) => {
      item.addEventListener("click", (e) => {
        e.stopPropagation();
        const h = parseInt(item.dataset.hour, 10);
        const m = parseInt(item.dataset.minute, 10);
        this.selectedDate.setHours(h, m, 0, 0);
        this.syncUI();
        if (this.targetInput) {
          this.targetInput.dispatchEvent(new Event("change", { bubbles: true }));
          this.targetInput.dispatchEvent(new Event("input", { bubbles: true }));
        }
        this.closeTimeDropdown();
      });
    });
  }

  openCalendar() {
    this.closeTimeDropdown();
    this.viewingYear = this.selectedDate.getFullYear();
    this.viewingMonth = this.selectedDate.getMonth();
    this.renderCalendar();
    this.calendarDropdown?.classList.remove("is-hidden");
    this.dateBox?.classList.add("active");
  }

  closeCalendar() {
    this.calendarDropdown?.classList.add("is-hidden");
    this.dateBox?.classList.remove("active");
  }

  openTimeDropdown() {
    this.closeCalendar();
    if (!this.timeDropdown) return;
    const curH = this.selectedDate.getHours();
    const curM = this.selectedDate.getMinutes();

    let closestItem = null;
    let minDiff = Infinity;
    this.timeDropdown.querySelectorAll(".yt-time-item").forEach((item) => {
      const h = parseInt(item.dataset.hour, 10);
      const m = parseInt(item.dataset.minute, 10);
      const diff = Math.abs((h * 60 + m) - (curH * 60 + curM));
      item.classList.toggle("selected", h === curH && m === curM);
      if (diff < minDiff) {
        minDiff = diff;
        closestItem = item;
      }
    });

    this.timeDropdown.classList.remove("is-hidden");
    this.timeBox?.classList.add("active");

    const targetScroll = this.timeDropdown.querySelector(".yt-time-item.selected") || closestItem;
    if (targetScroll) {
      requestAnimationFrame(() => {
        this.timeDropdown.scrollTop = targetScroll.offsetTop - (this.timeDropdown.clientHeight / 2) + (targetScroll.clientHeight / 2);
      });
    }
  }

  closeTimeDropdown() {
    this.timeDropdown?.classList.add("is-hidden");
    this.timeBox?.classList.remove("active");
  }

  parseUserTime(str) {
    if (!str) return null;
    const clean = str.trim().toLowerCase();
    const m = clean.match(/^(\d{1,2})(?::(\d{1,2}))?\s*(am|pm)?$/);
    if (!m) return null;
    let hours = parseInt(m[1], 10);
    let minutes = m[2] !== undefined ? parseInt(m[2], 10) : 0;
    const mer = m[3];
    if (minutes < 0 || minutes > 59) return null;
    if (mer) {
      if (hours < 1 || hours > 12) return null;
      if (mer === "am") {
        if (hours === 12) hours = 0;
      } else if (mer === "pm") {
        if (hours < 12) hours += 12;
      }
    } else {
      if (hours < 0 || hours > 23) return null;
    }
    return { hours, minutes };
  }

  commitTimeInput() {
    if (!this.timeInput) return;
    const parsed = this.parseUserTime(this.timeInput.value);
    if (parsed) {
      this.selectedDate.setHours(parsed.hours, parsed.minutes, 0, 0);
      this.syncUI();
      if (this.targetInput) {
        this.targetInput.dispatchEvent(new Event("change", { bubbles: true }));
        this.targetInput.dispatchEvent(new Event("input", { bubbles: true }));
      }
    } else {
      this.timeInput.value = this.formatTimeLabel(this.selectedDate);
    }
  }

  bindEvents() {
    this.dateBox?.addEventListener("click", (e) => {
      e.stopPropagation();
      if (this.calendarDropdown?.classList.contains("is-hidden")) {
        this.openCalendar();
      } else {
        this.closeCalendar();
      }
    });

    this.dateBox?.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        this.openCalendar();
      }
    });

    this.timeBox?.addEventListener("click", (e) => {
      e.stopPropagation();
      if (e.target === this.timeInput) {
        this.openTimeDropdown();
      } else {
        if (this.timeDropdown?.classList.contains("is-hidden")) {
          this.openTimeDropdown();
        } else {
          this.closeTimeDropdown();
        }
      }
    });

    this.timeInput?.addEventListener("focus", () => {
      this.openTimeDropdown();
    });

    this.timeInput?.addEventListener("input", () => {
      const parsed = this.parseUserTime(this.timeInput.value);
      if (parsed && this.timeDropdown && !this.timeDropdown.classList.contains("is-hidden")) {
        let closestItem = null;
        let minDiff = Infinity;
        this.timeDropdown.querySelectorAll(".yt-time-item").forEach((item) => {
          const h = parseInt(item.dataset.hour, 10);
          const m = parseInt(item.dataset.minute, 10);
          const diff = Math.abs((h * 60 + m) - (parsed.hours * 60 + parsed.minutes));
          item.classList.toggle("selected", diff === 0);
          if (diff < minDiff) {
            minDiff = diff;
            closestItem = item;
          }
        });
        if (closestItem) {
          this.timeDropdown.scrollTop = closestItem.offsetTop - (this.timeDropdown.clientHeight / 2) + (closestItem.clientHeight / 2);
        }
      }
    });

    this.timeInput?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        this.commitTimeInput();
        this.closeTimeDropdown();
        this.timeInput.blur();
      } else if (e.key === "Escape") {
        this.closeTimeDropdown();
      }
    });

    this.timeInput?.addEventListener("blur", () => {
      setTimeout(() => {
        this.commitTimeInput();
      }, 150);
    });

    document.addEventListener("click", (e) => {
      if (!this.blockEl.contains(e.target)) {
        this.closeCalendar();
        this.closeTimeDropdown();
      }
    });

    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        this.closeCalendar();
        this.closeTimeDropdown();
      }
    });

    this.targetInput?.addEventListener("change", () => {
      this.syncFromTargetInput();
    });
  }
}

window.ytSchedulePickers = {};

function initYouTubeSchedulePickers() {
  document.querySelectorAll(".yt-schedule-block").forEach((blockEl) => {
    const targetId = blockEl.dataset.ytScheduleTarget || blockEl.id;
    if (!window.ytSchedulePickers[targetId]) {
      window.ytSchedulePickers[targetId] = new YouTubeSchedulePicker(blockEl);
    } else {
      window.ytSchedulePickers[targetId].syncFromTargetInput();
    }
  });
}

function setDefaultEventTimes() {
  const startInput = document.getElementById("opsEventStartTime");
  const endInput = document.getElementById("opsEventEndTime");
  if (!startInput) return;
  const now = new Date();
  const start = new Date(now.getTime() + 30 * 60 * 1000);
  const end = new Date(now.getTime() + 90 * 60 * 1000);

  const formatLocalISO = (d) => {
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };

  startInput.value = formatLocalISO(start);
  startInput.dispatchEvent(new Event("change"));
  if (window.ytSchedulePickers["opsEventStartTime"]) {
    window.ytSchedulePickers["opsEventStartTime"].syncFromTargetInput();
  }
  if (endInput) endInput.value = formatLocalISO(end);
}

function switchOpsMode(mode) {
  opsState.activeMode = mode;
  const hubView = document.getElementById("opsHubView");
  const eventView = document.getElementById("opsEventView");
  const videoView = document.getElementById("opsVideoView");
  const shortsView = document.getElementById("opsShortsView");
  const backBtn = document.getElementById("opsBackToHubBtn");

  const isHub = mode === "hub";
  backBtn?.classList.toggle("is-hidden", isHub);

  hubView?.classList.toggle("is-hidden", !isHub);
  eventView?.classList.toggle("is-hidden", mode !== "event");
  videoView?.classList.toggle("is-hidden", mode !== "video");
  shortsView?.classList.toggle("is-hidden", mode !== "shorts");

  initYouTubeSchedulePickers();

  populateOpsChannels();
  if (mode === "event") {
    const sel = document.getElementById("opsEventChannelSelect");
    if (sel && sel.value) onOpsChannelChange("event", sel.value);
  } else if (mode === "video") {
    const sel = document.getElementById("opsVideoChannelSelect");
    if (sel && sel.value) onOpsChannelChange("video", sel.value);
  } else if (mode === "shorts") {
    const sel = document.getElementById("opsShortsChannelSelect");
    if (sel && sel.value) onOpsChannelChange("shorts", sel.value);
  }
}

async function populateOpsChannels() {
  let channels = (state.channels || []).filter((c) => c.id !== "all-in-one");
  if (channels.length === 0) {
    try {
      const res = await fetch("/api/channels");
      const data = await res.json();
      if (data && data.channels) {
        channels = data.channels.filter((c) => c.id !== "all-in-one");
      }
    } catch (e) {
      console.warn("Failed to fetch channels for ops:", e.message);
    }
  }

  const channelCountEl = document.getElementById("opsChannelCount");
  if (channelCountEl) {
    channelCountEl.textContent = channels.length;
  }
  const channelCountBadge = document.getElementById("opsChannelCountBadge");
  if (channelCountBadge) {
    channelCountBadge.textContent = channels.length > 0
      ? `${channels.length} Connected Channel${channels.length === 1 ? "" : "s"}`
      : "No Channels Connected";
  }

  const selects = [
    document.getElementById("opsEventChannelSelect"),
    document.getElementById("opsVideoChannelSelect"),
    document.getElementById("opsShortsChannelSelect"),
  ];

  selects.forEach((sel) => {
    if (!sel) return;
    const currentVal = sel.value;
    const options = channels.map((c) => {
      const name = c.name || c.title || c.channelTitle || "Channel";
      return `<option value="${escapeHtml(c.id)}">${escapeHtml(name)}</option>`;
    });

    sel.innerHTML = `<option value="">-- Select Connected Channel --</option>` + options.join("");
    if (currentVal && channels.some((c) => c.id === currentVal)) {
      sel.value = currentVal;
    } else if (channels.length > 0 && !sel.value) {
      sel.value = channels[0].id;
    }
  });
}

async function onOpsChannelChange(mode, channelId) {
  if (!channelId) return;

  try {
    let defaults = opsState.cachedDefaults[channelId];
    if (!defaults) {
      const res = await fetch(`/api/youtube-ops/defaults/${channelId}`);
      defaults = await res.json();
      opsState.cachedDefaults[channelId] = defaults;
    }

    if (mode === "event") {
      const courseInput = document.getElementById("opsEventCourseInput");
      const descInput = document.getElementById("opsEventDescInput");
      const tagsInput = document.getElementById("opsEventTagsInput");

      if (courseInput && defaults.courseCode) courseInput.value = defaults.courseCode;
      if (descInput) descInput.value = defaults.defaultDescription || "";
      if (tagsInput && defaults.defaultTags) tagsInput.value = defaults.defaultTags.join(", ");

      loadOpsPlaylists(channelId);
      if (opsState.eventSubmode === "reuse") {
        loadExistingEventsForReuse(channelId);
      }
    } else if (mode === "video") {
      const descInput = document.getElementById("opsVideoDescInput");
      const tagsInput = document.getElementById("opsVideoTagsInput");
      const channelMockup = document.getElementById("opsVideoMockupChannel");

      if (descInput) descInput.value = defaults.defaultDescription || "";
      if (tagsInput && defaults.defaultTags) tagsInput.value = defaults.defaultTags.join(", ");
      if (channelMockup) channelMockup.textContent = `${defaults.channelName || "Channel"} • Just now`;
    } else if (mode === "shorts") {
      const descInput = document.getElementById("opsShortsDescInput");
      const tagsInput = document.getElementById("opsShortsTagsInput");
      const channelMockup = document.getElementById("opsShortsMockupChannel");

      if (descInput) descInput.value = defaults.defaultDescription || "";
      if (tagsInput && defaults.defaultTags) tagsInput.value = defaults.defaultTags.join(", ");
      if (channelMockup) channelMockup.textContent = `@${(defaults.channelName || "channel").replace(/\s+/g, "").toLowerCase()} • Public`;
    }
  } catch (err) {
    console.warn("Failed to load channel ops defaults:", err.message);
  }
}

async function loadOpsPlaylists(channelId) {
  const plSelect = document.getElementById("opsEventPlaylistSelect");
  if (!plSelect) return;

  plSelect.innerHTML = `<option value="">Loading playlists...</option>`;

  try {
    let playlists = opsState.cachedPlaylists[channelId];
    if (!playlists) {
      const res = await fetch(`/api/youtube-ops/playlists/${channelId}`);
      const data = await res.json();
      playlists = data.playlists || [];
      opsState.cachedPlaylists[channelId] = playlists;
    }

    if (playlists.length === 0) {
      plSelect.innerHTML = `<option value="">Select Playlist</option>`;
      return;
    }

    plSelect.innerHTML = `<option value="">Select Playlist</option>` +
      playlists.map((p) => `<option value="${escapeHtml(p.id)}">${escapeHtml(p.title)}</option>`).join("");

    if (opsState.pendingPlaylistId) {
      plSelect.value = opsState.pendingPlaylistId;
      opsState.pendingPlaylistId = null;
    }
  } catch (err) {
    plSelect.innerHTML = `<option value="">-- Could not load playlists --</option>`;
  }
}

async function loadExistingEventsForReuse(channelId, forceRefresh = false) {
  const reuseSelect = document.getElementById("opsReuseEventSelect");
  const fetchBtn = document.getElementById("opsFetchExistingEventsBtn");
  if (!reuseSelect) return;
  if (!channelId) {
    reuseSelect.innerHTML = `<option value="">Select Existing Event</option>`;
    return;
  }

  if (fetchBtn) {
    fetchBtn.disabled = true;
    fetchBtn.textContent = "⏳ Fetching...";
  }

  reuseSelect.innerHTML = `<option value="">Loading broadcasts for channel...</option>`;
  reuseSelect.disabled = true;

  try {
    let events = opsState.cachedChannelEvents[channelId];
    if (!events || forceRefresh) {
      const res = await fetch(`/api/youtube-ops/events/${channelId}`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Failed to load events");
      events = data.events || [];
      opsState.cachedChannelEvents[channelId] = events;
    }

    if (events.length === 0) {
      reuseSelect.innerHTML = `<option value="">-- No broadcasts found on this channel --</option>`;
      reuseSelect.disabled = false;
      return;
    }

    let optsHtml = `<option value="">Select Existing Event</option>`;
    events.forEach((ev) => {
      optsHtml += `<option value="${escapeHtml(ev.eventId)}">${escapeHtml(ev.title)}</option>`;
    });

    reuseSelect.innerHTML = optsHtml;
    reuseSelect.disabled = false;
  } catch (err) {
    console.error("Error fetching channel broadcasts:", err);
    reuseSelect.innerHTML = `<option value="">Failed to load events: ${escapeHtml(err.message)}</option>`;
    reuseSelect.disabled = false;
  } finally {
    if (fetchBtn) {
      fetchBtn.disabled = false;
      fetchBtn.textContent = "Fetch Existing Events";
    }
  }
}

function onReuseEventSelected(eventId) {
  if (!eventId) return;
  const chSel = document.getElementById("opsEventChannelSelect");
  const channelId = chSel?.value;
  const events = opsState.cachedChannelEvents[channelId] || [];
  const ev = events.find((item) => item.eventId === eventId);
  if (!ev) return;

  // 1. Pre-Title
  const titleInput = document.getElementById("opsEventTitleInput");
  if (titleInput) {
    titleInput.value = ev.title || "";
    titleInput.dispatchEvent(new Event("input"));
  }

  // 2. Pre-Description
  const descInput = document.getElementById("opsEventDescInput");
  if (descInput) {
    descInput.value = ev.description || "";
  }

  // 3. Pre-Tags
  const tagsInput = document.getElementById("opsEventTagsInput");
  if (tagsInput) {
    const rawTags = ev.tags;
    tagsInput.value = Array.isArray(rawTags) ? rawTags.join(", ") : (rawTags || "");
  }

  // 4. Pre-Visibility
  const privacySelect = document.getElementById("opsEventPrivacySelect");
  if (privacySelect && ev.privacyStatus) {
    privacySelect.value = ev.privacyStatus;
  }

  // 5. Pre-Playlist
  const plSelect = document.getElementById("opsEventPlaylistSelect");
  if (plSelect) {
    if (ev.playlistId) {
      plSelect.value = ev.playlistId;
      if (plSelect.value !== ev.playlistId) {
        opsState.pendingPlaylistId = ev.playlistId;
      }
    } else {
      plSelect.value = "";
    }
  }

  // 6. Pre-Thumbnail
  const thumbWrap = document.getElementById("opsEventThumbPreviewWrap");
  const thumbImg = document.getElementById("opsEventThumbImg");
  const thumbPrompt = document.getElementById("opsEventThumbPrompt");
  const thumbFileInput = document.getElementById("opsEventThumbFile");
  const thumbText = document.querySelector("#opsEventThumbDropzone .ops-clean-thumb-text");

  if (ev.thumbnailUrl) {
    opsState.eventExistingThumbUrl = ev.thumbnailUrl;
    opsState.eventThumbBase64 = null;
    if (thumbFileInput) thumbFileInput.value = "";
    if (thumbImg) thumbImg.src = ev.thumbnailUrl;
    if (thumbWrap) thumbWrap.classList.remove("is-hidden");
    if (thumbPrompt) thumbPrompt.classList.add("is-hidden");
    if (thumbText) thumbText.textContent = "Using original event thumbnail";
  }

  // 7. Pre-Faculty
  if (ev.facultyEmail) {
    selectFaculty({
      email: ev.facultyEmail,
      facultyName: ev.facultyName || ev.facultyEmail,
      facultyCBSid: ev.facultyId || "",
    });
  } else if (ev.title && opsState.facultyList.length > 0) {
    const titleLower = ev.title.toLowerCase();
    const match = opsState.facultyList.find((f) => {
      const cleanName = (f.facultyName || "").toLowerCase().replace(/\(.*?\)/g, "").trim();
      return cleanName && cleanName.length > 2 && titleLower.includes(cleanName);
    });
    if (match) {
      selectFaculty(match);
    }
  }

  // 8. New Scheduled Start Time
  setDefaultEventTimes();
}

function setupDescDefaultButtons(mode) {
  const prefix = mode === "event" ? "opsEvent" : mode === "video" ? "opsVideo" : "opsShorts";
  const resetBtn = document.getElementById(`${prefix}ResetDescBtn`);
  const saveBtn = document.getElementById(`${prefix}SaveDescBtn`);
  const chSelect = document.getElementById(`${prefix}ChannelSelect`);
  const descInput = document.getElementById(`${prefix}DescInput`);
  const tagsInput = document.getElementById(`${prefix}TagsInput`);

  if (resetBtn) {
    resetBtn.addEventListener("click", () => {
      const chId = chSelect?.value;
      if (!chId) {
        alert("Please select a channel first.");
        return;
      }
      const defaults = opsState.cachedDefaults[chId];
      if (defaults && defaults.defaultDescription) {
        if (descInput) descInput.value = defaults.defaultDescription;
        if (defaults.defaultTags && tagsInput) tagsInput.value = defaults.defaultTags.join(", ");
      } else {
        onOpsChannelChange(mode, chId);
      }
    });
  }

  if (saveBtn) {
    saveBtn.addEventListener("click", async () => {
      const chId = chSelect?.value;
      if (!chId) {
        alert("Please select a channel first.");
        return;
      }
      const defaultDescription = descInput?.value || "";
      const defaultTags = tagsInput?.value || "";

      try {
        saveBtn.textContent = "💾 Saving...";
        saveBtn.disabled = true;
        const res = await fetch(`/api/youtube-ops/defaults/${chId}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ defaultDescription, defaultTags }),
        });
        const data = await res.json();
        if (data.success) {
          if (!opsState.cachedDefaults[chId]) opsState.cachedDefaults[chId] = {};
          opsState.cachedDefaults[chId].defaultDescription = defaultDescription;
          opsState.cachedDefaults[chId].defaultTags = defaultTags.split(",").map((t) => t.trim()).filter(Boolean);
          alert("✓ Channel default description saved successfully!");
        } else {
          alert("Error: " + (data.error || "Failed to save default."));
        }
      } catch (e) {
        alert("Error: " + e.message);
      } finally {
        saveBtn.textContent = "💾 Save as Default";
        saveBtn.disabled = false;
      }
    });
  }
}

function setupTitleCounterAndMockup(inputId, countId, mockupId, defaultText) {
  const input = document.getElementById(inputId);
  const count = document.getElementById(countId);
  const mockup = mockupId ? document.getElementById(mockupId) : null;

  if (!input) return;

  input.addEventListener("input", () => {
    const val = input.value;
    if (count) count.textContent = `${val.length} / 100`;
    if (mockup) mockup.textContent = val.trim() || defaultText;
  });
}

function setupVideoFilePicker() {
  const dropzone = document.getElementById("opsVideoDropzone");
  const fileInput = document.getElementById("opsVideoFileInput");
  const prompt = document.getElementById("opsVideoDropPrompt");
  const previewWrap = document.getElementById("opsVideoPreviewWrap");
  const player = document.getElementById("opsVideoPlayer");
  const changeBtn = document.getElementById("opsVideoChangeFileBtn");
  const nameEl = document.getElementById("opsVideoMetaName");
  const sizeEl = document.getElementById("opsVideoMetaSize");
  const durationEl = document.getElementById("opsVideoMetaDuration");
  const mockupDuration = document.getElementById("opsVideoMockupDuration");

  if (!dropzone || !fileInput) return;

  function handleVideoFile(file) {
    if (!file) return;
    opsState.videoFile = file;
    nameEl.textContent = file.name;
    sizeEl.textContent = formatBytes(file.size);

    const url = URL.createObjectURL(file);
    player.src = url;

    player.onloadedmetadata = () => {
      opsState.videoDuration = player.duration;
      const formatted = formatDuration(player.duration);
      durationEl.textContent = formatted;
      if (mockupDuration) mockupDuration.textContent = formatted;
    };

    prompt.classList.add("is-hidden");
    previewWrap.classList.remove("is-hidden");
    validateVideoStep1();
  }

  dropzone.addEventListener("click", (e) => {
    if (e.target === changeBtn || e.target.closest("#opsVideoPreviewWrap")) return;
    fileInput.click();
  });

  changeBtn?.addEventListener("click", (e) => {
    e.stopPropagation();
    fileInput.click();
  });

  fileInput.addEventListener("change", (e) => {
    handleVideoFile(e.target.files?.[0]);
  });

  // Drag & drop support
  ["dragenter", "dragover"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.add("drag-over");
    });
  });

  ["dragleave", "dragend"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove("drag-over");
    });
  });

  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropzone.classList.remove("drag-over");
    const file = e.dataTransfer?.files?.[0];
    if (file) {
      handleVideoFile(file);
    }
  });
}

function setupShortsFilePicker() {
  const dropzone = document.getElementById("opsShortsDropzone");
  const fileInput = document.getElementById("opsShortsFileInput");
  const prompt = document.getElementById("opsShortsDropPrompt");
  const previewWrap = document.getElementById("opsShortsPreviewWrap");
  const player = document.getElementById("opsShortsPlayer");
  const changeBtn = document.getElementById("opsShortsChangeFileBtn");
  const nameEl = document.getElementById("opsShortsMetaName");
  const sizeEl = document.getElementById("opsShortsMetaSize");
  const durationEl = document.getElementById("opsShortsMetaDuration");
  const durationAlert = document.getElementById("opsShortsDurationAlert");
  const alertSeconds = document.getElementById("opsShortsAlertSeconds");

  if (!dropzone || !fileInput) return;

  function handleShortsFile(file) {
    if (!file) return;
    opsState.shortsFile = file;
    nameEl.textContent = file.name;
    sizeEl.textContent = formatBytes(file.size);

    const url = URL.createObjectURL(file);
    player.src = url;

    player.onloadedmetadata = () => {
      opsState.shortsDuration = player.duration;
      const formatted = formatDuration(player.duration);
      durationEl.textContent = formatted;

      if (player.duration > 60) {
        durationAlert?.classList.remove("is-hidden");
        if (alertSeconds) alertSeconds.textContent = Math.round(player.duration);
      } else {
        durationAlert?.classList.add("is-hidden");
      }
    };

    prompt.classList.add("is-hidden");
    previewWrap.classList.remove("is-hidden");
    validateShortsStep1();
  }

  dropzone.addEventListener("click", (e) => {
    if (e.target === changeBtn || e.target.closest("#opsShortsPreviewWrap")) return;
    fileInput.click();
  });

  changeBtn?.addEventListener("click", (e) => {
    e.stopPropagation();
    fileInput.click();
  });

  fileInput.addEventListener("change", (e) => {
    handleShortsFile(e.target.files?.[0]);
  });

  // Drag & drop support
  ["dragenter", "dragover"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.add("drag-over");
    });
  });

  ["dragleave", "dragend"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove("drag-over");
    });
  });

  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropzone.classList.remove("drag-over");
    const file = e.dataTransfer?.files?.[0];
    if (file) {
      handleShortsFile(file);
    }
  });
}

function setupThumbPicker(mode) {
  const prefix = mode === "event" ? "opsEvent" : mode === "video" ? "opsVideo" : "opsShorts";
  const dropzone = document.getElementById(`${prefix}ThumbDropzone`);
  const fileInput = document.getElementById(`${prefix}ThumbFile`);
  const prompt = document.getElementById(`${prefix}ThumbPrompt`);
  const previewWrap = document.getElementById(`${prefix}ThumbPreviewWrap`);
  const previewImg = document.getElementById(`${prefix}ThumbImg`);
  const removeBtn = document.getElementById(`${prefix}ThumbRemoveBtn`);
  const mockupImg = document.getElementById(`${prefix}MockupImg`);
  const mockupPlaceholder = document.getElementById(`${prefix}MockupPlaceholder`);

  if (!dropzone || !fileInput) return;

  async function handleThumbFile(file) {
    if (!file) return;
    if (file.type && !file.type.startsWith("image/")) {
      alert("Please upload an image file (PNG, JPG, or WEBP).");
      return;
    }

    const base64 = await fileToBase64(file);
    if (mode === "event") opsState.eventThumbBase64 = base64;
    if (mode === "video") opsState.videoThumbBase64 = base64;
    if (mode === "shorts") opsState.shortsThumbBase64 = base64;

    const thumbText = dropzone.querySelector(".ops-clean-thumb-text");
    if (thumbText) thumbText.textContent = file.name;

    previewImg.src = base64;
    prompt?.classList.add("is-hidden");
    previewWrap?.classList.remove("is-hidden");

    if (mockupImg) {
      mockupImg.src = base64;
      mockupImg.classList.remove("is-hidden");
      mockupPlaceholder?.classList.add("is-hidden");
    }
  }

  dropzone.addEventListener("click", (e) => {
    if (e.target === removeBtn || e.target.closest(".ops-thumb-remove-btn")) return;
    fileInput.click();
  });

  fileInput.addEventListener("change", (e) => {
    handleThumbFile(e.target.files?.[0]);
  });

  // Drag & drop support
  ["dragenter", "dragover"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.add("drag-over");
    });
  });

  ["dragleave", "dragend"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (e) => {
      e.preventDefault();
      e.stopPropagation();
      dropzone.classList.remove("drag-over");
    });
  });

  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    e.stopPropagation();
    dropzone.classList.remove("drag-over");
    const file = e.dataTransfer?.files?.[0];
    if (file) {
      handleThumbFile(file);
    }
  });

  removeBtn?.addEventListener("click", (e) => {
    e.stopPropagation();
    fileInput.value = "";
    if (mode === "event") opsState.eventThumbBase64 = null;
    if (mode === "video") opsState.videoThumbBase64 = null;
    if (mode === "shorts") opsState.shortsThumbBase64 = null;

    const thumbText = dropzone.querySelector(".ops-clean-thumb-text");
    if (thumbText) thumbText.textContent = "No file chosen";

    previewImg.src = "";
    previewWrap?.classList.add("is-hidden");
    prompt?.classList.remove("is-hidden");

    if (mockupImg) {
      mockupImg.src = "";
      mockupImg.classList.add("is-hidden");
      mockupPlaceholder?.classList.remove("is-hidden");
    }
  });
}

function validateVideoStep1() {
  const ch = document.getElementById("opsVideoChannelSelect")?.value;
  const file = opsState.videoFile;
  const nextBtn = document.getElementById("opsVideoStep1NextBtn");
  if (nextBtn) nextBtn.disabled = !(ch && file);
}

function validateShortsStep1() {
  const ch = document.getElementById("opsShortsChannelSelect")?.value;
  const file = opsState.shortsFile;
  const nextBtn = document.getElementById("opsShortsStep1NextBtn");
  if (nextBtn) nextBtn.disabled = !(ch && file);
}

function updateVideoWizardStep(step) {
  opsState.videoStep = step;
  [1, 2, 3].forEach((s) => {
    const stepEl = document.querySelector(`.ops-step[data-video-step="${s}"]`);
    const contentEl = document.getElementById(`opsVideoStep${s}`);
    if (stepEl) {
      stepEl.classList.toggle("active", s === step);
      stepEl.classList.toggle("completed", s < step);
    }
    contentEl?.classList.toggle("is-hidden", s !== step);
  });
}

function updateShortsWizardStep(step) {
  opsState.shortsStep = step;
  [1, 2, 3].forEach((s) => {
    const stepEl = document.querySelector(`.ops-step[data-shorts-step="${s}"]`);
    const contentEl = document.getElementById(`opsShortsStep${s}`);
    if (stepEl) {
      stepEl.classList.toggle("active", s === step);
      stepEl.classList.toggle("completed", s < step);
    }
    contentEl?.classList.toggle("is-hidden", s !== step);
  });
}

function setupVisibilityRadios(prefix) {
  const radioInputs = document.querySelectorAll(`input[name="${prefix}Visibility"]`);
  const scheduleWrap = document.getElementById(`${prefix}SchedulePickerWrap`);
  const mockupVisibility = document.getElementById(`${prefix}MockupVisibility`);

  radioInputs.forEach((radio) => {
    radio.addEventListener("change", () => {
      document.querySelectorAll(`input[name="${prefix}Visibility"]`).forEach((r) => {
        r.closest(".ops-radio-card")?.classList.toggle("active", r.checked);
      });

      const val = radio.value;
      scheduleWrap?.classList.toggle("is-hidden", val !== "schedule");

      if (val === "schedule") {
        initYouTubeSchedulePickers();
        const scheduleInput = document.getElementById(`${prefix}ScheduleTime`);
        if (scheduleInput && !scheduleInput.value) {
          const defaultDate = new Date(Date.now() + 24 * 60 * 60 * 1000);
          defaultDate.setMinutes(Math.ceil(defaultDate.getMinutes() / 15) * 15, 0, 0);
          const pad = (n) => String(n).padStart(2, "0");
          scheduleInput.value = `${defaultDate.getFullYear()}-${pad(defaultDate.getMonth() + 1)}-${pad(defaultDate.getDate())}T${pad(defaultDate.getHours())}:${pad(defaultDate.getMinutes())}`;
          scheduleInput.dispatchEvent(new Event("change"));
          if (window.ytSchedulePickers[`${prefix}ScheduleTime`]) {
            window.ytSchedulePickers[`${prefix}ScheduleTime`].syncFromTargetInput();
          }
        }
      }

      if (mockupVisibility) {
        mockupVisibility.textContent = val === "public" ? "Public" : val === "unlisted" ? "Unlisted" : val === "private" ? "Private" : "Scheduled";
      }
    });
  });
}

function setupVideoWizardNavigation() {
  const step1Next = document.getElementById("opsVideoStep1NextBtn");
  const step2Back = document.getElementById("opsVideoStep2BackBtn");
  const step2Next = document.getElementById("opsVideoStep2NextBtn");
  const step3Back = document.getElementById("opsVideoStep3BackBtn");
  const uploadSubmit = document.getElementById("opsVideoUploadSubmitBtn");

  step1Next?.addEventListener("click", () => updateVideoWizardStep(2));
  step2Back?.addEventListener("click", () => updateVideoWizardStep(1));
  step2Next?.addEventListener("click", () => {
    const title = document.getElementById("opsVideoTitleInput")?.value?.trim();
    if (!title) {
      alert("Please provide a video title before proceeding.");
      return;
    }
    updateVideoWizardStep(3);
  });
  step3Back?.addEventListener("click", () => updateVideoWizardStep(2));

  setupVisibilityRadios("opsVideo");

  uploadSubmit?.addEventListener("click", () => handleExecuteUpload(false));
}

function setupShortsWizardNavigation() {
  const step1Next = document.getElementById("opsShortsStep1NextBtn");
  const step2Back = document.getElementById("opsShortsStep2BackBtn");
  const step2Next = document.getElementById("opsShortsStep2NextBtn");
  const step3Back = document.getElementById("opsShortsStep3BackBtn");
  const uploadSubmit = document.getElementById("opsShortsUploadSubmitBtn");

  step1Next?.addEventListener("click", () => updateShortsWizardStep(2));
  step2Back?.addEventListener("click", () => updateShortsWizardStep(1));
  step2Next?.addEventListener("click", () => {
    const title = document.getElementById("opsShortsTitleInput")?.value?.trim();
    if (!title) {
      alert("Please provide a Short title before proceeding.");
      return;
    }
    updateShortsWizardStep(3);
  });
  step3Back?.addEventListener("click", () => updateShortsWizardStep(2));

  setupVisibilityRadios("opsShorts");

  uploadSubmit?.addEventListener("click", () => handleExecuteUpload(true));
}

async function handleExecuteUpload(isShorts) {
  const prefix = isShorts ? "opsShorts" : "opsVideo";
  const chSelect = document.getElementById(`${prefix}ChannelSelect`);
  const titleInput = document.getElementById(`${prefix}TitleInput`);
  const descInput = document.getElementById(`${prefix}DescInput`);
  const tagsInput = document.getElementById(`${prefix}TagsInput`);
  const submitBtn = document.getElementById(`${prefix}UploadSubmitBtn`);
  const progressCard = document.getElementById(`${prefix}ProgressCard`);
  const progressBar = document.getElementById(`${prefix}ProgressBar`);
  const progressPercent = document.getElementById(`${prefix}ProgressPercent`);
  const progressStatus = document.getElementById(`${prefix}ProgressStatus`);
  const progressSubtext = document.getElementById(`${prefix}ProgressSubtext`);
  const successBox = document.getElementById(`${prefix}SuccessBox`);

  const channelId = chSelect?.value;
  const title = titleInput?.value?.trim();
  const description = descInput?.value || "";
  const tags = tagsInput?.value || "";
  const file = isShorts ? opsState.shortsFile : opsState.videoFile;
  const thumbBase64 = isShorts ? opsState.shortsThumbBase64 : opsState.videoThumbBase64;

  const visibilityRadio = document.querySelector(`input[name="${prefix}Visibility"]:checked`);
  const visibility = visibilityRadio ? visibilityRadio.value : "public";
  const scheduleInput = document.getElementById(`${prefix}ScheduleTime`);
  const publishAt = visibility === "schedule" ? scheduleInput?.value : undefined;

  if (!channelId) {
    alert("Please select a target channel.");
    return;
  }
  if (!title) {
    alert("Please enter a title.");
    return;
  }
  if (!file) {
    alert("Please select a video file.");
    return;
  }
  if (visibility === "schedule" && !publishAt) {
    alert("Please select scheduled release date & time.");
    return;
  }

  try {
    submitBtn.disabled = true;
    progressCard.classList.remove("is-hidden");
    successBox.classList.add("is-hidden");
    progressBar.style.width = "0%";
    progressPercent.textContent = "0%";
    progressStatus.textContent = "Initiating Resumable Upload...";
    progressSubtext.textContent = "Communicating with YouTube Upload servers...";

    // 1. Init upload on server
    const initRes = await fetch("/api/youtube-ops/init-upload", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        channelId,
        title,
        description,
        tags,
        privacyStatus: visibility === "schedule" ? "private" : visibility,
        publishAt,
        videoMimeType: file.type || "video/mp4",
        videoFileSize: file.size,
        isShorts,
      }),
    });

    const initData = await initRes.json();
    if (!initRes.ok || !initData.success || !initData.uploadUrl) {
      throw new Error(initData.error || "Failed to initialize upload session.");
    }

    const uploadUrl = initData.uploadUrl;
    progressStatus.textContent = `Uploading ${isShorts ? "Short" : "video"} to YouTube...`;

    // 2. Upload file in chunks (2 MB each, multiple of 256 KB) to stay well below Vercel's 4.5 MB payload limit
    const CHUNK_SIZE = 2 * 1024 * 1024; // 2,097,152 bytes = 8 * 256 KB
    const totalSize = file.size;
    let offset = 0;
    let uploadedVideo = null;
    const targetUrl = `/api/youtube-ops/upload-proxy?uploadUrl=${encodeURIComponent(uploadUrl)}`;

    while (offset < totalSize) {
      const nextChunkEnd = Math.min(offset + CHUNK_SIZE, totalSize);
      const chunk = file.slice(offset, nextChunkEnd);
      const contentRange = `bytes ${offset}-${nextChunkEnd - 1}/${totalSize}`;

      const chunkNumber = Math.floor(offset / CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(totalSize / CHUNK_SIZE);
      progressStatus.textContent = `Uploading ${isShorts ? "Short" : "video"} to YouTube (Part ${chunkNumber} of ${totalChunks})...`;

      let retries = 0;
      let chunkSuccess = false;

      while (!chunkSuccess && retries < 3) {
        try {
          const chunkResult = await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open("PUT", targetUrl, true);
            xhr.setRequestHeader("Content-Type", file.type || "video/mp4");
            xhr.setRequestHeader("Content-Range", contentRange);

            xhr.upload.onprogress = (e) => {
              if (e.lengthComputable) {
                const currentLoaded = offset + e.loaded;
                const percent = Math.min(99, Math.round((currentLoaded / totalSize) * 100));
                progressBar.style.width = `${percent}%`;
                progressPercent.textContent = `${percent}%`;
                progressSubtext.textContent = `Uploaded ${formatBytes(currentLoaded)} of ${formatBytes(totalSize)}`;
              }
            };

            xhr.onload = () => {
              let parsed = null;
              try {
                parsed = JSON.parse(xhr.responseText);
              } catch (e) {
                parsed = xhr.responseText;
              }

              if (xhr.status === 200 || xhr.status === 201) {
                resolve(parsed);
              } else if (xhr.status === 308) {
                resolve({ incomplete: true, status: 308, range: xhr.getResponseHeader("Range") });
              } else {
                let errorMsg = typeof parsed === "object" && parsed !== null
                  ? parsed.error?.message || parsed.error || JSON.stringify(parsed)
                  : xhr.responseText;
                reject(new Error(`YouTube upload failed (HTTP ${xhr.status}): ${errorMsg}`));
              }
            };

            xhr.onerror = () => reject(new Error("Connection error while streaming video chunk to YouTube."));
            xhr.onabort = () => reject(new Error("Upload aborted."));

            xhr.send(chunk);
          });

          if (chunkResult && chunkResult.incomplete) {
            if (chunkResult.range) {
              const rangeMatch = String(chunkResult.range).match(/bytes=0-(\d+)/);
              if (rangeMatch) {
                offset = parseInt(rangeMatch[1], 10) + 1;
              } else {
                offset = nextChunkEnd;
              }
            } else {
              offset = nextChunkEnd;
            }
            chunkSuccess = true;
          } else if (chunkResult && (chunkResult.id || typeof chunkResult === "object")) {
            uploadedVideo = chunkResult;
            offset = totalSize;
            chunkSuccess = true;
          } else {
            throw new Error("Unexpected response from upload proxy.");
          }
        } catch (chunkErr) {
          retries++;
          if (retries >= 3) {
            throw chunkErr;
          }
          await new Promise((r) => setTimeout(r, 1500));
        }
      }
    }

    const videoId = uploadedVideo?.id || "";

    // 3. Set Custom Thumbnail if provided
    if (thumbBase64 && videoId) {
      progressStatus.textContent = "Setting custom thumbnail...";
      progressSubtext.textContent = "Uploading thumbnail image...";
      try {
        await fetch("/api/youtube-ops/set-thumbnail", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            channelId,
            videoId,
            thumbnailBase64: thumbBase64,
          }),
        });
      } catch (thumbErr) {
        console.warn("Thumbnail upload warning:", thumbErr.message);
      }
    }

    let uploadSucceeded = false;
    // 4. Success UI
    progressBar.style.width = "100%";
    progressPercent.textContent = "100%";
    progressStatus.textContent = "🎉 Processing & Publishing Complete!";
    progressSubtext.textContent = `Successfully uploaded to YouTube.`;

    renderUploadSuccessBox(prefix, videoId, title, isShorts, visibility, publishAt);
    uploadSucceeded = true;

  } catch (err) {
    alert("Upload error: " + err.message);
    progressStatus.textContent = "❌ Upload Failed";
    progressSubtext.textContent = err.message;
  } finally {
    if (!uploadSucceeded) {
      submitBtn.disabled = false;
    }
  }
}

function renderUploadSuccessBox(prefix, videoId, title, isShorts, visibility, publishAt) {
  const successBox = document.getElementById(`${prefix}SuccessBox`);
  if (!successBox) return;

  // Hide the step 3 controls and submit footer to prevent duplicate uploads
  document.querySelector(`#${prefix}Step3 .ops-form-row`)?.classList.add("is-hidden");
  document.getElementById(`${prefix}Step3Footer`)?.classList.add("is-hidden");
  document.getElementById(`${prefix}ProgressCard`)?.classList.add("is-hidden");

  const watchUrl = videoId
    ? (isShorts ? `https://www.youtube.com/shorts/${videoId}` : `https://www.youtube.com/watch?v=${videoId}`)
    : "#";
  const studioUrl = videoId ? `https://studio.youtube.com/video/${videoId}/edit` : "#";

  successBox.innerHTML = `
    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px;">
      <div>
        <h4 style="margin: 0 0 6px; font-size: 18px; font-weight: 700; color: #15803d;">🎉 ${isShorts ? "Short" : "Video"} Uploaded Successfully!</h4>
        <p style="margin: 0; font-size: 13px; color: #166534;">
          <strong>${escapeHtml(title)}</strong> is uploaded and ready on YouTube.
        </p>
      </div>
      <span style="background: #dcfce7; color: #15803d; font-size: 12px; font-weight: 700; padding: 4px 10px; border-radius: 20px;">
        ${visibility === "schedule" ? "SCHEDULED" : visibility.toUpperCase()}
      </span>
    </div>

    ${visibility === "schedule" ? `
      <div style="background: #fff; border: 1px solid #bbf7d0; border-radius: 8px; padding: 12px; margin-bottom: 16px; font-size: 13px; color: #1e293b;">
        ⏰ <strong>Scheduled Release:</strong> ${new Date(publishAt).toLocaleString()} (Video will remain private until this time).
      </div>
    ` : ""}

    ${videoId ? `
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin-bottom: 20px;">
        <div style="background: #fff; border: 1px solid #bbf7d0; border-radius: 8px; padding: 12px;">
          <span style="font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase;">Watch on YouTube</span>
          <div style="margin-top: 4px;">
            <a href="${escapeHtml(watchUrl)}" target="_blank" style="font-size: 13px; font-weight: 600; color: #2563eb; text-decoration: none; word-break: break-all;">
              ${escapeHtml(watchUrl)} ↗
            </a>
          </div>
        </div>

        <div style="background: #fff; border: 1px solid #bbf7d0; border-radius: 8px; padding: 12px;">
          <span style="font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase;">Manage in YouTube Studio</span>
          <div style="margin-top: 4px;">
            <a href="${escapeHtml(studioUrl)}" target="_blank" style="font-size: 13px; font-weight: 600; color: #dc2626; text-decoration: none; word-break: break-all;">
              Open in Studio Editor ↗
            </a>
          </div>
        </div>
      </div>
    ` : ""}

    <div style="display: flex; justify-content: flex-end; gap: 12px; margin-top: 16px;">
      <button type="button" class="connect-button" style="padding: 10px 20px; font-weight: 700; background: var(--green, #15803d);" onclick="resetOpsUploadWizard('${prefix}')">+ Upload Another ${isShorts ? "Short" : "Video"}</button>
    </div>
  `;
  successBox.classList.remove("is-hidden");
  successBox.scrollIntoView({ behavior: "smooth", block: "nearest" });
}

window.resetOpsUploadWizard = function(prefix) {
  // Restore Step 3 form elements and footer
  document.querySelector(`#${prefix}Step3 .ops-form-row`)?.classList.remove("is-hidden");
  document.getElementById(`${prefix}Step3Footer`)?.classList.remove("is-hidden");
  const submitBtn = document.getElementById(`${prefix}UploadSubmitBtn`);
  if (submitBtn) submitBtn.disabled = false;

  const isShorts = prefix === "opsShorts";
  if (isShorts) {
    opsState.shortsFile = null;
    opsState.shortsThumbBase64 = null;
    opsState.shortsStep = 1;
    document.getElementById("opsShortsThumbRemoveBtn")?.click();
    document.getElementById("opsShortsChangeFileBtn")?.click();
    const tInput = document.getElementById("opsShortsTitleInput");
    if (tInput) tInput.value = "";
    const tagsInput = document.getElementById("opsShortsTagsInput");
    if (tagsInput) tagsInput.value = "";
    document.getElementById("opsShortsProgressCard")?.classList.add("is-hidden");
    document.getElementById("opsShortsSuccessBox")?.classList.add("is-hidden");
    updateShortsWizardStep(1);
  } else {
    opsState.videoFile = null;
    opsState.videoThumbBase64 = null;
    opsState.videoStep = 1;
    document.getElementById("opsVideoThumbRemoveBtn")?.click();
    document.getElementById("opsVideoChangeFileBtn")?.click();
    const tInput = document.getElementById("opsVideoTitleInput");
    if (tInput) tInput.value = "";
    const tagsInput = document.getElementById("opsVideoTagsInput");
    if (tagsInput) tagsInput.value = "";
    document.getElementById("opsVideoProgressCard")?.classList.add("is-hidden");
    document.getElementById("opsVideoSuccessBox")?.classList.add("is-hidden");
    updateVideoWizardStep(1);
  }
};

async function handleCreateLiveEvent(e) {
  e.preventDefault();
  const channelId = document.getElementById("opsEventChannelSelect")?.value;
  const title = document.getElementById("opsEventTitleInput")?.value?.trim();
  const startTime = document.getElementById("opsEventStartTime")?.value;
  const endTime = document.getElementById("opsEventEndTime")?.value;
  const privacyStatus = document.getElementById("opsEventPrivacySelect")?.value || "public";
  const playlistId = document.getElementById("opsEventPlaylistSelect")?.value || "";
  const description = document.getElementById("opsEventDescInput")?.value || "";
  const tags = document.getElementById("opsEventTagsInput")?.value || "";
  const submitBtn = document.getElementById("opsCreateEventSubmitBtn");
  const resultBox = document.getElementById("opsEventResultBox");
  const isReuse = opsState.eventSubmode === "reuse";
  const existingEventId = document.getElementById("opsReuseEventSelect")?.value;

  const facultyEmail = document.getElementById("opsFacultyEmail")?.value?.trim() || 
                       document.getElementById("opsFacultySearchInput")?.value?.trim();
  const facultyName = document.getElementById("opsFacultyName")?.value?.trim() || "";
  const facultyId = document.getElementById("opsFacultyId")?.value?.trim() || "";
  const eventType = document.getElementById("opsEventTypeSelect")?.value || "Live Class";

  if (!channelId) {
    alert("Please select a target channel.");
    return;
  }
  if (!facultyEmail) {
    alert("Please select or enter the Faculty Email ID for this event.");
    document.getElementById("opsFacultySearchInput")?.focus();
    return;
  }
  if (isReuse && !existingEventId) {
    alert("Please select an existing broadcast to reuse its stream key.");
    return;
  }
  if (!title) {
    alert("Please enter a broadcast title.");
    return;
  }
  if (!startTime) {
    alert("Please select scheduled start time.");
    return;
  }

  const oldBtnText = submitBtn.textContent;
  submitBtn.disabled = true;
  submitBtn.textContent = isReuse
    ? "⏳ Binding to Existing Stream Key & Scheduling..."
    : "⏳ Creating Event & Generating RTMP Key...";

  const endpoint = isReuse ? "/api/youtube-ops/reuse-event" : "/api/youtube-ops/create-event";
  const payload = {
    channelId,
    title,
    description,
    scheduledStartTime: startTime,
    scheduledEndTime: endTime || undefined,
    privacyStatus,
    tags,
    playlistId: playlistId || undefined,
    thumbnailBase64: opsState.eventThumbBase64 || undefined,
    facultyEmail,
    facultyName,
    facultyId,
    eventType,
  };
  if (isReuse) {
    payload.existingEventId = existingEventId;
    payload.existingThumbUrl = opsState.eventExistingThumbUrl || undefined;
  }

  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await res.json();
    if (!res.ok || !data.success) {
      throw new Error(data.error || "Failed to create YouTube Live event.");
    }

    renderEventSuccessBox(data);
    resultBox.classList.remove("is-hidden");
    resultBox.scrollIntoView({ behavior: "smooth" });

  } catch (err) {
    alert("Error creating event: " + err.message);
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = oldBtnText;
  }
}

function renderEventSuccessBox(data) {
  const resultBox = document.getElementById("opsEventResultBox");
  if (!resultBox) return;

  resultBox.innerHTML = `
    <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 16px; flex-wrap: wrap; gap: 10px;">
      <div>
        <h4 style="margin: 0 0 6px; font-size: 18px; font-weight: 700; color: #15803d;">
          ${data.isReused ? "🎉 Event Reused & Scheduled Successfully!" : "🎉 Live Event Created Successfully!"}
        </h4>
        <p style="margin: 0; font-size: 13px; color: #166534;">
          ${data.isReused 
            ? "Your live broadcast has been scheduled using the original stream key. OBS Studio requires NO reconfiguration!" 
            : "Your live broadcast has been scheduled on YouTube and is ready for OBS streaming."}
        </p>
      </div>
      <div style="display: flex; gap: 6px; align-items: center;">
        ${data.isReused ? `<span style="background: #e0e7ff; color: #3730a3; font-size: 11px; font-weight: 700; padding: 4px 8px; border-radius: 20px;">↻ REUSED STREAM KEY</span>` : ""}
        <span style="background: #dcfce7; color: #15803d; font-size: 12px; font-weight: 700; padding: 4px 10px; border-radius: 20px;">${escapeHtml((data.privacyStatus || "public").toUpperCase())}</span>
      </div>
    </div>

    ${data.facultyEmail ? `
      <div style="background: #f8fafc; border: 1px solid #cbd5e1; border-radius: 8px; padding: 10px 14px; margin-bottom: 16px; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px;">
        <div style="display: flex; align-items: center; gap: 10px;">
          <span style="font-size: 20px;">👨‍🏫</span>
          <div>
            <strong style="font-size: 13px; color: #1e293b;">Assigned Faculty:</strong>
            <span style="font-size: 13px; color: #0284c7; font-weight: 600; margin-left: 4px;">
              ${escapeHtml(data.facultyName ? `${data.facultyName} (${data.facultyEmail})` : data.facultyEmail)}
            </span>
          </div>
        </div>
        <span style="font-size: 11px; background: #e0f2fe; color: #0369a1; padding: 3px 10px; border-radius: 12px; font-weight: 600;">Saved to Faculty Memory</span>
      </div>
    ` : ""}

    <div style="background: #f0fdf4; border: 1.5px solid #86efac; border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 10px;">
      <div style="display: flex; align-items: center; gap: 10px;">
        <span style="font-size: 22px;">📊</span>
        <div>
          <strong style="font-size: 13px; color: #14532d;">Logged to Google Sheet (YT Events by Dashboard)</strong>
          <p style="margin: 2px 0 0; font-size: 12px; color: #166534;">
            ${data.sheetSync?.synced 
              ? "✓ Real-time entry appended to <strong>Sheet1</strong> with faculty email, channel, date, time & stream keys." 
              : "Event metadata recorded and saved. Click to view or verify in the master sheet."}
          </p>
        </div>
      </div>
      <a href="${data.sheetSync?.sheetUrl || 'https://docs.google.com/spreadsheets/d/1-9hSD9ugLV8rrZq8cOdFULSZ_wFUwilP1LmNzGTmxKo/edit?usp=sharing'}" target="_blank" style="display: inline-flex; align-items: center; gap: 6px; background: #15803d; color: #ffffff; font-size: 12px; font-weight: 700; padding: 7px 14px; border-radius: 6px; text-decoration: none;">
        📊 Open Google Sheet ↗
      </a>
    </div>

    ${data.isReused ? `
      <div style="background: #f0fdf4; border: 1px solid #86efac; border-radius: 8px; padding: 12px 16px; margin-bottom: 16px; display: flex; align-items: center; gap: 12px;">
        <span style="font-size: 24px;">⚡</span>
        <div>
          <strong style="color: #166534; font-size: 14px; display: block;">Reused Stream Key (OBS Configuration Unchanged)</strong>
          <span style="color: #15803d; font-size: 12px;">This broadcast is bound to your existing stream key. You do NOT need to update stream credentials in OBS Studio!</span>
        </div>
      </div>
    ` : ""}

    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin-bottom: 20px;">
      <div style="background: #fff; border: 1px solid #bbf7d0; border-radius: 8px; padding: 12px;">
        <span style="font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase;">Direct YouTube Watch Link</span>
        <div style="margin-top: 4px;">
          <a href="${escapeHtml(data.eventLink)}" target="_blank" style="font-size: 13px; font-weight: 600; color: #2563eb; text-decoration: none; word-break: break-all;">
            ${escapeHtml(data.eventLink)} ↗
          </a>
        </div>
      </div>

      <div style="background: #fff; border: 1px solid #bbf7d0; border-radius: 8px; padding: 12px;">
        <span style="font-size: 11px; font-weight: 700; color: #475569; text-transform: uppercase;">Live Control Room (YouTube Studio)</span>
        <div style="margin-top: 4px;">
          <a href="${escapeHtml(data.studioLink)}" target="_blank" style="font-size: 13px; font-weight: 600; color: #dc2626; text-decoration: none; word-break: break-all;">
            Open Studio Live Room ↗
          </a>
        </div>
      </div>
    </div>

    <!-- RTMP OBS Ingestion Panel -->
    <div class="ops-rtmp-panel">
      <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 4px;">
        <span style="font-size: 18px;">📡</span>
        <strong style="font-size: 14px; color: #1e293b;">OBS Studio Stream Credentials</strong>
      </div>
      <p style="margin: 0 0 12px; font-size: 12px; color: #64748b;">
        ${data.isReused 
          ? "Stream key matches your existing OBS profile. Re-copying is only needed if OBS was reset." 
          : "Copy and paste these credentials into OBS Studio &rarr; Settings &rarr; Stream &rarr; Service: Custom."}
      </p>

      <div class="rtmp-row">
        <label>Server / RTMP URL</label>
        <div class="rtmp-input-wrap">
          <input type="text" readonly value="${escapeHtml(data.rtmpUrl || 'rtmp://a.rtmp.youtube.com/live2')}" id="opsRtmpUrlVal" />
          <button type="button" class="mini-button copy-btn" onclick="copyOpsText('opsRtmpUrlVal', this)">📋 Copy URL</button>
        </div>
      </div>

      <div class="rtmp-row">
        <label>Stream Key</label>
        <div class="rtmp-input-wrap">
          <input type="password" readonly value="${escapeHtml(data.rtmpKey || '')}" id="opsRtmpKeyVal" />
          <button type="button" class="mini-button" onclick="toggleOpsKeyVisibility('opsRtmpKeyVal', this)">👁️ Show</button>
          <button type="button" class="mini-button copy-btn" onclick="copyOpsText('opsRtmpKeyVal', this)">📋 Copy Key</button>
        </div>
      </div>
    </div>

    <div style="display: flex; justify-content: flex-end; margin-top: 16px;">
      <button type="button" class="ghost-button" onclick="resetOpsEventForm()">+ Create Another Event</button>
    </div>
  `;
}

window.copyOpsText = function(inputId, btn) {
  const input = document.getElementById(inputId);
  if (input) {
    input.select();
    navigator.clipboard.writeText(input.value);
    const orig = btn.textContent;
    btn.textContent = "✓ Copied!";
    setTimeout(() => { btn.textContent = orig; }, 2000);
  }
};

window.toggleOpsKeyVisibility = function(inputId, btn) {
  const input = document.getElementById(inputId);
  if (input) {
    if (input.type === "password") {
      input.type = "text";
      btn.textContent = "🔒 Hide";
    } else {
      input.type = "password";
      btn.textContent = "👁️ Show";
    }
  }
};

window.resetOpsEventForm = function() {
  const form = document.getElementById("opsCreateEventForm");
  if (form) form.reset();
  const reuseSelect = document.getElementById("opsReuseEventSelect");
  if (reuseSelect) reuseSelect.value = "";
  clearFacultySelection();
  document.getElementById("opsEventThumbRemoveBtn")?.click();
  document.getElementById("opsEventResultBox")?.classList.add("is-hidden");
  setDefaultEventTimes();
};






