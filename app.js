const ranges = {
  month: { label: "This month" },
  "7": { label: "Last 7 days" },
};

const formats = {
  videos: { label: "Videos", color: "#3c6ee8" },
  shorts: { label: "Shorts", color: "#d91632" },
  live: { label: "Live", color: "#0f9f96" },
};

const competitorCategories = ["Testbook", "Teaching", "UGC NET", "CGL", "Odisha", "Bengali", "Marathi", "MPSC", "AE JE", "Bihar", "Banking", "Railways", "UPSC", "Punjab", "Telugu"];
const competitorAutoRefreshMs = 24 * 60 * 60 * 1000; // once a day
let competitorAutoRefreshTimer = null;

const candidateMappings = {
  "Vinayak": [
    "SuperCoaching MPSC by Testbook",
    "Banking Testbook",
    "Railway Testbook"
  ],
  "Mohit": [
    "UPSC PrepLab",
    "Bihar Testbook",
    "Testbook"
  ],
  "Raubinsh": [
    "Odisha Testbook",
    "Odisha Teaching by Testbook"
  ],
  "Saijal": [
    "UGC NET Testbook",
    "Testbook NET JRF",
    "TET PRT Testbook",
    "TGT PGT Testbook",
    "CTET Testbook",
    "Bihar Teaching Exams by Testbook",
    "Assistant Professor & PhD by Testbook"
  ],
  "Aditya": [
    "Testbook Bengali",
    "WBPSC Testbook",
    "Punjab Testbook",
    "SuperCoaching Marathi by Testbook",
    "TET Factory by Testbook",
    "Testbook Telugu"
  ],
  "Vivek": [
    "AE JE Testbook",
    "SSC Testbook",
    "Testbook Tamil"
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
  "Himanshu": [
    "SuperCoaching MPSC by Testbook",
    "SuperCoaching Marathi by Testbook"
  ],
  "Ayush": [
    "Supercoaching Regulatory Bodies by Testbook",
    "Testbook - JAIIB CAIIB"
  ],
  "Atul Sharma": [
    "UPSC PrepLab"
  ],
  "Shubham": [
    "Bihar Testbook",
    "Testbook",
    "Banking Testbook"
  ],
  "Raubnish": [
    "Odisha Testbook",
    "Odisha Teaching by Testbook"
  ],
  "Amit": [
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
    "SSC Testbook"
  ]
};

function getYtmName(channelTitle) {
  const cleanTitle = String(channelTitle || "").trim().toLowerCase();
  for (const [manager, channels] of Object.entries(ytmMappings)) {
    if (channels.some(ch => ch.trim().toLowerCase() === cleanTitle)) {
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

document.querySelector("#connectChannelButton").addEventListener("click", () => {
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
  loadDashboard({ force: true });
});

document.querySelectorAll("[data-view-tab]").forEach((button) => {
  button.addEventListener("click", () => {
    state.activeView = button.dataset.viewTab;
    applyView();
    if (state.activeView === "competitors") {
      enterCompetitorView();
      return;
    }
    stopCompetitorAutoRefresh();
    if (state.activeView === "research") renderResearchView();
    if (state.activeView === "seo") renderSeoAuditView();
    if (state.activeView === "ytm") renderYtmAuditView();
  });
});

rangeSelect.addEventListener("change", (event) => {
  state.activeRange = event.target.value;
  loadDashboard();
});

if (monthSelect) {
  monthSelect.value = state.selectedMonth;
  monthSelect.addEventListener("change", (event) => {
    state.selectedMonth = event.target.value || currentMonthValue();
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

document.querySelector("#suggestTopicsButton")?.addEventListener("click", () => {
  suggestResearchTopics();
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
    const forceQuery = options.force ? "&force=1" : "";
    const report = await api(`/api/dashboard?range=${state.activeRange}${monthQuery}${channelQuery}${forceQuery}`);
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
  if (report.isAllInOne) {
    renderAllInOneDashboard(report);
  } else {
    renderUploadTable(report.series);
    renderViewsSplit(report.totals);
    renderSubscriberContent(report.topContent);
    renderAverageViews(report.totals);
    renderYoutubeSearch(report);
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
  const button = document.createElement("button");
  button.className = `channel-button ${channel.id === state.selectedChannelId ? "active" : ""}`;
  button.innerHTML = `
    <span>
      <strong>${escapeHtml(channel.name)}</strong>
    </span>
  `;
  button.addEventListener("click", () => {
    state.selectedChannelId = channel.id;
    loadDashboard();
  });
  return button;
}

function renderGrowth(totals, comparisonTotals = {}) {
  document.querySelector("#viewsTrend").textContent = Number(totals.organicViews || 0).toLocaleString();
  document.querySelector("#subTrend").textContent = `+${totals.subscribers.toLocaleString()}`;
  document.querySelector("#viewsTrendDetail").textContent = rangeLabel();
  document.querySelector("#subTrendDetail").textContent = rangeLabel();
  renderDelta("#viewsDelta", totals.organicViews, comparisonTotals.organicViews);
  renderDelta("#subDelta", totals.subscribers, comparisonTotals.subscribers);
}

function renderMetrics(totals, comparisonTotals = state.report?.comparisonTotals || {}, isAllInOne = false) {
  if (isAllInOne) {
    document.querySelector("#metric1Label").textContent = "Views gained";
    document.querySelector("#metric2Label").textContent = "Subscribers gained";
    document.querySelector("#metric3Label").textContent = "Shorts views";
    document.querySelector("#metric4Label").textContent = "Video views";
    document.querySelector("#metric5Label").textContent = "Live views";
    document.querySelector("#metric3Detail").textContent = "All authorized channels";
    document.querySelector("#metric4Detail").textContent = "All authorized channels";
    document.querySelector("#metric5Detail").textContent = "All authorized channels";
    document.querySelector("#videoCount").textContent = Number(totals.views.shorts || 0).toLocaleString();
    document.querySelector("#shortCount").textContent = Number(totals.views.videos || 0).toLocaleString();
    document.querySelector("#liveCount").textContent = Number(totals.views.live || 0).toLocaleString();
    renderDelta("#videoDelta", totals.views.shorts, comparisonTotals.views?.shorts);
    renderDelta("#shortDelta", totals.views.videos, comparisonTotals.views?.videos);
    renderDelta("#liveDelta", totals.views.live, comparisonTotals.views?.live);
    return;
  }
  document.querySelector("#metric1Label").textContent = "Views gained";
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
  const visibleSeries = dashboardVisibleSeries(series).slice().reverse();
  document.querySelector("#uploadChart").innerHTML = `
    <div class="publish-row publish-head">
      <span>Date</span>
      <span>Organic views</span>
      <span>Videos</span>
      <span>Shorts</span>
      <span>Live</span>
      <span>Total</span>
      <span>Shares</span>
    </div>
    ${visibleSeries.map((day) => `
      <div class="publish-row">
        <strong>${escapeHtml(day.label)}</strong>
        <span class="organic-cell">${Number(day.organicViews || 0).toLocaleString()}</span>
        <span>${formatKnownCount(day.uploads.videos, day.uploadsKnown !== false)}</span>
        <span>${formatKnownCount(day.uploads.shorts, day.uploadsKnown !== false)}</span>
        <span>${formatKnownCount(day.uploads.live, day.uploadsKnown !== false)}</span>
        <b>${formatKnownCount(day.uploads.total, day.uploadsKnown !== false)}</b>
        <span>${Number(day.shares || 0).toLocaleString()}</span>
      </div>
    `).join("") || `<div class="publish-row empty-row"><strong>No organic/search data in this range yet.</strong></div>`}
  `;
}

function renderViewsSplit(totals) {
  const values = ["shorts", "videos", "live"].map((key) => totals.views[key]);
  const total = Math.max(1, values.reduce((sum, value) => sum + value, 0));
  let cursor = 0;
  const stops = ["shorts", "videos", "live"].map((key) => {
    const start = cursor;
    cursor += (totals.views[key] / total) * 100;
    return `${formats[key].color} ${start}% ${cursor}%`;
  });
  document.querySelector("#viewsDonut").style.background = `conic-gradient(${stops.join(", ")})`;
  document.querySelector("#viewsLegend").innerHTML = ["shorts", "videos", "live"].map((key) => `
    <div class="legend-row">
      <i class="dot ${key === "videos" ? "video" : key === "shorts" ? "short" : "live"}"></i>
      <strong>${formats[key].label}</strong>
      <span>${Math.round((totals.views[key] / total) * 100)}%</span>
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
  renderAllInOneDailyTotals(report.allInOne?.dailyTotals || []);
  renderAllInOneTopContent(report.topContent || []);
  renderChannelRankings("#allInOneOrganicChannels", report.allInOne?.channelRankings?.organicViews || [], "organicViews");
  renderChannelRankings("#allInOneSubscriberChannels", report.allInOne?.channelRankings?.subscribers || [], "subscribers");
}

function renderAllInOneDailyTotals(days) {
  const container = document.querySelector("#allInOneDailyTotals");
  const visibleDays = days
    .filter((day) => Number(day.organicViews || 0) !== 0 || Number(day.subscribers || 0) !== 0)
    .slice()
    .reverse();
  container.innerHTML = `
    <div class="daily-total-row daily-total-head">
      <span>Date</span>
      <span>Total views</span>
      <span>Subscribers</span>
    </div>
    ${visibleDays.map((day) => `
      <div class="daily-total-row">
        <strong>${escapeHtml(day.label)}</strong>
        <span>${Number(day.organicViews || 0).toLocaleString()}</span>
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
          <span>Eng %</span>
        </div>
        ${data.groups.map((group) => `
          <div class="benchmark-row">
            ${benchmarkChannelCell(group)}
            ${benchmarkCell(group.views, leaders.views, group.name)}
            ${benchmarkCell(group.averageViews.shorts, leaders.shorts, group.name)}
            ${benchmarkCell(group.averageViews.videos, leaders.videos, group.name)}
            ${benchmarkCell(group.averageViews.live, leaders.live, group.name)}
            ${benchmarkCell(group.engagement, leaders.engagement, group.name, formatPercentCompact)}
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
  const isAllInOne = state.report?.isAllInOne;
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
          : "Research";
}

function renderResearchView() {
  if (researchKeywordInput) researchKeywordInput.value = state.researchKeyword;
  if (researchRangeSelect) researchRangeSelect.value = state.researchRange;
  renderResearchFilters();
  renderResearchSummary();
  renderResearchResults();
  renderResearchIdeas();
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
        <span>Outlier</span>
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
          <strong>${formatOutlier(item.outlierScore)}</strong>
          <a class="link-chip" href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">YouTube</a>
        </div>
      `).join("")}
    </div>
  `;
}

function renderResearchIdeas() {
  const container = document.querySelector("#researchIdeas");
  if (!container) return;
  if (!state.researchIdeas.length) {
    container.innerHTML = emptyCard("Run research first, then click Suggest topics to turn the winning videos into 5 publishable ideas.");
    return;
  }
  container.innerHTML = state.researchIdeas.map((idea, index) => `
    <article class="idea-card">
      <div class="idea-topline">
        <b>${index + 1}</b>
        <mark class="channel-tag ${ideaFormatClass(idea.format)}">${escapeHtml(idea.format)}</mark>
      </div>
      <strong>${escapeHtml(idea.title)}</strong>
      <p>${escapeHtml(idea.reason)}</p>
    </article>
  `).join("");
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
  state.researchIdeas = [];
  document.querySelector("#researchResults").innerHTML = emptyCard("Loading top videos...");
  document.querySelector("#researchIdeas").innerHTML = emptyCard("Topic ideas will appear here after analysis.");
  
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

async function suggestResearchTopics() {
  if (!state.researchResults.length) {
    document.querySelector("#researchIdeas").innerHTML = emptyCard("Run research first.");
    return;
  }
  document.querySelector("#researchIdeas").innerHTML = emptyCard("Asking Claude for topic ideas...");
  
  const suggestSteps = [
    { time: 0, text: "Sending top video details to AI..." },
    { time: 3, text: "Analyzing search outliers..." },
    { time: 7, text: "Generating topic ideas and formatting recommendations..." }
  ];
  const progressBar = startProgressBar("researchProgressBarContainer", "researchProgressBarFill", "researchProgressBarLabel", suggestSteps);

  try {
    const data = await api("/api/research/suggest", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        keyword: state.researchKeyword,
        range: state.researchRange,
        items: state.researchResults.slice(0, 20),
      }),
    });
    state.researchIdeas = data.ideas || [];
    renderResearchIdeas();
    if (progressBar) progressBar.stop(true, "Suggestions Complete! (100%)", "Suggestions Failed!");
  } catch (error) {
    document.querySelector("#researchIdeas").innerHTML = emptyCard(error.message);
    if (progressBar) progressBar.stop(false, "Suggestions Failed!");
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
  const searchDays = dashboardVisibleSeries(report.series).map((day) => ({
    date: day.date,
    label: day.label,
    views: Number(day.youtubeSearchViews || 0),
  }));
  const maxViews = Math.max(1, ...searchDays.map((day) => day.views));
  const bestDay = searchDays.slice().sort((a, b) => b.views - a.views)[0];
  const selectedDate = state.selectedSearchDate && searchDays.some((day) => day.date === state.selectedSearchDate)
    ? state.selectedSearchDate
    : bestDay?.date;
  state.selectedSearchDate = selectedDate;
  document.querySelector("#searchTotal").textContent = `${Number(report.totals.youtubeSearchViews || 0).toLocaleString()} search views`;
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
  if (state.activeRange !== "selectMonth") return ranges[state.activeRange].label;
  return new Date(`${state.selectedMonth}-01T00:00:00Z`).toLocaleDateString("en-IN", { month: "short", year: "numeric" });
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

function showDashboard() {
  accessScreen.hidden = true;
  setupScreen.hidden = true;
  appShell.hidden = false;
  
  const isAd = (state.isAuditAdmin === undefined || state.isAuditAdmin === true);
  document.querySelectorAll('[data-view-tab="seo"], [data-view-tab="ytm"]').forEach(btn => {
    btn.style.display = isAd ? "" : "none";
  });
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
  
  return {
    stop: (success = true, customSuccessText = "Audit Complete! (100%)", customFailText = "Audit Failed!") => {
      clearInterval(interval);
      fill.style.width = "100%";
      label.textContent = success ? customSuccessText : customFailText;
      setTimeout(() => {
        container.classList.add("is-hidden");
        fill.style.width = "0%";
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
  
  const candidateList = ["Vinayak", "Mohit", "Raubinsh", "Saijal", "Aditya", "Vivek", "Other"].filter(c => candidatesWithResults.has(c));
  
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
              <button class="connect-button suggest-btn-sm" type="button" data-seo-action="suggest" data-video-id="${video.id}" data-channel-id="${video.channelId}" data-video-title="${escapeHtml(video.title)}">AI Suggest</button>
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
    const manager = getYtmName(video.channelTitle);
    managersWithResults.add(manager);
  });
  
  const managerList = ["Himanshu", "Ayush", "Atul Sharma", "Shubham", "Raubnish", "Amit", "Abhinav", "Shukendu", "Ashish Tyagi", "Lubna", "Vivek", "Other"].filter(m => managersWithResults.has(m));
  
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
      const manager = getYtmName(video.channelTitle);
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
            <div class="actions-cell">
              <a class="link-chip watch-link" href="https://www.youtube.com/watch?v=${video.id}" target="_blank" rel="noreferrer">Open</a>
            </div>
          </div>
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
