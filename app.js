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

let state = {
  connected: false,
  viewer: null,
  teamAuthEnabled: true,
  allowedEmailDomain: "testbook.com",
  googleConfigured: false,
  youtubeApiKeyConfigured: false,
  claudeConfigured: false,
  allConfigured: false,
  maxChannels: 20,
  channels: [],
  selectedChannelId: "",
  channelSearch: "",
  activeRange: "month",
  selectedMonth: currentMonthValue(),
  activeView: "dashboard",
  activeCompetitorCategory: "UGC NET",
  competitorRequestId: 0,
  researchRequestId: 0,
  researchKeyword: "",
  researchRange: "48h",
  researchFilter: "All",
  researchResults: [],
  researchIdeas: [],
  seoAuditRequestId: 0,
  seoAuditFilter: "All",
  seoAuditChannel: "All",
  seoAuditItems: [],
  report: null,
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
    loadCategoryCompetitors();
    return;
  }
  if (state.activeView === "research") {
    loadResearch({ force: true });
    return;
  }
  if (state.activeView === "seo-audit") {
    loadSeoAudit({ force: true });
    return;
  }
  loadDashboard();
});

document.querySelectorAll("[data-view-tab]").forEach((button) => {
  button.addEventListener("click", () => {
    state.activeView = button.dataset.viewTab;
    applyView();
    if (state.activeView === "competitors") loadCategoryCompetitors();
    if (state.activeView === "research") renderResearchView();
    if (state.activeView === "seo-audit") renderSeoAuditView();
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
  const channelToggle = event.target.closest(".channel-map-toggle");
  if (channelToggle) {
    const tags = channelToggle.closest(".benchmark-channel-cell")?.querySelector(".linked-channel-tags");
    if (tags) {
      tags.hidden = !tags.hidden;
      channelToggle.classList.toggle("active", !tags.hidden);
    }
    return;
  }

  const facultyToggle = event.target.closest(".faculty-video-toggle");
  if (facultyToggle) {
    const links = facultyToggle.closest(".faculty-meta")?.querySelector(".faculty-video-links");
    if (links) {
      links.hidden = !links.hidden;
      facultyToggle.classList.toggle("active", !links.hidden);
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

document.querySelector("#seoAuditFilterRow")?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-seo-filter]");
  if (!button) return;
  state.seoAuditFilter = button.dataset.seoFilter;
  renderSeoAuditFilters();
  renderSeoAuditSummary();
  renderSeoAuditResults();
});

document.querySelector("#seoAuditChannelRow")?.addEventListener("click", (event) => {
  const button = event.target.closest("[data-seo-channel]");
  if (!button) return;
  state.seoAuditChannel = button.dataset.seoChannel;
  renderSeoAuditChannels();
  renderSeoAuditSummary();
  renderSeoAuditResults();
});

document.querySelector("#seoAuditRunButton")?.addEventListener("click", () => {
  loadSeoAudit({ force: true });
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
      showAccessOnly(status, `Sign in with your ${status.allowedEmailDomain || "team"} Google account to continue.`);
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

async function loadDashboard() {
  try {
    showDashboard();
    setLoading();
    const channelQuery = state.selectedChannelId ? `&channelId=${encodeURIComponent(state.selectedChannelId)}` : "";
    const monthQuery = state.activeRange === "selectMonth" ? `&month=${encodeURIComponent(state.selectedMonth)}` : "";
    const report = await api(`/api/dashboard?range=${state.activeRange}${monthQuery}${channelQuery}`);
    state.report = report;
    state.channels = report.channels;
    state.selectedChannelId = report.selectedChannelId;
    renderReport(report);
  } catch (error) {
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
  renderSeoAuditView();
  applyView();
  if (state.activeView === "competitors") loadCategoryCompetitors();
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
  document.querySelector("#videoCount").textContent = totals.uploads.videos.toLocaleString();
  document.querySelector("#shortCount").textContent = totals.uploads.shorts.toLocaleString();
  document.querySelector("#liveCount").textContent = totals.uploads.live.toLocaleString();
  renderDelta("#videoDelta", totals.uploads.videos, comparisonTotals.uploads?.videos);
  renderDelta("#shortDelta", totals.uploads.shorts, comparisonTotals.uploads?.shorts);
  renderDelta("#liveDelta", totals.uploads.live, comparisonTotals.uploads?.live);
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
        <span>${day.uploads.videos}</span>
        <span>${day.uploads.shorts}</span>
        <span>${day.uploads.live}</span>
        <b>${day.uploads.total}</b>
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
    const average = Math.round(publishedViews / Math.max(1, totals.uploads[key]));
    return `
      <div class="average-card">
        <span>${formats[key].label}</span>
        <strong>${average.toLocaleString()}</strong>
      </div>
    `;
  }).join("");
}

function renderAllInOneDashboard(report) {
  renderAllInOneTopContent(report.topContent || []);
  renderChannelRankings("#allInOneOrganicChannels", report.allInOne?.channelRankings?.organicViews || [], "organicViews");
  renderChannelRankings("#allInOneSubscriberChannels", report.allInOne?.channelRankings?.subscribers || [], "subscribers");
  renderFacultyList("#facultyViews", report.allInOne?.faculties?.views || [], "views");
  renderFacultyList("#facultySubscribers", report.allInOne?.faculties?.subscribers || [], "subscribers");
}

function renderAllInOneTopContent(content) {
  const container = document.querySelector("#allInOneTopContent");
  container.innerHTML = content.length ? content.map((row, index) => `
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

function renderFacultyList(selector, rows, key) {
  const container = document.querySelector(selector);
  container.innerHTML = rows.length ? rows.map((row, index) => `
    <div class="rank-row faculty-row all-in-one-row">
      ${renderRankBadge(index)}
      <span>
        <strong>${escapeHtml(row.name)}</strong>
        <span class="faculty-meta">
          <button class="faculty-video-toggle" type="button" aria-label="Show tagged videos for ${escapeHtml(row.name)}"><sup>${row.videos}</sup></button>
          <ol class="faculty-video-links" hidden>
            ${dedupeFacultyVideos(row.taggedVideos || []).map((video, videoIndex) => `
              <li>
                <a href="${escapeHtml(video.url)}" target="_blank" rel="noreferrer">${escapeHtml(truncateTitle(video.title, 40))}</a>
              </li>
            `).join("")}
          </ol>
        </span>
      </span>
      <em>${Number(row[key] || 0).toLocaleString()}</em>
    </div>
  `).join("") : emptyCard("No faculty names detected in titles yet.");
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
      await loadCategoryCompetitors();
    });
  });
}

async function loadCategoryCompetitors() {
  const target = document.querySelector("#competitorBenchmark");
  const requestId = ++state.competitorRequestId;
  const category = state.activeCompetitorCategory;
  if (!document.querySelector("#competitorCategoryTabs").innerHTML.trim()) {
    renderCompetitorCategoryTabs();
  }
  document.querySelector("#competitorPageTitle").textContent = "Channel benchmark";
  document.querySelector("#competitorCategoryHeading").textContent = category;
  document.querySelector("#competitorRangeLabel").textContent = "Last 7 days";
  target.innerHTML = emptyCard("Loading competitor benchmark...");
  try {
    const data = await api(`/api/category-competitors?category=${encodeURIComponent(category)}&range=7`);
    if (requestId !== state.competitorRequestId) return;
    renderCategoryBenchmark(data);
  } catch (error) {
    if (requestId !== state.competitorRequestId) return;
    target.innerHTML = emptyCard(error.message);
  }
}

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
      : state.activeView === "research"
        ? "Research"
        : "SEO Audit";
}

function renderResearchView() {
  if (researchKeywordInput) researchKeywordInput.value = state.researchKeyword;
  if (researchRangeSelect) researchRangeSelect.value = state.researchRange;
  renderResearchFilters();
  renderResearchSummary();
  renderResearchResults();
  renderResearchIdeas();
}

function renderSeoAuditView() {
  renderSeoAuditChannels();
  renderSeoAuditFilters();
  renderSeoAuditSummary();
  renderSeoAuditResults();
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
  try {
    const data = await api(`/api/research?keyword=${encodeURIComponent(keyword)}&range=${encodeURIComponent(state.researchRange)}`);
    if (requestId !== state.researchRequestId) return;
    state.researchResults = data.items || [];
    state.researchFilter = "All";
    renderResearchView();
  } catch (error) {
    if (requestId !== state.researchRequestId) return;
    document.querySelector("#researchResults").innerHTML = emptyCard(error.message);
  }
}

async function suggestResearchTopics() {
  if (!state.researchResults.length) {
    document.querySelector("#researchIdeas").innerHTML = emptyCard("Run research first.");
    return;
  }
  document.querySelector("#researchIdeas").innerHTML = emptyCard("Asking Claude for topic ideas...");
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
  } catch (error) {
    document.querySelector("#researchIdeas").innerHTML = emptyCard(error.message);
  }
}

function filteredResearchResults() {
  if (state.researchFilter === "All") return state.researchResults;
  return state.researchResults.filter((item) => item.format === state.researchFilter);
}

function filteredSeoAuditItems() {
  const channelFiltered = state.seoAuditItems.filter((item) => item.channelId === state.seoAuditChannel);
  if (state.seoAuditFilter === "All") return channelFiltered;
  if (state.seoAuditFilter === "Lowest") {
    return [...channelFiltered].sort((a, b) => {
      if (a.score !== b.score) return a.score - b.score;
      return new Date(b.publishedAt).getTime() - new Date(a.publishedAt).getTime();
    });
  }
  return channelFiltered.filter((item) => item.format === state.seoAuditFilter);
}

function renderSeoAuditFilters() {
  document.querySelectorAll("[data-seo-filter]").forEach((button) => {
    button.classList.toggle("active", button.dataset.seoFilter === state.seoAuditFilter);
  });
}

function renderSeoAuditChannels() {
  const row = document.querySelector("#seoAuditChannelRow");
  if (!row) return;
  const channels = (state.channels || [])
    .filter((channel) => channel.id && channel.id !== "all-in-one")
    .map((channel) => ({ id: channel.id, name: channel.name }));
  const fallbackChannels = Array.from(new Map((state.seoAuditItems || [])
    .filter((item) => item.channelId && item.channelTitle)
    .map((item) => [item.channelId, { id: item.channelId, name: item.channelTitle }]))).map(([, value]) => value);
  const mergedChannels = (channels.length ? channels : fallbackChannels)
    .sort((a, b) => a.name.localeCompare(b.name));
  if (!mergedChannels.length) {
    row.innerHTML = "";
    return;
  }
  if (!mergedChannels.some((channel) => channel.id === state.seoAuditChannel)) {
    state.seoAuditChannel = mergedChannels[0].id;
  }
  row.innerHTML = mergedChannels.map((channel) => `
      <button class="filter-chip ${state.seoAuditChannel === channel.id ? "active" : ""}" type="button" data-seo-channel="${escapeHtml(channel.id)}">
        ${escapeHtml(channel.name)}
      </button>
    `).join("");
}

function renderSeoAuditSummary() {
  const summary = document.querySelector("#seoAuditSummary");
  if (!summary) return;
  if (!state.seoAuditItems.length) {
    summary.innerHTML = "";
    return;
  }
  const filtered = filteredSeoAuditItems();
  const averageScore = filtered.length
    ? Math.round(filtered.reduce((sum, item) => sum + Number(item.score || 0), 0) / filtered.length)
    : 0;
  const optimizedCount = filtered.filter((item) => item.status === "Optimized").length;
  summary.innerHTML = `
    <div class="research-stat">
      <span>Window</span>
      <strong>Last 24 hours</strong>
    </div>
    <div class="research-stat">
      <span>Audited items</span>
      <strong>${filtered.length}</strong>
    </div>
    <div class="research-stat">
      <span>Average SEO score</span>
      <strong>${averageScore}</strong>
    </div>
    <div class="research-stat">
      <span>Optimized</span>
      <strong>${optimizedCount}</strong>
    </div>
  `;
}

function renderSeoAuditResults() {
  const container = document.querySelector("#seoAuditResults");
  if (!container) return;
  const filtered = filteredSeoAuditItems();
  if (!state.seoAuditItems.length) {
    container.innerHTML = emptyCard("Open SEO Audit to scan the last 24 hours of owned videos and live streams.");
    return;
  }
  if (!filtered.length) {
    const channelLabel = `${channelNameById(state.seoAuditChannel).toLowerCase()} ${state.seoAuditFilter.toLowerCase()}`;
    container.innerHTML = emptyCard(`No ${channelLabel} items found in the last 24 hours.`);
    return;
  }
  container.innerHTML = `
    <div class="seo-audit-table">
      <div class="seo-audit-row seo-audit-head">
        <span>Score</span>
        <span>Content</span>
        <span>Channel</span>
        <span>Type</span>
        <span>Status</span>
        <span>Missing</span>
        <span>Link</span>
      </div>
      ${filtered.map((item) => `
        <div class="seo-audit-row">
          <div class="seo-score ${seoScoreClass(item.score)}">${Number(item.score || 0)}</div>
          <div class="research-title-cell">
            <strong>${escapeHtml(item.title)}</strong>
            <small>${escapeHtml(formatPublishedAt(item.publishedAt))}</small>
          </div>
          <span class="research-channel"><mark class="channel-tag own-tag">${escapeHtml(item.channelTitle)}</mark></span>
          <span class="research-format">${escapeHtml(item.format)}</span>
          <span class="seo-status ${seoStatusClass(item.status)}">${escapeHtml(item.status)}</span>
          <span class="seo-issues">${escapeHtml((item.issues || []).slice(0, 3).join(" · ") || "Looks good")}</span>
          <a class="link-chip" href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">YouTube</a>
        </div>
      `).join("")}
    </div>
  `;
}

async function loadSeoAudit(options = {}) {
  if (!options.force && state.seoAuditItems.length) {
    renderSeoAuditView();
    return;
  }
  const requestId = ++state.seoAuditRequestId;
  const results = document.querySelector("#seoAuditResults");
  if (results) {
    results.innerHTML = emptyCard("Scanning owned videos and live streams from the last 24 hours...");
  }
  try {
    const data = await api("/api/seo-audit");
    if (requestId !== state.seoAuditRequestId) return;
    state.seoAuditItems = data.items || [];
    state.seoAuditFilter = "All";
    if (!state.seoAuditItems.some((item) => item.channelId === state.seoAuditChannel)) {
      state.seoAuditChannel = "";
    }
    renderSeoAuditView();
  } catch (error) {
    if (requestId !== state.seoAuditRequestId) return;
    if (results) results.innerHTML = emptyCard(error.message);
  }
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

function seoScoreClass(score) {
  if (score >= 80) return "good";
  if (score >= 60) return "warn";
  return "bad";
}

function seoStatusClass(status) {
  if (status === "Optimized") return "good";
  if (status === "Needs review") return "warn";
  return "bad";
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
