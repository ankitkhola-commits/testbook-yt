import "dotenv/config";
import express from "express";
import { google } from "googleapis";
import postgres from "postgres";
import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const currentFilePath = fileURLToPath(import.meta.url);
const app = express();
const port = Number(process.env.PORT || 4173);
const dataDir = path.join(__dirname, ".data");
const tokenPath = path.join(dataDir, "google-tokens.json");
const profilesPath = path.join(dataDir, "google-profiles.json");
const competitorPath = path.join(dataDir, "competitors.json");
const competitorViewHistoryPath = path.join(dataDir, "competitor-view-history.json");
const envPath = path.join(__dirname, ".env");
const assetDir = path.join(__dirname, "assets");
const maxChannels = 20;
const databaseUrl = process.env.DATABASE_URL || process.env.POSTGRES_URL || "";
const sql = databaseUrl ? postgres(databaseUrl, { prepare: false }) : null;
let storageReadyPromise;
const responseCache = new Map();
const inFlightRequests = new Map();
const ttl = {
  dashboard: 10 * 60 * 1000,
  uploads: 30 * 60 * 1000,
  videoDetails: 60 * 60 * 1000,
  publicChannels: 24 * 60 * 60 * 1000,
  publicVideos: 30 * 60 * 1000,
  research: 6 * 60 * 60 * 1000,
  competitors: 30 * 60 * 1000,
};
const teamScopes = [
  "openid",
  "email",
  "profile",
];
const competitorCategoryMap = {
  Testbook: [
    { group: "Testbook", ids: ["UCgM9qPLv7R-hTRQIGa4wgKA"] },
    { group: "Selection Way", ids: ["UCF2gVDN_WKLhNSUx4CxKeUA"] },
    { group: "KGS SSC Exams", ids: ["UCrqTJFBTIEenpDv6evRtEhQ"] },
    { group: "SSC Adda", ids: ["UCAyYBPzFioHUxvVZEn4rMJA"] },
    { group: "PW SSC", ids: ["UCcaEVV7A47J4k9GFcqOOYkg", "UC5pmu8I-LBWsv79u1U7ZQcA"] },
    { group: "RWA", ids: ["UC5H9MzrMkJ5iuN11vV2PLhA"] },
    { group: "Exampur", ids: ["UCgVg6dmZHCxze_ay0bolPew"] },
    { group: "CareerWill GD", ids: ["UC2z5-3o-pO0sUTJyZWEevlA"] },
  ],
  Teaching: [
    { group: "Testbook", ids: ["UCYmiOXiMpiwlQFU4OIDPZzA", "UCLLhVCsO2Em-IQXwqq2EVyw", "UCUdYrLrnPFVpvWM6hNJSnJg", "UCaPCIXHm03fSYi3M3_LOf2Q"] },
    { group: "Adda247", ids: ["UCoiiIMZUxu0LxabtFk6PfVA", "UCi4HtAURYrk8kY6qfkI1C5Q"] },
    { group: "Adhyayan Mantra", ids: ["UCLAYiGjxDrpJYaBO6FDfMYQ", "UC-5WbwZfzC7oIkBVSuQGb2g", "UCm8llWK_kX9SStTZRVsNprg"] },
    { group: "Sachin Academy", ids: ["UC7Pb8pDlwmU8UvEx2Q6K5GA"] },
    { group: "Teaching Wallah", ids: ["UCUzqJZOkCejFoc5eqWWA9Eg"] },
    { group: "Teaching Pariksha", ids: ["UC88s854iaGReKSEJiT5qOeQ"] },
    { group: "Chandra Institute", ids: ["UCm-EPfZgP_gy8n0ry8mFHaA", "UCoCSoQk-GHdmGjgteK9PiNw"] },
  ],
  Railways: [
    { group: "Testbook", ids: ["UCuTgFUujt6tQXWxQMg-DHrQ"] },
    { group: "Adda247", ids: ["UCnejwhgQB5D_H7envJJgbXQ"] },
    { group: "PW", ids: ["UCVwOR1IRZ4b0OaBC3w5y2uw"] },
    { group: "MD Classes", ids: ["UCW67BMQKvGzv5gqSdteP9Eg"] },
    { group: "Vidyagram", ids: ["UC11vUA2Hp85ldDqZWNiHu5A"] },
    { group: "Quick Trick Sahil", ids: ["UCFaF6hV1EsL_5qlqmCGpDYQ"] },
    { group: "RWA", ids: ["UCcEnPq88uaK0foJsfgw7r7g"] },
    { group: "RankersGurukul", ids: ["UCFKtaE9N_sFe01sxgnsqNSQ"] },
    { group: "Science Magnet", ids: ["UC-hnHgZpTEd0sqrX68juuKA"] },
  ],
  CGL: [
    { group: "Testbook", ids: ["UCynL1ZibAI7kC5yZPv6a_Sw"] },
    { group: "SSC Wallah", ids: ["UCcaEVV7A47J4k9GFcqOOYkg"] },
    { group: "SSC CGL Adda247", ids: ["UCV6HCLPearneK6wLQ-WitUA"] },
    { group: "CareerWill SSC", ids: ["UCx-7YPrGnNC81ahyqvqu27g"] },
    { group: "RWA SSC", ids: ["UCdMLyQSRPU6Gpwb8O7UnNvA"] },
    { group: "Drishti SSC", ids: ["UCynL1ZibAI7kC5yZPv6a_Sw"] },
  ],
  "UGC NET": [
    { group: "Testbook", ids: ["UC_uR26BodKBZ4HVwxwd5isQ", "UCXx7EB5fueJeOI5pYuYSagQ", "UC0F3xKUqvg-h4rlzrTTWSxA"] },
    { group: "Adda247", ids: ["UCeOoUjlLiP5qKBozcdIjnGA", "UCwwjfrWuuNlDUiNnfBgJ8oQ", "UCmT8CI_kHjvpO6vgs6SjH-Q", "UCdJlZoHXu7AFaxz92swQwNw"] },
    { group: "PW", ids: ["UCx8deYX1wXwFTWtdt0afmIQ"] },
    { group: "Unacademy", ids: ["UCFdyj9XtVS52WYXHh51OOaQ"] },
    { group: "RWA", ids: ["UCVO98T-mKv8WYakZC_pfHaQ"] },
    { group: "Apni University", ids: ["UCNLBII1rySGWqVQ-m63jGug"] },
  ],
  Bihar: [
    { group: "Testbook Bihar", ids: ["UC0FSg3oiJZlpTYgI0hCNazQ"] },
    { group: "Adda Bihar", ids: ["UCNxOioAL26MIhh7jpCilmsw"] },
    { group: "Bihar Exams Wallah", ids: ["UCmfWgsWlWZwmyzMP8Bixs6w"] },
    { group: "KGS Bihar Exams", ids: ["UCeDihmXqku0PCLB18kD8FZw"] },
    { group: "Careerwill Bihar", ids: ["UCw9feQwG1p7I-kf6ACC8xvA"] },
    { group: "BiharShila By MD Classes", ids: ["UC1v_28kndB74-M6Za9u7LhQ"] },
  ],
  Banking: [
    { group: "Banking Testbook", ids: ["UC_fKmFGyY4MPVzz4TT_N_6Q"] },
    { group: "Banking Wallah", ids: ["UCg5_K50hLTKerLkSE7I1yWQ"] },
    { group: "Adda247", ids: ["UC1L2JoMpcY6MRLhFd3gg5Xg"] },
    { group: "Adda247 Bankers", ids: ["UC7DgZZeZD2HKc7JUAsIwe-w"] },
    { group: "Bankers Way by Unacademy", ids: ["UCzdgWZfyWtkrlRUrLwWYfbw"] },
  ],
  UPSC: [
    { group: "Testbook PrepLab", ids: ["UC1pJ8ods7vGboH2BXuZVBbQ"] },
    { group: "Sarthi IAS", ids: ["UChO18r_h8-K_Lf4Hb-bYGDQ"] },
    { group: "Unacademy", ids: ["UCVOyyXupdtEblFbno_4ibLQ"] },
    { group: "UPSC Wallah", ids: ["UCqOy6oOu6RPJNHYQ8f_Ybvg"] },
    { group: "Drishti IAS", ids: ["UCzLqOSZPtUKrmSEnlH4LAvw"] },
    { group: "DECODE CIVILS With Mudit Jain", ids: ["UC2_GuSyUafdqfL2TFiDbuEg"] },
    { group: "Khan Global Studies", ids: ["UC7krt1E6XvrywJBu0ZOyq3Q"] },
  ],
  "AE JE": [
    { group: "Testbook", ids: ["UCFTTIDN58laGeV8DM9D4spg"] },
    { group: "Engineers Wallah", ids: ["UCOGnjpVWVV2ixP2S9Pq5afQ"] },
    { group: "Engineers Adda", ids: ["UCOqpyBqmT-eJsMinC3ggmbw"] },
  ],
  Odisha: [
    { group: "Odisha Testbook", ids: ["UC1y7Nv1-ZdkJdNUkUgFaydg", "UCUIaneuBNuiTdMQIyfOvUFA", "UCW84HFL8R663JRJyM6SZJ0Q"] },
    { group: "ADDA", ids: ["UCBS2TBsSg4bl--zknEC6cbQ"] },
    { group: "VEDANG CLASSES", ids: ["UCZ1foSst2Enawd6dAg3gbuw"] },
    { group: "OPSC WALLAH", ids: ["UCaqI6pFUCixOMebRPmLbRUg"] },
  ],
  Bengali: [
    { group: "Testbook", ids: ["UCSzinFg4bwJktBgvmWKNP6Q", "UC1pQayM4quYlDZbrhD6VEUg"] },
    { group: "Adda247 Bengali", ids: ["UC0bd1IAfOijSGAZQYhpEypg", "UCZUvmnlLmB18VlbkiRGYM0A"] },
    { group: "The Way Of Solution", ids: ["UCY5r9XeelQQwUg7-VI10Q7A"] },
    { group: "WBPSC WALLAH", ids: ["UCL0Bks2X-37TI1NKGVh4cuA"] },
  ],
  Marathi: [
    { group: "Marathi Testbook", ids: ["UCKtAel248rFM1Nxd1UJlQHA"] },
    { group: "Ignite MPSC", ids: ["UC2PEXIAsGEcXRcvGBLsJZtg"] },
    { group: "Mharashtra Academy", ids: ["UC02stQ3Q-WPoNOAdDHeCSTQ"] },
    { group: "Master Teacher", ids: ["UC2qIazfAHyV852UQRA-NN7A"] },
  ],
  MPSC: [
    { group: "MPSC TESTBOOK", ids: ["UCfG_bedD0HBbrFo1JpgUEqA"] },
    { group: "MPSC WALLAH", ids: ["UC1Fc-8RLxmoi6Tw9cbNoxxQ"] },
    { group: "Dnyanadeep Academy Pune", ids: ["UCvMnTzkPFCLMZpEyI64VuyA"] },
    { group: "STEP UP ACADEMY", ids: ["UCfBRbHCzBxwC39mmYDal1Hg"] },
  ],
  Punjab: [
    { group: "PUNJAB TESTBOOK", ids: ["UCW3qu1ViFQhIMLZRXugCFRg"] },
    { group: "Adda247 Punjab", ids: ["UChC-qt80cJxW1xQEsq7BETg"] },
    { group: "Arora Classes", ids: ["UCA6LT_oB4Z_AdIOThNVVh7g"] },
    { group: "Prep Punjab", ids: ["UCXNAImEe2VzG0tI_EkVJ_8Q"] },
    { group: "PW Punjab", ids: ["UCRioFimAz1zw6RXhlT9znUg"] },
  ],
  Telugu: [
    { group: "Testbook Telugu", ids: ["UC08MiG8-bjllfy94OeY5zWw"] },
    { group: "Adda247 Telugu", ids: ["UCqqDD-1K6WTjS16Y8pdY1_Q"] },
    { group: "SI / Constable & Groups", ids: ["UCN52umFhTwXEkVoFZqZs4eg"] },
  ],
};

const scopes = [
  "https://www.googleapis.com/auth/youtube.readonly",
  "https://www.googleapis.com/auth/yt-analytics.readonly",
];

app.use(express.json());

app.get("/", (_req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.get("/app.js", (_req, res) => {
  res.type("application/javascript");
  res.sendFile(path.join(__dirname, "app.js"));
});

app.get("/styles.css", (_req, res) => {
  res.type("text/css");
  res.sendFile(path.join(__dirname, "styles.css"));
});

app.get("/assets/:file", (req, res) => {
  const file = path.basename(String(req.params.file || ""));
  res.sendFile(path.join(assetDir, file));
});

app.get("/api/config", async (req, res) => {
  res.json({
    ...configStatus(),
    viewer: readViewerSession(req),
    teamAuthEnabled: teamAuthEnabled(),
    allowedEmailDomain: allowedEmailDomain(),
    viewerAllowlistEnabled: allowedViewerEmails().length > 0,
  });
});

app.post("/api/config", async (req, res, next) => {
  try {
    const current = await readEnvFile();
    const updates = {
      GOOGLE_CLIENT_ID: cleanSecret(req.body.googleClientId),
      GOOGLE_CLIENT_SECRET: cleanSecret(req.body.googleClientSecret),
      GOOGLE_REDIRECT_URI: cleanSecret(req.body.googleRedirectUri) || `http://localhost:${port}/oauth2callback`,
      YOUTUBE_API_KEY: cleanSecret(req.body.youtubeApiKey),
      ANTHROPIC_API_KEY: cleanSecret(req.body.anthropicApiKey),
    };

    for (const [key, value] of Object.entries(updates)) {
      if (value) {
        current[key] = value;
        process.env[key] = value;
      }
    }

    await writeEnvFile(current);
    res.json(configStatus());
  } catch (error) {
    next(error);
  }
});

app.get("/api/status", async (req, res) => {
  res.json({
    ...configStatus(),
    connected: await hasConnectedGoogle(),
    maxChannels,
    viewer: readViewerSession(req),
    teamAuthEnabled: teamAuthEnabled(),
    allowedEmailDomain: allowedEmailDomain(),
    viewerAllowlistEnabled: allowedViewerEmails().length > 0,
  });
});

app.get("/api/session", async (req, res) => {
  const viewer = readViewerSession(req);
  res.json({
    viewer,
    teamAuthEnabled: teamAuthEnabled(),
    allowedEmailDomain: allowedEmailDomain(),
    viewerAllowlistEnabled: allowedViewerEmails().length > 0,
  });
});

app.get("/auth/team-google", async (_req, res) => {
  await hydrateEnvFromFile();
  if (!hasGoogleConfig()) {
    res.status(400).send("Missing Google OAuth config.");
    return;
  }
  const oauth2Client = makeTeamOAuthClient();
  const url = oauth2Client.generateAuthUrl({
    access_type: "online",
    prompt: "select_account",
    scope: teamScopes,
  });
  res.redirect(url);
});

app.get("/oauth2callback/team", async (req, res, next) => {
  try {
    const code = String(req.query.code || "");
    if (!code) {
      res.status(400).send("Google OAuth did not return a code.");
      return;
    }
    const oauth2Client = makeTeamOAuthClient();
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);
    const oauth2 = google.oauth2({ version: "v2", auth: oauth2Client });
    const me = await oauth2.userinfo.get();
    const email = String(me.data.email || "").toLowerCase();
    if (!isAllowedViewerEmail(email)) {
      clearViewerSession(res);
      res.status(403).send("Only approved team email IDs can access this app.");
      return;
    }
    setViewerSession(res, {
      email,
      name: String(me.data.name || email.split("@")[0] || "Team member"),
      picture: String(me.data.picture || ""),
    });
    res.redirect("/");
  } catch (error) {
    next(error);
  }
});

app.post("/api/logout-app", async (_req, res) => {
  clearViewerSession(res);
  res.json({ ok: true });
});

app.get("/auth/google", async (_req, res) => {
  await hydrateEnvFromFile();
  if (!hasGoogleConfig()) {
    res.status(400).send("Missing Google OAuth config. Copy .env.example to .env and fill GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and GOOGLE_REDIRECT_URI.");
    return;
  }

  const oauth2Client = makeOAuthClient();
  const url = oauth2Client.generateAuthUrl({
    access_type: "offline",
    prompt: "consent",
    scope: scopes,
  });
  res.redirect(url);
});

app.get("/oauth2callback", async (req, res, next) => {
  try {
    const code = String(req.query.code || "");
    if (!code) {
      res.status(400).send("Google OAuth did not return a code.");
      return;
    }
    const oauth2Client = makeOAuthClient();
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);
    const channels = await listOwnedChannels(oauth2Client);
    await saveGoogleProfile(tokens, channels);
    res.redirect("/");
  } catch (error) {
    next(error);
  }
});

app.use((req, res, next) => {
  if (!teamAuthEnabled()) {
    next();
    return;
  }
  const publicApiPaths = new Set([
    "/api/status",
    "/api/config",
    "/api/session",
    "/api/logout-app",
  ]);
  if (publicApiPaths.has(req.path)) {
    next();
    return;
  }
  if (req.path === "/" || req.path === "/app.js" || req.path === "/styles.css" || req.path.startsWith("/assets/")) {
    next();
    return;
  }
  const viewer = readViewerSession(req);
  if (!viewer) {
    res.status(401).json({ error: "Sign in with your team Google account to access this dashboard." });
    return;
  }
  req.viewer = viewer;
  next();
});

app.post("/api/logout", async (_req, res) => {
  await saveTokens(null);
  await saveProfiles([]);
  res.json({ ok: true });
});

app.post("/api/reset", async (_req, res) => {
  await saveTokens(null);
  await saveProfiles([]);
  await rm(competitorPath, { force: true }).catch(() => {});
  res.json({ ok: true, ...configStatus() });
});

app.get("/api/channels", async (_req, res, next) => {
  try {
    const entries = await connectedChannelEntries();
    res.json({ channels: publicChannels(entries).slice(0, maxChannels) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/dashboard", async (req, res, next) => {
  try {
    const entries = (await connectedChannelEntries()).slice(0, maxChannels);
    const channels = [{ id: "all-in-one", name: "All in One", handle: "@all-in-one" }, ...publicChannels(entries)];
    const range = String(req.query.range || "month");
    const month = String(req.query.month || "");
    const requestedChannelId = String(req.query.channelId || "");
    const activeChannelId = channels.some((channel) => channel.id === requestedChannelId) ? requestedChannelId : channels[0]?.id;
    const selectedEntries = activeChannelId === "all-in-one"
      ? entries
      : entries.filter((entry) => entry.channel.id === activeChannelId);
    const dates = dateWindow(range, month);
    const compareDates = comparisonWindow(dates, range);
    const payload = await cached(
      makeCacheKey("dashboard", activeChannelId, range, month, dates.startDate, dates.endDate, selectedEntries.map((entry) => entry.channel.id).sort()),
      ttl.dashboard,
      async () => {
        const [channelReports, comparisonReports] = await Promise.all([
          Promise.all(selectedEntries.map((entry) => cachedChannelReport(entry.auth, entry.channel, dates))),
          Promise.all(selectedEntries.map((entry) => cachedChannelReport(entry.auth, entry.channel, compareDates))),
        ]);
        const merged = mergeReports(channelReports, dates.days);
        const comparison = mergeReports(comparisonReports, compareDates.days);
        return {
          range,
          dates,
          comparisonDates: compareDates,
          channels,
          connectedCount: entries.length,
          selectedChannelId: activeChannelId,
          isAllInOne: activeChannelId === "all-in-one",
          title: activeChannelId === "all-in-one" ? "All in One" : (selectedEntries[0]?.channel.name || "Channel analytics"),
          totals: merged.totals,
          comparisonTotals: comparison.totals,
          series: merged.series,
          topContent: merged.topContent,
          competitors: [],
          insights: buildLiveInsights([], merged),
          allInOne: activeChannelId === "all-in-one" ? buildAllInOneDashboard(selectedEntries, channelReports, dates) : null,
        };
      },
      { force: req.query.force === "1" }
    );
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get("/api/search-keywords", async (req, res, next) => {
  try {
    const entries = (await connectedChannelEntries()).slice(0, maxChannels);
    const channels = publicChannels(entries);
    const range = String(req.query.range || "7");
    const month = String(req.query.month || "");
    const requestedChannelId = String(req.query.channelId || "");
    const activeChannelId = channels.some((channel) => channel.id === requestedChannelId) ? requestedChannelId : channels[0]?.id;
    const date = String(req.query.date || "");
    const dates = dateWindow(range, month);
    if (!date || date < dates.startDate || date > dates.endDate) {
      res.status(400).json({ error: "Choose a date inside the selected range." });
      return;
    }
    const selectedEntries = entries.filter((entry) => entry.channel.id === activeChannelId);
    const keywords = await searchKeywordsForDate(selectedEntries, date);
    res.json({ date, keywords });
  } catch (error) {
    next(error);
  }
});

app.post("/api/competitors", async (req, res, next) => {
  try {
    const channelId = String(req.body.channelId || "");
    const nameOrUrl = String(req.body.name || "").trim();
    const competitorChannelId = String(req.body.competitorChannelId || "").trim();
    const format = String(req.body.format || "Balanced");
    if (!channelId || !nameOrUrl) {
      res.status(400).json({ error: "Select one owned channel and enter a competitor channel." });
      return;
    }
    const competitors = await readCompetitors();
    const current = competitors[channelId] || [];
    if (current.length >= 6) {
      res.status(400).json({ error: "Each owned channel can have up to 6 competitors." });
      return;
    }
    current.push({ id: randomUUID(), name: nameOrUrl, channelId: competitorChannelId, format });
    competitors[channelId] = current;
    await saveCompetitors(competitors);
    res.json({ competitors: current });
  } catch (error) {
    next(error);
  }
});

app.get("/api/search-channels", async (req, res, next) => {
  try {
    if (!process.env.YOUTUBE_API_KEY) {
      res.status(400).json({ error: "Add a YouTube API key before searching competitor channels." });
      return;
    }
    const query = String(req.query.q || "").trim();
    if (!query) {
      res.json({ channels: [] });
      return;
    }
    res.json({ channels: await searchPublicChannels(query) });
  } catch (error) {
    next(error);
  }
});

app.get("/api/category-competitors", async (req, res, next) => {
  try {
    if (!process.env.YOUTUBE_API_KEY) {
      res.status(400).json({ error: "Add a YouTube API key before loading category competitors." });
      return;
    }
    const category = String(req.query.category || "UGC NET");
    const range = String(req.query.range || "7");
    const month = String(req.query.month || "");
    const mappings = competitorCategoryMap[category];
    if (!mappings) {
      res.json({ available: false, category, message: `${category} mapping will be added later.` });
      return;
    }
    const dates = dateWindow(range, month);
    const data = await cached(
      makeCacheKey("category-competitors", category, range, month, dates.startDate, dates.endDate),
      ttl.competitors,
      () => categoryCompetitorReport(category, mappings, dates),
      { force: req.query.force === "1" }
    );
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.get("/api/research", async (req, res, next) => {
  try {
    if (!process.env.YOUTUBE_API_KEY) {
      res.status(400).json({ error: "Add a YouTube API key before running keyword research." });
      return;
    }
    const keyword = String(req.query.keyword || "").trim();
    const range = String(req.query.range || "48h");
    if (!keyword) {
      res.status(400).json({ error: "Enter a keyword to run research." });
      return;
    }
    const items = await cached(
      makeCacheKey("research", keyword.toLowerCase(), range),
      ttl.research,
      () => researchKeywordVideos(keyword, range),
      { force: req.query.force === "1" }
    );
    res.json({ keyword, range, items });
  } catch (error) {
    next(error);
  }
});

app.post("/api/research/suggest", async (req, res, next) => {
  try {
    if (!process.env.ANTHROPIC_API_KEY) {
      res.status(400).json({ error: "Add an Anthropic API key before suggesting topics." });
      return;
    }
    const keyword = String(req.body.keyword || "").trim();
    const range = String(req.body.range || "48h");
    const items = Array.isArray(req.body.items) ? req.body.items.slice(0, 20) : [];
    if (!keyword || !items.length) {
      res.status(400).json({ error: "Run research first so there are videos to analyze." });
      return;
    }
    const ideas = await suggestTopicsFromResearch(keyword, range, items);
    res.json({ keyword, range, ideas });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/competitors/:channelId/:competitorId", async (req, res, next) => {
  try {
    const competitors = await readCompetitors();
    competitors[req.params.channelId] = (competitors[req.params.channelId] || []).filter((item) => item.id !== req.params.competitorId);
    await saveCompetitors(competitors);
    res.json({ competitors: competitors[req.params.channelId] || [] });
  } catch (error) {
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  console.error(error);
  const message = error?.response?.data?.error?.message || error?.message || "Unknown server error";
  res.status(error?.code || error?.response?.status || 500).json({ error: message });
});

await hydrateEnvFromFile();

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === currentFilePath;
if (isDirectRun) {
  app.listen(port, () => {
    console.log(`YouTube dashboard running at http://localhost:${port}`);
  });
}

function makeCacheKey(...parts) {
  return parts.map((part) => {
    if (part === null || part === undefined) return "";
    if (Array.isArray(part)) return part.join(",");
    if (typeof part === "object") return JSON.stringify(part);
    return String(part);
  }).join("|");
}

async function cached(key, maxAgeMs, loader, options = {}) {
  if (!options.force) {
    const existing = responseCache.get(key);
    if (existing && Date.now() - existing.createdAt < maxAgeMs) {
      return existing.value;
    }
    if (inFlightRequests.has(key)) {
      return inFlightRequests.get(key);
    }
  }
  const promise = Promise.resolve()
    .then(loader)
    .then((value) => {
      responseCache.set(key, { createdAt: Date.now(), value });
      return value;
    })
    .finally(() => {
      inFlightRequests.delete(key);
    });
  inFlightRequests.set(key, promise);
  return promise;
}

export default app;

function hasGoogleConfig() {
  return Boolean(process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET && process.env.GOOGLE_REDIRECT_URI);
}

function configStatus() {
  const googleConfigured = hasGoogleConfig();
  const youtubeApiKeyConfigured = Boolean(process.env.YOUTUBE_API_KEY);
  const claudeConfigured = Boolean(process.env.ANTHROPIC_API_KEY);
  return {
    connected: false,
    googleConfigured,
    youtubeApiKeyConfigured,
    claudeConfigured,
    allConfigured: googleConfigured && youtubeApiKeyConfigured && claudeConfigured,
    redirectUri: process.env.GOOGLE_REDIRECT_URI || `http://localhost:${port}/oauth2callback`,
    clientIdPreview: previewSecret(process.env.GOOGLE_CLIENT_ID),
    youtubeKeyPreview: previewSecret(process.env.YOUTUBE_API_KEY),
    anthropicKeyPreview: previewSecret(process.env.ANTHROPIC_API_KEY),
    storageMode: sql ? "database" : "local",
  };
}

function makeOAuthClient() {
  return new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI
  );
}

function teamRedirectUri() {
  if (process.env.TEAM_GOOGLE_REDIRECT_URI) return process.env.TEAM_GOOGLE_REDIRECT_URI;
  const fallback = process.env.GOOGLE_REDIRECT_URI || `http://localhost:${port}/oauth2callback`;
  return fallback.endsWith("/oauth2callback") ? `${fallback}/team` : `${fallback}-team`;
}

function makeTeamOAuthClient() {
  return new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    teamRedirectUri()
  );
}

function teamAuthEnabled() {
  if (process.env.TEAM_AUTH_ENABLED != null) {
    return String(process.env.TEAM_AUTH_ENABLED).toLowerCase() !== "false";
  }
  return Boolean(process.env.VERCEL);
}

function allowedEmailDomain() {
  return String(process.env.ALLOWED_EMAIL_DOMAIN || "").toLowerCase().trim();
}

function allowedViewerEmails() {
  return String(process.env.ALLOWED_VIEWER_EMAILS || "")
    .split(",")
    .map((email) => email.trim().toLowerCase())
    .filter(Boolean);
}

function sessionSecret() {
  return process.env.SESSION_SECRET || process.env.GOOGLE_CLIENT_SECRET || "team-dashboard-secret";
}

function isAllowedViewerEmail(email = "") {
  const normalized = String(email || "").toLowerCase().trim();
  const allowlist = allowedViewerEmails();
  if (allowlist.length) return allowlist.includes(normalized);
  const domain = allowedEmailDomain();
  if (!domain) return false;
  return normalized.endsWith(`@${domain}`);
}

function parseCookies(req) {
  return Object.fromEntries(
    String(req.headers.cookie || "")
      .split(/;\s*/)
      .filter(Boolean)
      .map((pair) => {
        const index = pair.indexOf("=");
        return [pair.slice(0, index), decodeURIComponent(pair.slice(index + 1))];
      })
  );
}

function signSessionPayload(payload) {
  return createHmac("sha256", sessionSecret()).update(payload).digest("hex");
}

function secureCookieFlag() {
  return process.env.NODE_ENV === "production" || Boolean(process.env.VERCEL);
}

function setViewerSession(res, viewer) {
  const payload = Buffer.from(JSON.stringify({
    email: viewer.email,
    name: viewer.name,
    picture: viewer.picture,
    issuedAt: Date.now(),
  })).toString("base64url");
  const signature = signSessionPayload(payload);
  const cookie = `tb_session=${payload}.${signature}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${60 * 60 * 12}${secureCookieFlag() ? "; Secure" : ""}`;
  res.setHeader("Set-Cookie", cookie);
}

function clearViewerSession(res) {
  res.setHeader("Set-Cookie", `tb_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0${secureCookieFlag() ? "; Secure" : ""}`);
}

function readViewerSession(req) {
  const raw = parseCookies(req).tb_session;
  if (!raw || !raw.includes(".")) return null;
  const [payload, signature] = raw.split(".");
  const expected = signSessionPayload(payload);
  const left = Buffer.from(signature);
  const right = Buffer.from(expected);
  if (left.length !== right.length || !timingSafeEqual(left, right)) return null;
  try {
    const parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    if (!isAllowedViewerEmail(parsed.email)) return null;
    return parsed;
  } catch {
    return null;
  }
}

async function ensureStorage() {
  if (!sql) return;
  if (!storageReadyPromise) {
    storageReadyPromise = sql`
      create table if not exists app_state (
        key text primary key,
        payload jsonb not null,
        updated_at timestamptz not null default now()
      )
    `;
  }
  await storageReadyPromise;
}

async function authedClient() {
  const tokens = await readTokens();
  if (!tokens) {
    const error = new Error("Not connected to Google.");
    error.code = 401;
    throw error;
  }
  const client = makeOAuthClient();
  client.setCredentials(tokens);
  client.on("tokens", async (newTokens) => {
    await saveTokens({ ...tokens, ...newTokens });
  });
  return client;
}

async function hasConnectedGoogle() {
  const profiles = await readProfiles();
  if (profiles.length) return true;
  return Boolean(await readTokens());
}

async function connectedChannelEntries() {
  let profiles = await readProfiles();
  if (!profiles.length) {
    profiles = await migrateLegacyToken();
  }

  const entries = [];
  for (const profile of profiles) {
    const auth = makeProfileAuth(profile);
    let channels = profile.channels || [];
    try {
      channels = await listOwnedChannels(auth);
      await updateProfileChannels(profile.id, channels);
    } catch {
      // Keep last-known channels visible if a token refresh fails; API calls will surface errors later.
    }
    for (const channel of channels) {
      entries.push({ auth, profileId: profile.id, channel: { ...channel, profileId: profile.id } });
    }
  }

  const seen = new Set();
  return entries.filter((entry) => {
    if (seen.has(entry.channel.id)) return false;
    seen.add(entry.channel.id);
    return true;
  });
}

function publicChannels(entries) {
  return entries.map((entry) => {
    const { auth, ...rest } = entry;
    return rest.channel;
  });
}

function makeProfileAuth(profile) {
  const client = makeOAuthClient();
  client.setCredentials(profile.tokens);
  client.on("tokens", async (newTokens) => {
    await updateProfileTokens(profile.id, { ...profile.tokens, ...newTokens });
  });
  return client;
}

async function migrateLegacyToken() {
  const tokens = await readTokens();
  if (!tokens) return [];
  const auth = makeOAuthClient();
  auth.setCredentials(tokens);
  const channels = await listOwnedChannels(auth).catch(() => []);
  const profile = { id: randomUUID(), tokens, channels };
  await saveProfiles([profile]);
  return [profile];
}

async function readProfiles() {
  if (sql) {
    await ensureStorage();
    const rows = await sql`select payload from app_state where key = 'google_profiles' limit 1`;
    return Array.isArray(rows[0]?.payload) ? rows[0].payload : [];
  }
  try {
    return JSON.parse(await readFile(profilesPath, "utf8"));
  } catch {
    return [];
  }
}

async function saveProfiles(profiles) {
  if (sql) {
    await ensureStorage();
    await sql`
      insert into app_state (key, payload)
      values ('google_profiles', ${sql.json(profiles)})
      on conflict (key) do update set payload = excluded.payload, updated_at = now()
    `;
    return;
  }
  await mkdir(dataDir, { recursive: true });
  await writeFile(profilesPath, JSON.stringify(profiles, null, 2), "utf8");
}

async function saveGoogleProfile(tokens, channels) {
  const profiles = await readProfiles();
  const channelIds = new Set(channels.map((channel) => channel.id));
  const existing = profiles.find((profile) => (profile.channels || []).some((channel) => channelIds.has(channel.id)));
  if (existing) {
    existing.tokens = { ...existing.tokens, ...tokens };
    existing.channels = channels;
  } else {
    profiles.push({ id: randomUUID(), tokens, channels });
  }
  await saveProfiles(profiles);
  await saveTokens(tokens);
}

async function updateProfileTokens(profileId, tokens) {
  const profiles = await readProfiles();
  const profile = profiles.find((item) => item.id === profileId);
  if (!profile) return;
  profile.tokens = tokens;
  await saveProfiles(profiles);
}

async function updateProfileChannels(profileId, channels) {
  const profiles = await readProfiles();
  const profile = profiles.find((item) => item.id === profileId);
  if (!profile) return;
  profile.channels = channels;
  await saveProfiles(profiles);
}

async function readTokens() {
  if (sql) {
    await ensureStorage();
    const rows = await sql`select payload from app_state where key = 'legacy_tokens' limit 1`;
    return rows[0]?.payload || null;
  }
  try {
    return JSON.parse(await readFile(tokenPath, "utf8"));
  } catch {
    return null;
  }
}

async function saveTokens(tokens) {
  if (sql) {
    await ensureStorage();
    if (!tokens) {
      await sql`delete from app_state where key = 'legacy_tokens'`;
      return;
    }
    await sql`
      insert into app_state (key, payload)
      values ('legacy_tokens', ${sql.json(tokens)})
      on conflict (key) do update set payload = excluded.payload, updated_at = now()
    `;
    return;
  }
  await mkdir(dataDir, { recursive: true });
  if (!tokens) {
    await rm(tokenPath, { force: true }).catch(() => {});
    return;
  }
  await writeFile(tokenPath, JSON.stringify(tokens, null, 2), "utf8");
}

async function readEnvFile() {
  try {
    const text = await readFile(envPath, "utf8");
    return Object.fromEntries(
      text
        .split(/\r?\n/)
        .filter((line) => line.trim() && !line.trim().startsWith("#"))
        .map((line) => {
          const index = line.indexOf("=");
          return index === -1 ? [line.trim(), ""] : [line.slice(0, index).trim(), line.slice(index + 1).trim()];
        })
    );
  } catch {
    return {};
  }
}

async function hydrateEnvFromFile() {
  const values = await readEnvFile();
  for (const [key, value] of Object.entries(values)) {
    if (value && !process.env[key]) {
      process.env[key] = value;
    }
  }
}

async function writeEnvFile(values) {
  const orderedKeys = ["GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET", "GOOGLE_REDIRECT_URI", "YOUTUBE_API_KEY", "ANTHROPIC_API_KEY"];
  const body = orderedKeys
    .filter((key) => values[key])
    .map((key) => `${key}=${values[key]}`)
    .join("\n");
  await writeFile(envPath, `${body}\n`, "utf8");
}

function cleanSecret(value) {
  return String(value || "").trim();
}

function previewSecret(value) {
  if (!value) return "";
  if (value.length <= 10) return "configured";
  return `${value.slice(0, 4)}...${value.slice(-4)}`;
}

async function readCompetitors() {
  if (sql) {
    await ensureStorage();
    const rows = await sql`select payload from app_state where key = 'competitors' limit 1`;
    return rows[0]?.payload || {};
  }
  try {
    return JSON.parse(await readFile(competitorPath, "utf8"));
  } catch {
    return {};
  }
}

async function saveCompetitors(competitors) {
  if (sql) {
    await ensureStorage();
    await sql`
      insert into app_state (key, payload)
      values ('competitors', ${sql.json(competitors)})
      on conflict (key) do update set payload = excluded.payload, updated_at = now()
    `;
    return;
  }
  await mkdir(dataDir, { recursive: true });
  await writeFile(competitorPath, JSON.stringify(competitors, null, 2), "utf8");
}

async function readCompetitorViewHistory() {
  if (sql) {
    await ensureStorage();
    const rows = await sql`select payload from app_state where key = 'competitor_view_history' limit 1`;
    return rows[0]?.payload || {};
  }
  try {
    return JSON.parse(await readFile(competitorViewHistoryPath, "utf8"));
  } catch {
    return {};
  }
}

async function saveCompetitorViewHistory(history) {
  if (sql) {
    await ensureStorage();
    await sql`
      insert into app_state (key, payload)
      values ('competitor_view_history', ${sql.json(history)})
      on conflict (key) do update set payload = excluded.payload, updated_at = now()
    `;
    return;
  }
  await mkdir(dataDir, { recursive: true });
  await writeFile(competitorViewHistoryPath, JSON.stringify(history, null, 2), "utf8");
}

async function listOwnedChannels(auth) {
  const youtube = google.youtube({ version: "v3", auth });
  const response = await youtube.channels.list({
    part: ["snippet", "statistics", "contentDetails"],
    mine: true,
    maxResults: maxChannels,
  });
  return (response.data.items || []).map((item) => ({
    id: item.id,
    name: item.snippet?.title || "Untitled channel",
    handle: item.snippet?.customUrl || item.id,
    thumbnail: item.snippet?.thumbnails?.default?.url,
    subscribers: Number(item.statistics?.subscriberCount || 0),
    totalViews: Number(item.statistics?.viewCount || 0),
    uploadsPlaylistId: item.contentDetails?.relatedPlaylists?.uploads,
    competitors: [],
  }));
}

async function channelReport(auth, channel, dates) {
  const [daily, contentType, searchTraffic, uploadResult, topContent, uploadedVideoViews, uploadedVideoSubscribers] = await Promise.all([
    analyticsRows(auth, {
      ids: `channel==${channel.id}`,
      startDate: dates.startDate,
      endDate: dates.endDate,
      dimensions: "day",
      metrics: "views,subscribersGained,subscribersLost,averageViewPercentage,shares",
      sort: "day",
    }),
    analyticsRows(auth, {
      ids: `channel==${channel.id}`,
      startDate: dates.startDate,
      endDate: dates.endDate,
      dimensions: "day,creatorContentType",
      metrics: "views",
      sort: "day",
    }).catch(() => []),
    analyticsRows(auth, {
      ids: `channel==${channel.id}`,
      startDate: dates.startDate,
      endDate: dates.endDate,
      dimensions: "day,insightTrafficSourceType",
      metrics: "views",
      sort: "day",
    }).catch(() => []),
    publishedContentResult(auth, channel, dates),
    topVideos(auth, channel.id, dates),
    uploadedVideoViewsById(auth, channel.id, dates),
    uploadedVideoSubscribersById(auth, channel.id, dates),
  ]);

  return {
    channel,
    daily,
    contentType,
    searchTraffic,
    uploads: uploadResult.items,
    uploadsKnown: uploadResult.known,
    topContent,
    uploadedVideoViews,
    uploadedVideoSubscribers,
  };
}

async function cachedChannelReport(auth, channel, dates) {
  return cached(
    makeCacheKey("channel-report", channel.id, dates.startDate, dates.endDate),
    ttl.dashboard,
    () => channelReport(auth, channel, dates)
  );
}

async function analyticsRows(auth, params) {
  const analytics = google.youtubeAnalytics({ version: "v2", auth });
  const response = await analytics.reports.query(params);
  return response.data.rows || [];
}

async function topVideos(auth, channelId, dates) {
  const rows = await analyticsRows(auth, {
    ids: `channel==${channelId}`,
    startDate: dates.startDate,
    endDate: dates.endDate,
    dimensions: "video",
    metrics: "subscribersGained,views",
    sort: "-subscribersGained",
    maxResults: 12,
  }).catch(() => []);
  const ids = rows.map((row) => row[0]).filter(Boolean);
  const details = await videoDetails(auth, ids);
  return rows.slice(0, 12).map((row) => ({
    id: row[0],
    title: details[row[0]]?.title || row[0],
    channelId,
    channelTitle: details[row[0]]?.channelTitle || "",
    format: details[row[0]]?.format || "Video",
    subscribers: Number(row[1] || 0),
    views: Number(row[2] || 0),
  }));
}

async function searchKeywordsForDate(entries, date) {
  const totals = new Map();
  await Promise.all(entries.map(async (entry) => {
    const rows = await analyticsRows(entry.auth, {
      ids: `channel==${entry.channel.id}`,
      startDate: date,
      endDate: date,
      dimensions: "insightTrafficSourceDetail",
      metrics: "views",
      filters: "insightTrafficSourceType==YT_SEARCH",
      sort: "-views",
      maxResults: 10,
    }).catch(() => []);
    for (const row of rows) {
      const keyword = String(row[0] || "").trim();
      if (!keyword) continue;
      totals.set(keyword, (totals.get(keyword) || 0) + Number(row[1] || 0));
    }
  }));
  return [...totals.entries()]
    .map(([keyword, views]) => ({ keyword, views }))
    .sort((a, b) => b.views - a.views)
    .slice(0, 10);
}

async function uploadedVideoViewsById(auth, channelId, dates) {
  const rows = await analyticsRows(auth, {
    ids: `channel==${channelId}`,
    startDate: dates.startDate,
    endDate: dates.endDate,
    dimensions: "video",
    metrics: "views",
    sort: "-views",
    maxResults: 500,
  }).catch(() => []);
  return Object.fromEntries(rows.map((row) => [row[0], Number(row[1] || 0)]));
}

async function uploadedVideoSubscribersById(auth, channelId, dates) {
  const rows = await analyticsRows(auth, {
    ids: `channel==${channelId}`,
    startDate: dates.startDate,
    endDate: dates.endDate,
    dimensions: "video",
    metrics: "subscribersGained",
    sort: "-subscribersGained",
    maxResults: 500,
  }).catch(() => []);
  return Object.fromEntries(rows.map((row) => [row[0], Number(row[1] || 0)]));
}

async function publishedContent(auth, channel, dates) {
  if (!channel.uploadsPlaylistId) return [];
  return cached(
    makeCacheKey("uploads-playlist", channel.id, channel.uploadsPlaylistId, dates.startDate, dates.endDate),
    ttl.uploads,
    () => loadPublishedContent(auth, channel, dates)
  );
}

async function publishedContentResult(auth, channel, dates) {
  try {
    return { known: true, items: await publishedContent(auth, channel, dates) };
  } catch (error) {
    console.warn(`Could not load uploads playlist for ${channel.id}:`, error.message);
    return { known: false, items: [] };
  }
}

async function loadPublishedContent(auth, channel, dates) {
  const youtube = google.youtube({ version: "v3", auth });
  const videos = [];
  let pageToken;
  let reachedOlderVideos = false;
  do {
    const response = await youtube.playlistItems.list({
      part: ["snippet", "contentDetails"],
      playlistId: channel.uploadsPlaylistId,
      maxResults: 50,
      pageToken,
    });
    for (const item of response.data.items || []) {
      const publishedAt = item.contentDetails?.videoPublishedAt || item.snippet?.publishedAt;
      if (!publishedAt) continue;
      const dateOnly = publishedAt.slice(0, 10);
      if (dateOnly < dates.startDate) {
        reachedOlderVideos = true;
        break;
      }
      if (dateOnly <= dates.endDate) {
        videos.push({ id: item.contentDetails?.videoId, publishedAt, date: dateOnly, title: item.snippet?.title || "Untitled" });
      }
    }
    pageToken = reachedOlderVideos ? undefined : response.data.nextPageToken;
  } while (pageToken && videos.length < 500);

  const details = await videoDetails(auth, videos.map((video) => video.id));
  return videos.map((video) => ({
    ...video,
    format: details[video.id]?.format || "Video",
    views: Number(details[video.id]?.views || 0),
  }));
}

async function videoDetails(auth, ids) {
  const cleanIds = [...new Set(ids.filter(Boolean))];
  const detailMap = {};
  for (let index = 0; index < cleanIds.length; index += 50) {
    const chunk = cleanIds.slice(index, index + 50).sort();
    const chunkDetails = await cached(
      makeCacheKey("owned-video-details", chunk),
      ttl.videoDetails,
      async () => {
      const youtube = google.youtube({ version: "v3", auth });
      const response = await youtube.videos.list({
        part: ["snippet", "contentDetails", "liveStreamingDetails", "statistics"],
          id: chunk,
          maxResults: 50,
      });
        return (response.data.items || []).map((item) => ({
        id: item.id,
        title: item.snippet?.title || item.id,
        channelTitle: item.snippet?.channelTitle || "",
        format: classifyVideo(item),
        views: Number(item.statistics?.viewCount || 0),
        }));
      }
    );
    for (const detail of chunkDetails) {
      detailMap[detail.id] = detail;
    }
  }
  return detailMap;
}

function classifyVideo(video) {
  const title = `${video.snippet?.title || ""} ${(video.snippet?.tags || []).join(" ")}`.toLowerCase();
  if (
    video.liveStreamingDetails ||
    ["live", "upcoming"].includes(video.snippet?.liveBroadcastContent) ||
    /\b(live|livestream|live stream|streamed|premiere)\b/.test(title)
  ) {
    return "Live";
  }
  const seconds = isoDurationSeconds(video.contentDetails?.duration || "PT0S");
  if (/#shorts?\b|\bshorts?\b/.test(title)) return "Shorts";
  return seconds > 0 && seconds <= 180 ? "Shorts" : "Video";
}

function mergeReports(reports, days) {
  const labels = eachDate(days.startDate, days.endDate);
  const series = labels.map((date) => ({
    label: formatDate(date),
    date,
    views: { videos: 0, shorts: 0, live: 0, posts: 0, total: 0 },
    youtubeSearchViews: 0,
    adViews: 0,
    organicViews: 0,
    shares: 0,
    subscribers: 0,
    ctrNumerator: 0,
    ctrWeight: 0,
    ctr: 0,
    uploadsKnown: reports.length ? reports.every((report) => report.uploadsKnown !== false) : true,
    uploads: { videos: 0, shorts: 0, live: 0, posts: 0, total: 0 },
    publishedViews: { videos: 0, shorts: 0, live: 0, posts: 0, total: 0 },
    content: [],
  }));
  const byDate = new Map(series.map((day) => [day.date, day]));
  const fallbackShares = fallbackViewShares(reports);

  for (const report of reports) {
    for (const row of report.daily) {
      const day = byDate.get(row[0]);
      if (!day) continue;
      const views = Number(row[1] || 0);
      const gained = Number(row[2] || 0);
      const lost = Number(row[3] || 0);
      const averageViewPercentage = Number(row[4] || 0);
      const shares = Number(row[5] || 0);
      day.views.total += views;
      day.subscribers += gained - lost;
      day.shares += shares;
      day.ctrNumerator += averageViewPercentage * Math.max(1, views);
      day.ctrWeight += Math.max(1, views);
    }
    for (const row of report.contentType) {
      const day = byDate.get(row[0]);
      if (!day) continue;
      const type = normalizeContentType(row[1]);
      if (!type) continue;
      day.views[type] += Number(row[2] || 0);
    }
    for (const row of report.searchTraffic) {
      const day = byDate.get(row[0]);
      if (!day) continue;
      const views = Number(row[2] || 0);
      if (row[1] === "YT_SEARCH") {
        day.youtubeSearchViews += views;
      }
      if (row[1] === "ADVERTISING") {
        day.adViews += views;
      }
    }
    for (const item of report.uploads) {
      const day = byDate.get(item.date);
      if (!day) continue;
      const key = item.format === "Shorts" ? "shorts" : item.format === "Live" ? "live" : "videos";
      const periodViews = Object.hasOwn(report.uploadedVideoViews || {}, item.id)
        ? Number(report.uploadedVideoViews[item.id] || 0)
        : Number(item.views || 0);
      day.uploads[key] += 1;
      day.uploads.total += 1;
      day.publishedViews[key] += periodViews;
      day.publishedViews.total += periodViews;
    }
  }

  for (const day of series) {
    const knownSplit = day.views.videos + day.views.shorts + day.views.live + day.views.posts;
    const shouldUseFallbackSplit = day.views.total && (
      !knownSplit ||
      (day.views.shorts + day.views.live === 0 && fallbackShares.shorts + fallbackShares.live > 0)
    );
    if (shouldUseFallbackSplit) {
      day.views.videos = Math.round(day.views.total * fallbackShares.videos);
      day.views.shorts = Math.round(day.views.total * fallbackShares.shorts);
      day.views.live = Math.max(0, day.views.total - day.views.videos - day.views.shorts);
    }
    day.views.total = day.views.videos + day.views.shorts + day.views.live + day.views.posts || day.views.total;
    day.organicViews = Math.max(0, day.views.total - day.adViews);
    day.ctr = day.ctrWeight ? day.ctrNumerator / day.ctrWeight : 0;
  }

  const totals = summarizeSeries(series);
  totals.uploadsKnown = reports.length ? reports.every((report) => report.uploadsKnown !== false) : true;
  const topContent = reports
    .flatMap((report) => report.topContent)
    .sort((a, b) => (b.subscribers - a.subscribers) || (b.views - a.views))
    .slice(0, 15);
  return { series, totals, topContent };
}

function buildAllInOneDashboard(entries, reports, dates) {
  const perChannel = reports.map((report, index) => {
    const totals = mergeReports([report], dates.days).totals;
    return {
      id: entries[index]?.channel.id || report.channel.id,
      name: entries[index]?.channel.name || report.channel.name,
      organicViews: Number(totals.organicViews || 0),
      subscribers: Number(totals.subscribers || 0),
    };
  });

  return {
    channelRankings: {
      organicViews: perChannel.slice().sort((a, b) => b.organicViews - a.organicViews).slice(0, 10),
      subscribers: perChannel.slice().sort((a, b) => b.subscribers - a.subscribers).slice(0, 10),
    },
  };
}

async function competitorAnalysis(channelId, competitorsByChannel, dates) {
  const competitors = channelId === "global"
    ? Object.values(competitorsByChannel).flat()
    : competitorsByChannel[channelId] || [];
  return Promise.all(competitors.slice(0, 6).map((item) => publicCompetitor(item, dates)));
}

async function publicCompetitor(item, dates) {
  if (!process.env.YOUTUBE_API_KEY) {
    return { ...item, views: null, uploads: null, topContent: [], recentContent: null, note: "Add YOUTUBE_API_KEY for public competitor data." };
  }
  const channel = item.channelId ? { id: item.channelId, name: item.name } : await findPublicChannel(item.name);
  if (!channel) return { ...item, views: null, uploads: null, topContent: [], recentContent: null, note: "Competitor channel not found." };
  const videos = await recentPublicVideos(channel.id, dates);
  const topContent = videos.sort((a, b) => b.views - a.views).slice(0, 3);
  const recentContent = videos
    .filter((video) => Date.now() - new Date(video.publishedAt).getTime() <= 3 * 60 * 60 * 1000)
    .sort((a, b) => b.views - a.views)[0] || null;
  const avg = videos.reduce((sum, video) => sum + video.views, 0) / Math.max(1, videos.length);
  return {
    ...item,
    channelId: channel.id,
    views: videos.reduce((sum, video) => sum + video.views, 0),
    uploads: videos.length,
    topContent,
    recentContent: recentContent ? { ...recentContent, lift: (recentContent.views / Math.max(1, avg)).toFixed(1), hoursAgo: Math.max(1, Math.round((Date.now() - new Date(recentContent.publishedAt).getTime()) / 3600000)) } : null,
  };
}

async function findPublicChannel(query) {
  const clean = query.replace(/^@/, "");
  if (/^UC[a-zA-Z0-9_-]{20,}$/.test(clean)) {
    return { id: clean, name: clean };
  }
  const channels = await searchPublicChannels(query);
  return channels[0] ? { id: channels[0].id, name: channels[0].name } : null;
}

async function searchPublicChannels(query) {
  const clean = query.replace(/^@/, "");
  if (/^UC[a-zA-Z0-9_-]{20,}$/.test(clean)) {
    const url = `https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id=${encodeURIComponent(clean)}&key=${process.env.YOUTUBE_API_KEY}`;
    const data = await fetchJson(url);
    return (data.items || []).map(publicChannelResult);
  }
  const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=1&q=${encodeURIComponent(clean)}&key=${process.env.YOUTUBE_API_KEY}`;
  const data = await fetchJson(url);
  const ids = (data.items || []).map((item) => item.id.channelId).filter(Boolean);
  if (!ids.length) return [];
  const detailsUrl = `https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id=${ids.join(",")}&key=${process.env.YOUTUBE_API_KEY}`;
  const details = await fetchJson(detailsUrl);
  return (details.items || []).map(publicChannelResult);
}

function publicChannelResult(item) {
  return {
    id: item.id?.channelId || item.id,
    name: item.snippet?.title || item.id,
    handle: item.snippet?.customUrl || item.snippet?.handle || "",
    thumbnail: item.snippet?.thumbnails?.default?.url || "",
    subscribers: Number(item.statistics?.subscriberCount || 0),
  };
}

async function recentPublicVideos(channelId, dates) {
  const searchUrl = `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${channelId}&type=video&order=date&publishedAfter=${dates.startDate}T00:00:00Z&maxResults=25&key=${process.env.YOUTUBE_API_KEY}`;
  const searchData = await fetchJson(searchUrl);
  const ids = (searchData.items || []).map((item) => item.id.videoId).filter(Boolean);
  if (!ids.length) return [];
  const videoUrl = `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id=${ids.join(",")}&key=${process.env.YOUTUBE_API_KEY}`;
  const videoData = await fetchJson(videoUrl);
  return (videoData.items || []).map((item) => ({
    id: item.id,
    title: item.snippet?.title || item.id,
    publishedAt: item.snippet?.publishedAt,
    views: Number(item.statistics?.viewCount || 0),
  })).filter((item) => item.publishedAt?.slice(0, 10) <= dates.endDate);
}

async function categoryCompetitorReport(category, mappings, dates) {
  const connectedEntries = await connectedChannelEntries().catch(() => []);
  const ownedChannelIds = [...new Set(connectedEntries.map((entry) => entry.channel.id))];
  const groups = await Promise.all(mappings.map(async (mapping) => {
    const channelDetails = await publicChannelsByIds(mapping.ids);
    const channelReports = await Promise.all(mapping.ids.map((channelId) => publicChannelVideos(channelId, dates, mapping.group, channelDetails[channelId]?.title || channelId)));
    const videos = channelReports.flat();
    return {
      name: mapping.group,
      channels: mapping.ids.map((id) => ({ id, title: channelDetails[id]?.title || id })),
      videos,
      views: videos.reduce((sum, video) => sum + video.views, 0),
      engagement: engagementRate(videos),
      uploads: formatCounts(videos),
      averageViews: formatAverages(videos),
      viewSource: "publishedContentPublic",
    };
  }));
  const allVideos = groups.flatMap((group) => group.videos);
  const last24Cutoff = Date.now() - 24 * 60 * 60 * 1000;
  return {
    available: true,
    category,
    dates,
    ownedChannelIds,
    groups: groups.map(({ videos, ...group }) => group),
    top: {
      shorts: topPublicContent(allVideos, "Shorts"),
      videos: topPublicContent(allVideos, "Video"),
      live: topPublicContent(allVideos, "Live"),
      last24: allVideos
        .filter((video) => new Date(video.publishedAt).getTime() >= last24Cutoff)
        .sort((a, b) => b.views - a.views)
        .slice(0, 10),
    },
  };
}

async function publicChannelsByIds(ids) {
  const uniqueIds = [...new Set(ids.filter(Boolean))];
  const details = {};
  for (let index = 0; index < uniqueIds.length; index += 50) {
    const chunk = uniqueIds.slice(index, index + 50).sort();
    const chunkDetails = await cached(
      makeCacheKey("public-channels", chunk),
      ttl.publicChannels,
      async () => {
        const url = `https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id=${chunk.join(",")}&key=${process.env.YOUTUBE_API_KEY}`;
        const data = await fetchJson(url);
        return (data.items || []).map((item) => ({
          id: item.id,
          title: item.snippet?.title || item.id,
          totalViews: Number(item.statistics?.viewCount || 0),
          subscribers: Number(item.statistics?.subscriberCount || 0),
        }));
      }
    );
    for (const item of chunkDetails) {
      details[item.id] = {
        title: item.title,
        totalViews: item.totalViews,
        subscribers: item.subscribers,
      };
    }
  }
  return details;
}

async function publicChannelVideos(channelId, dates, group, channelTitle) {
  return cached(
    makeCacheKey("public-channel-videos", channelId, group, dates.startDate, dates.endDate),
    ttl.publicVideos,
    () => loadPublicChannelVideos(channelId, dates, group, channelTitle)
  );
}

async function loadPublicChannelVideos(channelId, dates, group, channelTitle) {
  const after = `${dates.startDate}T00:00:00Z`;
  const beforeDate = new Date(`${dates.endDate}T00:00:00Z`);
  beforeDate.setUTCDate(beforeDate.getUTCDate() + 1);
  const before = beforeDate.toISOString();
  const searchUrl = `https://www.googleapis.com/youtube/v3/search?part=snippet&channelId=${channelId}&type=video&order=date&publishedAfter=${after}&publishedBefore=${before}&maxResults=50&key=${process.env.YOUTUBE_API_KEY}`;
  const searchData = await fetchJson(searchUrl);
  const ids = (searchData.items || []).map((item) => item.id.videoId).filter(Boolean);
  if (!ids.length) return [];
  const detailUrl = `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails,liveStreamingDetails&id=${ids.join(",")}&key=${process.env.YOUTUBE_API_KEY}`;
  const detailData = await fetchJson(detailUrl);
  return (detailData.items || []).map((item) => {
    const format = classifyPublicVideo(item);
    return {
      id: item.id,
      title: item.snippet?.title || item.id,
      channelTitle,
      group,
      channelId,
      format,
      publishedAt: item.snippet?.publishedAt,
      views: Number(item.statistics?.viewCount || 0),
      likes: Number(item.statistics?.likeCount || 0),
      comments: Number(item.statistics?.commentCount || 0),
    };
  });
}

async function researchKeywordVideos(keyword, range) {
  const timeframe = researchWindow(range);
  const normalizedKeyword = keyword.toLowerCase();
  const results = [];
  const seen = new Set();
  let pageToken = "";
  let pageCount = 0;
  while (pageCount < 5 && results.length < 120) {
    const params = new URLSearchParams({
      part: "snippet",
      q: keyword,
      type: "video",
      order: "viewCount",
      publishedAfter: timeframe.startIso,
      maxResults: "50",
      key: process.env.YOUTUBE_API_KEY,
    });
    if (pageToken) params.set("pageToken", pageToken);
    const data = await fetchJson(`https://www.googleapis.com/youtube/v3/search?${params.toString()}`);
    for (const item of data.items || []) {
      const id = item.id?.videoId;
      if (!id || seen.has(id)) continue;
      seen.add(id);
      results.push({ id, snippet: item.snippet });
    }
    pageToken = data.nextPageToken || "";
    pageCount += 1;
    if (!pageToken) break;
  }

  if (!results.length) return [];
  const details = await publicVideoDetails(results.map((item) => item.id));
  const channelIds = [...new Set(results.map((item) => item.snippet?.channelId).filter(Boolean))];
  const channelStats = await publicChannelsByIds(channelIds);
  return results
    .map((item) => {
      const detail = details[item.id];
      if (!detail) return null;
      const title = detail.title || item.snippet?.title || item.id;
      if (!title.toLowerCase().includes(normalizedKeyword)) return null;
      const subscribers = Number(channelStats[detail.channelId]?.subscribers || 0);
      return {
        id: item.id,
        title,
        channelId: detail.channelId,
        channelTitle: detail.channelTitle || item.snippet?.channelTitle || detail.channelId,
        format: detail.format,
        publishedAt: detail.publishedAt,
        views: detail.views,
        likes: detail.likes,
        comments: detail.comments,
        subscribers,
        outlierScore: subscribers ? detail.views / subscribers : 0,
        url: `https://www.youtube.com/watch?v=${item.id}`,
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.views - a.views)
    .slice(0, 20);
}

async function publicVideoDetails(ids) {
  const cleanIds = [...new Set(ids.filter(Boolean))];
  const detailMap = {};
  for (let index = 0; index < cleanIds.length; index += 50) {
    const chunk = cleanIds.slice(index, index + 50).sort();
    const chunkDetails = await cached(
      makeCacheKey("public-video-details", chunk),
      ttl.videoDetails,
      async () => {
      const params = new URLSearchParams({
        part: "snippet,statistics,contentDetails,liveStreamingDetails",
          id: chunk.join(","),
        key: process.env.YOUTUBE_API_KEY,
      });
      const data = await fetchJson(`https://www.googleapis.com/youtube/v3/videos?${params.toString()}`);
        return (data.items || []).map((item) => ({
        id: item.id,
        title: item.snippet?.title || item.id,
        channelId: item.snippet?.channelId || "",
        channelTitle: item.snippet?.channelTitle || "",
        publishedAt: item.snippet?.publishedAt || "",
        format: classifyPublicVideo(item),
        views: Number(item.statistics?.viewCount || 0),
        likes: Number(item.statistics?.likeCount || 0),
        comments: Number(item.statistics?.commentCount || 0),
        }));
      }
    );
    for (const detail of chunkDetails) {
      detailMap[detail.id] = detail;
    }
  }
  return detailMap;
}

function classifyPublicVideo(video) {
  const title = `${video.snippet?.title || ""} ${(video.snippet?.tags || []).join(" ")}`.toLowerCase();
  if (
    video.liveStreamingDetails ||
    ["live", "upcoming"].includes(video.snippet?.liveBroadcastContent) ||
    /\b(live|livestream|live stream|streamed|premiere)\b/.test(title)
  ) {
    return "Live";
  }
  const seconds = isoDurationSeconds(video.contentDetails?.duration || "PT0S");
  if (/#shorts?\b|\bshorts?\b/.test(title)) return "Shorts";
  return seconds > 0 && seconds <= 180 ? "Shorts" : "Video";
}

function formatCounts(videos) {
  return {
    shorts: videos.filter((video) => video.format === "Shorts").length,
    videos: videos.filter((video) => video.format === "Video").length,
    live: videos.filter((video) => video.format === "Live").length,
    total: videos.length,
  };
}

function formatAverages(videos) {
  return {
    shorts: averagePublicViews(videos, "Shorts"),
    videos: averagePublicViews(videos, "Video"),
    live: averagePublicViews(videos, "Live"),
  };
}

function engagementRate(videos) {
  const views = videos.reduce((sum, video) => sum + Number(video.views || 0), 0);
  const likes = videos.reduce((sum, video) => sum + Number(video.likes || 0), 0);
  const comments = videos.reduce((sum, video) => sum + Number(video.comments || 0), 0);
  if (!views) return 0;
  return ((likes + comments) / views) * 100;
}

function averagePublicViews(videos, format) {
  const items = videos.filter((video) => video.format === format);
  return Math.round(items.reduce((sum, video) => sum + video.views, 0) / Math.max(1, items.length));
}

function topPublicContent(videos, format) {
  return videos.filter((video) => video.format === format).sort((a, b) => b.views - a.views).slice(0, 10);
}

async function fetchJson(url) {
  const response = await fetch(url);
  const data = await response.json();
  if (!response.ok) throw new Error(data.error?.message || "YouTube public API request failed.");
  return data;
}

async function fetchAnthropicJson(body) {
  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": process.env.ANTHROPIC_API_KEY,
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify(body),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error?.message || "Anthropic request failed.");
  }
  return data;
}

async function suggestTopicsFromResearch(keyword, range, items) {
  const prompt = [
    `Keyword: ${keyword}`,
    `Time frame: ${researchRangeLabel(range)}`,
    "Top videos:",
    ...items.map((item, index) => `${index + 1}. [${item.format}] ${item.title} | ${item.channelTitle} | ${item.views} views | outlier ${Number(item.outlierScore || 0).toFixed(2)}x`),
    "",
    "Suggest 5 best content ideas we can publish right now.",
    "Return strict JSON array only.",
    'Each item must be: {"title":"...","format":"Shorts|Video|Live","reason":"one concise sentence"}',
    "Use a mix of shorts, video, and live when justified by the winners.",
  ].join("\n");

  const data = await fetchAnthropicJson({
    model: "claude-sonnet-4-20250514",
    max_tokens: 900,
    messages: [{ role: "user", content: prompt }],
  });
  const text = (data.content || []).map((item) => item.text || "").join("\n").trim();
  const match = text.match(/\[[\s\S]*\]/);
  if (!match) throw new Error("Claude did not return valid topic ideas.");
  const parsed = JSON.parse(match[0]);
  if (!Array.isArray(parsed)) throw new Error("Claude did not return a topic list.");
  return parsed.slice(0, 5).map((item) => ({
    title: String(item.title || "Untitled idea"),
    format: normalizeResearchFormat(item.format),
    reason: String(item.reason || "Built from the current top-performing videos."),
  }));
}

function buildLiveInsights(competitors, merged) {
  const top = competitors.filter((item) => item.topContent?.length).sort((a, b) => (b.topContent[0]?.views || 0) - (a.topContent[0]?.views || 0))[0];
  const recent = competitors.filter((item) => item.recentContent).sort((a, b) => b.recentContent.views - a.recentContent.views)[0];
  const bestFormat = ["shorts", "videos", "live"].sort((a, b) => {
    const avgA = merged.totals.views[a] / Math.max(1, merged.totals.uploads[a]);
    const avgB = merged.totals.views[b] / Math.max(1, merged.totals.uploads[b]);
    return avgB - avgA;
  })[0];
  return [
    top
      ? { title: "Missing", body: `${top.name} uploaded "${top.topContent[0].title}" and it has ${top.topContent[0].views.toLocaleString()} public views in this period.` }
      : { title: "Missing", body: "Add competitor channels and a YouTube API key to detect competitor uploads you have not covered." },
    recent
      ? { title: "Live opportunity", body: `${recent.name} posted "${recent.recentContent.title}" ${recent.recentContent.hoursAgo} hours ago, performing ${recent.recentContent.lift}x above their recent average.` }
      : { title: "Live opportunity", body: "No competitor update from the last 2-3 hours was found yet. Keep this panel open during breaking update windows." },
    { title: "Evergreen topic", body: `Build one long-term ${formatsLabel(bestFormat)} pillar around recurring audience problems, then clip it into Shorts and live follow-ups.` },
  ];
}

function researchWindow(range) {
  const now = new Date();
  const start = new Date(now);
  if (range === "48h") {
    start.setHours(start.getHours() - 48);
  } else if (range === "30d") {
    start.setDate(start.getDate() - 30);
  } else if (range === "90d") {
    start.setDate(start.getDate() - 90);
  } else {
    start.setDate(start.getDate() - 365);
  }
  return { startIso: start.toISOString() };
}

function researchRangeLabel(range) {
  return {
    "48h": "Last 48 hours",
    "30d": "Last 30 days",
    "90d": "Last 90 days",
    "365d": "Last 365 days",
  }[range] || range;
}

function normalizeResearchFormat(value) {
  const text = String(value || "").trim().toLowerCase();
  if (text === "short" || text === "shorts") return "Shorts";
  if (text === "live") return "Live";
  return "Video";
}

function dateWindow(range, monthValue = "") {
  const today = new Date();
  const end = new Date(today);
  end.setDate(end.getDate() - 1);
  let start = new Date(end);
  if (range === "selectMonth" && /^\d{4}-\d{2}$/.test(monthValue)) {
    const [year, month] = monthValue.split("-").map(Number);
    start = new Date(Date.UTC(year, month - 1, 1));
    const monthEnd = new Date(Date.UTC(year, month, 0));
    end.setTime(Math.min(monthEnd.getTime(), end.getTime()));
  } else if (range === "month") {
    start.setDate(1);
  } else {
    start.setDate(end.getDate() - Number(range || 7) + 1);
  }
  return { startDate: isoDate(start), endDate: isoDate(end), days: { startDate: isoDate(start), endDate: isoDate(end) } };
}

function isoDateUtc(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()))
    .toISOString()
    .slice(0, 10);
}

function shiftIsoDate(dateText, days) {
  const date = new Date(`${dateText}T00:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function comparisonWindow(dates, range) {
  const start = new Date(`${dates.startDate}T00:00:00Z`);
  const end = new Date(`${dates.endDate}T00:00:00Z`);
  if (range === "selectMonth") {
    const previousStart = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth() - 1, 1));
    const previousEnd = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), 0));
    return { startDate: isoDate(previousStart), endDate: isoDate(previousEnd), days: { startDate: isoDate(previousStart), endDate: isoDate(previousEnd) } };
  }
  const days = Math.max(1, Math.round((end - start) / 86400000) + 1);
  const previousEnd = new Date(start);
  previousEnd.setUTCDate(previousEnd.getUTCDate() - 1);
  const previousStart = new Date(previousEnd);
  previousStart.setUTCDate(previousEnd.getUTCDate() - days + 1);
  return { startDate: isoDate(previousStart), endDate: isoDate(previousEnd), days: { startDate: isoDate(previousStart), endDate: isoDate(previousEnd) } };
}

function eachDate(startDate, endDate) {
  const dates = [];
  const cursor = new Date(`${startDate}T00:00:00Z`);
  const end = new Date(`${endDate}T00:00:00Z`);
  while (cursor <= end) {
    dates.push(isoDate(cursor));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return dates;
}

function summarizeSeries(series) {
  return series.reduce((acc, day) => {
    for (const key of ["videos", "shorts", "live", "posts", "total"]) {
      acc.views[key] += day.views[key] || 0;
      acc.uploads[key] += day.uploads[key] || 0;
      acc.publishedViews[key] += day.publishedViews[key] || 0;
    }
    acc.youtubeSearchViews += day.youtubeSearchViews || 0;
    acc.adViews += day.adViews || 0;
    acc.organicViews += day.organicViews || 0;
    acc.shares += day.shares || 0;
    acc.subscribers += day.subscribers;
    acc.ctrNumerator += day.ctr * Math.max(1, day.ctrWeight);
    acc.ctrWeight += Math.max(1, day.ctrWeight);
    acc.ctr = acc.ctrWeight ? acc.ctrNumerator / acc.ctrWeight : 0;
    return acc;
  }, {
    views: { videos: 0, shorts: 0, live: 0, posts: 0, total: 0 },
    uploads: { videos: 0, shorts: 0, live: 0, posts: 0, total: 0 },
    publishedViews: { videos: 0, shorts: 0, live: 0, posts: 0, total: 0 },
    youtubeSearchViews: 0,
    adViews: 0,
    organicViews: 0,
    shares: 0,
    subscribers: 0,
    ctr: 0,
    ctrNumerator: 0,
    ctrWeight: 0,
  });
}

function normalizeContentType(value) {
  const normalized = String(value || "").replace(/[_\s-]/g, "").toLowerCase();
  if (normalized === "shorts" || normalized === "short") return "shorts";
  if (normalized === "livestream" || normalized === "live") return "live";
  if (normalized === "videoondemand" || normalized === "video" || normalized === "upload") return "videos";
  if (normalized === "posts" || normalized === "post" || normalized === "communitypost") return "posts";
  return null;
}

function fallbackViewShares(reports) {
  const split = { videos: 0, shorts: 0, live: 0 };
  for (const report of reports) {
    for (const item of report.topContent || []) {
      const key = item.format === "Shorts" ? "shorts" : item.format === "Live" ? "live" : "videos";
      split[key] += Number(item.views || 0);
    }
  }

  let total = split.videos + split.shorts + split.live;
  if (!total) {
    for (const report of reports) {
      for (const item of report.uploads || []) {
        const key = item.format === "Shorts" ? "shorts" : item.format === "Live" ? "live" : "videos";
        split[key] += 1;
      }
    }
    total = split.videos + split.shorts + split.live;
  }

  if (!total) return { videos: 1, shorts: 0, live: 0 };
  return {
    videos: split.videos / total,
    shorts: split.shorts / total,
    live: split.live / total,
  };
}

function isoDurationSeconds(value) {
  const match = value.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!match) return 0;
  return Number(match[1] || 0) * 3600 + Number(match[2] || 0) * 60 + Number(match[3] || 0);
}

function isoDate(date) {
  return date.toISOString().slice(0, 10);
}

function formatDate(value) {
  const date = new Date(`${value}T00:00:00Z`);
  const dayMonth = date.toLocaleDateString("en-IN", { day: "numeric", month: "short" });
  const weekday = date.toLocaleDateString("en-IN", { weekday: "long" });
  return `${dayMonth}, ${weekday}`;
}

function formatsLabel(key) {
  return ({ shorts: "Shorts", videos: "video", live: "live" })[key] || "video";
}
