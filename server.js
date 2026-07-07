import "dotenv/config";
import express from "express";
import { google } from "googleapis";
import postgres from "postgres";
import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import fs from "node:fs";
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
const removedChannelsPath = path.join(dataDir, "removed-channels.json");
const envPath = path.join(__dirname, ".env");
const assetDir = path.join(__dirname, "assets");
const maxChannels = Number(process.env.MAX_CONNECTED_CHANNELS || 200);
const databaseUrl = process.env.DATABASE_URL || process.env.POSTGRES_URL || "";
const sql = databaseUrl ? postgres(databaseUrl, { prepare: false }) : null;
let storageReadyPromise;
const responseCache = new Map();
const inFlightRequests = new Map();
const ttl = {
  dashboard: 4 * 60 * 60 * 1000,
  uploads: 30 * 60 * 1000,
  videoDetails: 60 * 60 * 1000,
  publicChannels: 24 * 60 * 60 * 1000,
  publicVideos: 12 * 60 * 60 * 1000, // 12 hours
  research: 6 * 60 * 60 * 1000,
  competitors: 12 * 60 * 60 * 1000, // 12 hours
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
    { group: "Testbook", ids: ["UCGZmxSKg2tKMvn9TO-IYpqw"] },
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
  Tamil: [
    { group: "Testbook Tamil", ids: ["UC-a42jy3Ow5RTLGBQwa2y8g"] },
    { group: "VERANDA RACE", ids: ["UCaOd6Iy8peqYR8_wDBAcXaQ"] },
    { group: "Adda247 Tamil (SSC Railway)", ids: ["UCqDr2ZYjgARlzcRi5OEqCGA"] },
    { group: "Adda247 Tamil", ids: ["UCmJXBP6ccOwCodih8RZDGkQ"] },
    { group: "Genius SSC", ids: ["UCHGCINM4m3SpGyPI4i66b7Q"] },
    { group: "Learn with Vignesh", ids: ["UCk-VE43GWfhckqRrnM16kZA"] },
    { group: "Chandru Maths", ids: ["UCGpQ0YuawhirWPKFnXcTh5A"] },
  ],
  "JAIIB CAIIB": [
    { group: "Testbook JAIIB CAIIB", ids: ["UCqY1_5OPoTn2QoWL7mbiI_Q"] },
    { group: "PW JAIIB", ids: ["UC2TJx6PrcReByvidr7-tdSQ"] },
    { group: "Oliveboard JAIIB", ids: ["UC18uJ0aohtMg5BLRcdwoGkw"] },
    { group: "Mahesh Sir JAIIB", ids: ["UCkOSmpPtkRc86R9W_iw2TDw"] },
    { group: "Edutap JAIIB", ids: ["UC922p9PusjDy40qBsmypzYA"] },
    { group: "Adda247 JAIIB", ids: ["UCxykVmXr1GwZZZxKG8ZRaUg"] },
  ],
};

const scopes = [
  "https://www.googleapis.com/auth/youtube.readonly",
  "https://www.googleapis.com/auth/youtube.force-ssl",
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
  const viewer = readViewerSession(req);
  res.json({
    ...configStatus(),
    viewer,
    teamAuthEnabled: teamAuthEnabled(),
    allowedEmailDomain: allowedEmailDomain(),
    viewerAllowlistEnabled: allowedViewerEmails().length > 0,
    isAuditAdmin: isAuditAdmin(viewer),
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
  const viewer = readViewerSession(req);
  res.json({
    ...configStatus(),
    connected: await hasConnectedGoogle(),
    maxChannels,
    viewer,
    teamAuthEnabled: teamAuthEnabled(),
    allowedEmailDomain: allowedEmailDomain(),
    viewerAllowlistEnabled: allowedViewerEmails().length > 0,
    isAuditAdmin: isAuditAdmin(viewer),
    allowedToAddChannel: isAllowedToAddChannel(viewer),
  });
});

app.get("/api/session", async (req, res) => {
  const viewer = readViewerSession(req);
  res.json({
    viewer,
    teamAuthEnabled: teamAuthEnabled(),
    allowedEmailDomain: allowedEmailDomain(),
    viewerAllowlistEnabled: allowedViewerEmails().length > 0,
    isAuditAdmin: isAuditAdmin(viewer),
    allowedToAddChannel: isAllowedToAddChannel(viewer),
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

app.get("/auth/google", async (req, res) => {
  await hydrateEnvFromFile();
  if (!hasGoogleConfig()) {
    res.status(400).send("Missing Google OAuth config. Copy .env.example to .env and fill GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, and GOOGLE_REDIRECT_URI.");
    return;
  }

  const viewer = readViewerSession(req);
  if (!isAllowedToAddChannel(viewer)) {
    res.status(403).send("Access denied. You do not have permission to connect channels.");
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
    
    const viewer = readViewerSession(req);
    if (!isAllowedToAddChannel(viewer)) {
      res.status(403).send("Access denied. You do not have permission to connect channels.");
      return;
    }
    const userEmail = viewer ? viewer.email : null;

    const channels = await listOwnedChannels(oauth2Client);
    await saveGoogleProfile(tokens, channels, userEmail);
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
  await rm(removedChannelsPath, { force: true }).catch(() => {});
  res.json({ ok: true, ...configStatus() });
});

app.get("/api/channels", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);
    const entries = await connectedChannelEntries(viewer);
    res.json({ channels: publicChannels(entries) });
  } catch (error) {
    next(error);
  }
});

app.delete("/api/channels/:channelId", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);
    if (!isAuditAdmin(viewer)) {
      return res.status(403).json({ error: "Access denied. Only authorized admins can remove channels." });
    }

    const { channelId } = req.params;
    if (!channelId) {
      return res.status(400).json({ error: "Missing channelId" });
    }

    const removedIds = await readRemovedChannelIds();
    if (!removedIds.includes(channelId)) {
      removedIds.push(channelId);
      await saveRemovedChannelIds(removedIds);
    }

    res.json({ success: true });
  } catch (error) {
    next(error);
  }
});

app.get("/api/dashboard", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);
    const entries = await connectedChannelEntries(viewer);
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
    const viewer = readViewerSession(req);
    const entries = await connectedChannelEntries(viewer);
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

const quarterTargetsPath = path.join(dataDir, "quarter_targets.json");

const defaultTargets = {
  "AMJ_2026": {
    "ytm": [
      {
        "id": "ytm_t1",
        "employee": "Atul Sharma",
        "channelId": "UC1pJ8ods7vGboH2BXuZVBbQ",
        "channelName": "UPSC PrepLab",
        "viewsTarget": 1000000,
        "subsTarget": 10000
      },
      {
        "id": "ytm_t2",
        "employee": "Shubham",
        "channelId": "UC0FSg3oiJZlpTYgI0hCNazQ",
        "channelName": "Bihar Testbook",
        "viewsTarget": 5000000,
        "subsTarget": 25000
      },
      {
        "id": "ytm_t3",
        "employee": "Shubham",
        "channelId": "UCgM9qPLv7R-hTRQIGa4wgKA",
        "channelName": "Testbook",
        "viewsTarget": 25000000,
        "subsTarget": 110000
      },
      {
        "id": "ytm_t4",
        "employee": "Shubham",
        "channelId": "UC_fKmFGyY4MPVzz4TT_N_6Q",
        "channelName": "Banking Testbook",
        "viewsTarget": 4000000,
        "subsTarget": 12000
      },
      {
        "id": "ytm_t5",
        "employee": "Raubnish",
        "channelId": "UC1y7Nv1-ZdkJdNUkUgFaydg",
        "channelName": "Odisha Testbook",
        "viewsTarget": 18000000,
        "subsTarget": 35000
      },
      {
        "id": "ytm_t6",
        "employee": "Raubnish",
        "channelId": "UCUIaneuBNuiTdMQIyfOvUFA",
        "channelName": "Odisha Teaching by Testbook",
        "viewsTarget": 3000000,
        "subsTarget": 15000
      },
      {
        "id": "ytm_t7",
        "employee": "Narendra/Amit",
        "channelId": "UC_uR26BodKBZ4HVwxwd5isQ",
        "channelName": "UGC NET Testbook",
        "viewsTarget": 20000000,
        "subsTarget": 60000
      },
      {
        "id": "ytm_t8",
        "employee": "Narendra/Amit",
        "channelId": "UCXx7EB5fueJeOI5pYuYSagQ",
        "channelName": "Testbook NET JRF",
        "viewsTarget": 9000000,
        "subsTarget": 40000
      },
      {
        "id": "ytm_t9",
        "employee": "Abhinav",
        "channelId": "UCLLhVCsO2Em-IQXwqq2EVyw",
        "channelName": "TET PRT Testbook",
        "viewsTarget": 9000000,
        "subsTarget": 30000
      },
      {
        "id": "ytm_t10",
        "employee": "Abhinav",
        "channelId": "UCUdYrLrnPFVpvWM6hNJSnJg",
        "channelName": "TGT PGT Testbook",
        "viewsTarget": 4500000,
        "subsTarget": 10000
      },
      {
        "id": "ytm_t11",
        "employee": "Abhinav",
        "channelId": "UCYmiOXiMpiwlQFU4OIDPZzA",
        "channelName": "CTET Testbook",
        "viewsTarget": 3000000,
        "subsTarget": 15000
      },
      {
        "id": "ytm_t12",
        "employee": "Abhinav",
        "channelId": "UCaPCIXHm03fSYi3M3_LOf2Q",
        "channelName": "Bihar Teaching Exams by Testbook",
        "viewsTarget": 5000000,
        "subsTarget": 12000
      },
      {
        "id": "ytm_t13",
        "employee": "Shukendu",
        "channelId": "UCSzinFg4bwJktBgvmWKNP6Q",
        "channelName": "Testbook Bengali",
        "viewsTarget": 4000000,
        "subsTarget": 25000
      },
      {
        "id": "ytm_t14",
        "employee": "Shukendu",
        "channelId": "UC1pQayM4quYlDZbrhD6VEUg",
        "channelName": "WBPSC Testbook",
        "viewsTarget": 3000000,
        "subsTarget": 25000
      },
      {
        "id": "ytm_t15",
        "employee": "Ashish Tyagi",
        "channelId": "UCW3qu1ViFQhIMLZRXugCFRg",
        "channelName": "Punjab Testbook",
        "viewsTarget": 15000000,
        "subsTarget": 60000
      },
      {
        "id": "ytm_t16",
        "employee": "Lubna",
        "channelId": "UCuTgFUujt6tQXWxQMg-DHrQ",
        "channelName": "Railway Testbook",
        "viewsTarget": 23000000,
        "subsTarget": 100000
      },
      {
        "id": "ytm_t17",
        "employee": "Vivek",
        "channelId": "UCFTTIDN58laGeV8DM9D4spg",
        "channelName": "AE JE Testbook",
        "viewsTarget": 7000000,
        "subsTarget": 20000
      },
      {
        "id": "ytm_t18",
        "employee": "Vivek",
        "channelId": "UCGZmxSKg2tKMvn9TO-IYpqw",
        "channelName": "SSC Testbook",
        "viewsTarget": 10000000,
        "subsTarget": 75000
      }
    ],
    "seo": [
      {
        "id": "seo_t1",
        "employee": "Saijal",
        "channelId": "UCLLhVCsO2Em-IQXwqq2EVyw",
        "channelName": "TET PRT Testbook",
        "searchViewsTarget": 2500000
      },
      {
        "id": "seo_t2",
        "employee": "Saijal",
        "channelId": "UCUdYrLrnPFVpvWM6hNJSnJg",
        "channelName": "TGT PGT Testbook",
        "searchViewsTarget": 1230000
      },
      {
        "id": "seo_t3",
        "employee": "Saijal",
        "channelId": "UCYmiOXiMpiwlQFU4OIDPZzA",
        "channelName": "CTET Testbook",
        "searchViewsTarget": 675000
      },
      {
        "id": "seo_t4",
        "employee": "Saijal",
        "channelId": "UC_uR26BodKBZ4HVwxwd5isQ",
        "channelName": "UGC NET Testbook",
        "searchViewsTarget": 6200000
      },
      {
        "id": "seo_t5",
        "employee": "Saijal",
        "channelId": "UCXx7EB5fueJeOI5pYuYSagQ",
        "channelName": "Testbook NET JRF",
        "searchViewsTarget": 1530000
      },
      {
        "id": "seo_t6",
        "employee": "Saijal",
        "channelId": "UCaPCIXHm03fSYi3M3_LOf2Q",
        "channelName": "Bihar Teaching Exams by Testbook",
        "searchViewsTarget": 600000
      },
      {
        "id": "seo_t7",
        "employee": "Mohit",
        "channelId": "UC0FSg3oiJZlpTYgI0hCNazQ",
        "channelName": "Bihar Testbook",
        "searchViewsTarget": 1250000
      },
      {
        "id": "seo_t8",
        "employee": "Mohit",
        "channelId": "UCgM9qPLv7R-hTRQIGa4wgKA",
        "channelName": "Testbook",
        "searchViewsTarget": 6250000
      },
      {
        "id": "seo_t9",
        "employee": "Mohit",
        "channelId": "UC1pJ8ods7vGboH2BXuZVBbQ",
        "channelName": "UPSC PrepLab",
        "searchViewsTarget": 150000
      },
      {
        "id": "seo_t10",
        "employee": "Vinayak",
        "channelId": "UC_fKmFGyY4MPVzz4TT_N_6Q",
        "channelName": "Banking Testbook",
        "searchViewsTarget": 1080000
      },
      {
        "id": "seo_t11",
        "employee": "Vinayak",
        "channelId": "UCfG_bedD0HBbrFo1JpgUEqA",
        "channelName": "SuperCoaching MPSC by Testbook",
        "searchViewsTarget": 960000
      },
      {
        "id": "seo_t12",
        "employee": "Vinayak",
        "channelId": "UCuTgFUujt6tQXWxQMg-DHrQ",
        "channelName": "Railway Testbook",
        "searchViewsTarget": 6210000
      },
      {
        "id": "seo_t13",
        "employee": "Aditya",
        "channelId": "UCSzinFg4bwJktBgvmWKNP6Q",
        "channelName": "Testbook Bengali",
        "searchViewsTarget": 640000
      },
      {
        "id": "seo_t14",
        "employee": "Aditya",
        "channelId": "UC1pQayM4quYlDZbrhD6VEUg",
        "channelName": "WBPSC Testbook",
        "searchViewsTarget": 720000
      },
      {
        "id": "seo_t15",
        "employee": "Aditya",
        "channelId": "UCKtAel248rFM1Nxd1UJlQHA",
        "channelName": "SuperCoaching Marathi by Testbook",
        "searchViewsTarget": 1610000
      },
      {
        "id": "seo_t16",
        "employee": "Aditya",
        "channelId": "UCUIaneuBNuiTdMQIyfOvUFA",
        "channelName": "Odisha Teaching by Testbook",
        "searchViewsTarget": 480000
      },
      {
        "id": "seo_t17",
        "employee": "Aditya",
        "channelId": "UC1y7Nv1-ZdkJdNUkUgFaydg",
        "channelName": "Odisha Testbook",
        "searchViewsTarget": 2880000
      },
      {
        "id": "seo_t18",
        "employee": "Aditya",
        "channelId": "UCW3qu1ViFQhIMLZRXugCFRg",
        "channelName": "Punjab Testbook",
        "searchViewsTarget": 1950000
      }
    ]
  },
  "JAS_2026": {
    "ytm": [
      {
        "id": "ytm_t19_nitin_mpsc",
        "employee": "Nitin",
        "channelId": "UCfG_bedD0HBbrFo1JpgUEqA",
        "channelName": "SuperCoaching MPSC by Testbook",
        "viewsTarget": 7000000,
        "subsTarget": 12000
      },
      {
        "id": "ytm_t20_nitin_marathi",
        "employee": "Nitin",
        "channelId": "UCKtAel248rFM1Nxd1UJlQHA",
        "channelName": "SuperCoaching Marathi by Testbook",
        "viewsTarget": 7000000,
        "subsTarget": 20000
      },
      {
        "id": "ytm_t21_nitin_tet",
        "employee": "Nitin",
        "channelId": "UCcpVPJAwpfJlcGE1J84QXvA",
        "channelName": "TET Factory",
        "viewsTarget": 1000000,
        "subsTarget": 20000
      },
      {
        "id": "ytm_t22_shubham_bihar",
        "employee": "Shubham",
        "channelId": "UC0FSg3oiJZlpTYgI0hCNazQ",
        "channelName": "Bihar Testbook",
        "viewsTarget": 7000000,
        "subsTarget": 30000
      },
      {
        "id": "ytm_t23_shubham_testbook",
        "employee": "Shubham",
        "channelId": "UCgM9qPLv7R-hTRQIGa4wgKA",
        "channelName": "Testbook",
        "viewsTarget": 40000000,
        "subsTarget": 186000
      },
      {
        "id": "ytm_t24_shubham_banking",
        "employee": "Shubham",
        "channelId": "UC_fKmFGyY4MPVzz4TT_N_6Q",
        "channelName": "Banking Testbook",
        "viewsTarget": 8000000,
        "subsTarget": 30000
      },
      {
        "id": "ytm_t25_raubnish_odisha",
        "employee": "Raubnish",
        "channelId": "UC1y7Nv1-ZdkJdNUkUgFaydg",
        "channelName": "Odisha Testbook",
        "viewsTarget": 20000000,
        "subsTarget": 35000
      },
      {
        "id": "ytm_t26_raubnish_odishateach",
        "employee": "Raubnish",
        "channelId": "UCUIaneuBNuiTdMQIyfOvUFA",
        "channelName": "Odisha Teaching by Testbook",
        "viewsTarget": 1000000,
        "subsTarget": 25000
      },
      {
        "id": "ytm_t27_amit_narendra_combined",
        "employee": "Narendra/Amit",
        "channelIds": [
          "UC_uR26BodKBZ4HVwxwd5isQ",
          "UCXx7EB5fueJeOI5pYuYSagQ"
        ],
        "channelName": "UGC NET & NET JRF",
        "viewsTarget": 18000000,
        "subsTarget": 70000
      },
      {
        "id": "ytm_t28_abhinav_tetprt",
        "employee": "Abhinav",
        "channelId": "UCLLhVCsO2Em-IQXwqq2EVyw",
        "channelName": "TET PRT Testbook",
        "viewsTarget": 9000000,
        "subsTarget": 30000
      },
      {
        "id": "ytm_t29_abhinav_tgtpgt",
        "employee": "Abhinav",
        "channelId": "UCUdYrLrnPFVpvWM6hNJSnJg",
        "channelName": "TGT PGT Testbook",
        "viewsTarget": 5000000,
        "subsTarget": 10000
      },
      {
        "id": "ytm_t30_abhinav_ctet",
        "employee": "Abhinav",
        "channelId": "UCYmiOXiMpiwlQFU4OIDPZzA",
        "channelName": "CTET Testbook",
        "viewsTarget": 10000000,
        "subsTarget": 38000
      },
      {
        "id": "ytm_t31_abhinav_biharteach",
        "employee": "Abhinav",
        "channelId": "UCaPCIXHm03fSYi3M3_LOf2Q",
        "channelName": "Bihar Teaching Exams by Testbook",
        "viewsTarget": 7500000,
        "subsTarget": 10000
      },
      {
        "id": "ytm_t32_shukendu_bengali",
        "employee": "Shukendu",
        "channelId": "UCSzinFg4bwJktBgvmWKNP6Q",
        "channelName": "Testbook Bengali",
        "viewsTarget": 6000000,
        "subsTarget": 30000
      },
      {
        "id": "ytm_t33_shukendu_wbpsc",
        "employee": "Shukendu",
        "channelId": "UC1pQayM4quYlDZbrhD6VEUg",
        "channelName": "WBPSC Testbook",
        "viewsTarget": 4000000,
        "subsTarget": 27000
      },
      {
        "id": "ytm_t34_ashish_punjab",
        "employee": "Ashish Tyagi",
        "channelId": "UCW3qu1ViFQhIMLZRXugCFRg",
        "channelName": "Punjab Testbook",
        "viewsTarget": 25000000,
        "subsTarget": 50000
      },
      {
        "id": "ytm_t35_lubna_railway",
        "employee": "Lubna",
        "channelId": "UCuTgFUujt6tQXWxQMg-DHrQ",
        "channelName": "Railway Testbook",
        "viewsTarget": 25000000,
        "subsTarget": 70000
      },
      {
        "id": "ytm_t36_vivek_aeje",
        "employee": "Vivek",
        "channelId": "UCFTTIDN58laGeV8DM9D4spg",
        "channelName": "AE JE Testbook",
        "viewsTarget": 7500000,
        "subsTarget": 25000
      },
      {
        "id": "ytm_t37_vivek_ssc",
        "employee": "Vivek",
        "channelId": "UCGZmxSKg2tKMvn9TO-IYpqw",
        "channelName": "SSC Testbook",
        "viewsTarget": 12000000,
        "subsTarget": 80000
      },
      {
        "id": "ytm_t38_vivek_jaiib",
        "employee": "Vivek",
        "channelId": "UCqY1_5OPoTn2QoWL7mbiI_Q",
        "channelName": "Testbook - JAIIB CAIIB",
        "viewsTarget": 150000,
        "subsTarget": 600
      },
      {
        "id": "ytm_t39_govardhan_tamil",
        "employee": "Govardhan",
        "channelId": "UC-a42jy3Ow5RTLGBQwa2y8g",
        "channelName": "Testbook Tamil",
        "viewsTarget": 1800000,
        "subsTarget": 18000
      },
      {
        "id": "ytm_t40_govardhan_telugu",
        "employee": "Govardhan",
        "channelId": "UC08MiG8-bjllfy94OeY5zWw",
        "channelName": "Testbook Telugu",
        "viewsTarget": 1530000,
        "subsTarget": 12000
      }
    ],
    "seo": [
      {
        "id": "seo_t19_saijal_tetprt",
        "employee": "Saijal",
        "channelId": "UCLLhVCsO2Em-IQXwqq2EVyw",
        "channelName": "TET PRT Testbook",
        "searchViewsTarget": 2379960
      },
      {
        "id": "seo_t20_saijal_tgtpgt",
        "employee": "Saijal",
        "channelId": "UCUdYrLrnPFVpvWM6hNJSnJg",
        "channelName": "TGT PGT Testbook",
        "searchViewsTarget": 1327748
      },
      {
        "id": "seo_t21_saijal_ctet",
        "employee": "Saijal",
        "channelId": "UCYmiOXiMpiwlQFU4OIDPZzA",
        "channelName": "CTET Testbook",
        "searchViewsTarget": 2244039
      },
      {
        "id": "seo_t22_saijal_combined",
        "employee": "Saijal",
        "channelIds": [
          "UC_uR26BodKBZ4HVwxwd5isQ",
          "UCXx7EB5fueJeOI5pYuYSagQ"
        ],
        "channelName": "UGC NET & NET JRF",
        "searchViewsTarget": 4230900
      },
      {
        "id": "seo_t23_saijal_biharteach",
        "employee": "Saijal",
        "channelId": "UCaPCIXHm03fSYi3M3_LOf2Q",
        "channelName": "Bihar Teaching Exams by Testbook",
        "searchViewsTarget": 923624
      },
      {
        "id": "seo_t24_mohit_bihar",
        "employee": "Mohit",
        "channelId": "UC0FSg3oiJZlpTYgI0hCNazQ",
        "channelName": "Bihar Testbook",
        "searchViewsTarget": 1580288
      },
      {
        "id": "seo_t25_mohit_testbook",
        "employee": "Mohit",
        "channelId": "UCgM9qPLv7R-hTRQIGa4wgKA",
        "channelName": "Testbook",
        "searchViewsTarget": 10038040
      },
      {
        "id": "seo_t26_mohit_punjab",
        "employee": "Mohit",
        "channelId": "UCW3qu1ViFQhIMLZRXugCFRg",
        "channelName": "Punjab Testbook",
        "searchViewsTarget": 4000000
      },
      {
        "id": "seo_t27_vinayak_banking",
        "employee": "Vinayak",
        "channelId": "UC_fKmFGyY4MPVzz4TT_N_6Q",
        "channelName": "Banking Testbook",
        "searchViewsTarget": 2343827
      },
      {
        "id": "seo_t28_vinayak_mpsc",
        "employee": "Vinayak",
        "channelId": "UCfG_bedD0HBbrFo1JpgUEqA",
        "channelName": "SuperCoaching MPSC by Testbook",
        "searchViewsTarget": 1044109
      },
      {
        "id": "seo_t29_vinayak_railway",
        "employee": "Vinayak",
        "channelId": "UCuTgFUujt6tQXWxQMg-DHrQ",
        "channelName": "Railway Testbook",
        "searchViewsTarget": 5998456
      },
      {
        "id": "seo_t30_aditya_bengali",
        "employee": "Aditya",
        "channelId": "UCSzinFg4bwJktBgvmWKNP6Q",
        "channelName": "Testbook Bengali",
        "searchViewsTarget": 819798
      },
      {
        "id": "seo_t31_aditya_wbpsc",
        "employee": "Aditya",
        "channelId": "UC1pQayM4quYlDZbrhD6VEUg",
        "channelName": "WBPSC Testbook",
        "searchViewsTarget": 847632
      },
      {
        "id": "seo_t32_aditya_marathi",
        "employee": "Aditya",
        "channelId": "UCKtAel248rFM1Nxd1UJlQHA",
        "channelName": "SuperCoaching Marathi by Testbook",
        "searchViewsTarget": 1570271
      },
      {
        "id": "seo_t33_aditya_odishateach",
        "employee": "Aditya",
        "channelId": "UCUIaneuBNuiTdMQIyfOvUFA",
        "channelName": "Odisha Teaching by Testbook",
        "searchViewsTarget": 110087
      },
      {
        "id": "seo_t34_aditya_odisha",
        "employee": "Aditya",
        "channelId": "UC1y7Nv1-ZdkJdNUkUgFaydg",
        "channelName": "Odisha Testbook",
        "searchViewsTarget": 3744070
      },
      {
        "id": "seo_t35_aditya_tetfactory",
        "employee": "Aditya",
        "channelId": "UCcpVPJAwpfJlcGE1J84QXvA",
        "channelName": "TET Factory",
        "searchViewsTarget": 150000
      }
    ]
  }
};

async function readQuarterTargets() {
  let targets;
  if (sql) {
    await ensureStorage();
    const rows = await sql`select payload from app_state where key = 'quarter_targets' limit 1`;
    targets = rows[0]?.payload || defaultTargets;
  } else {
    try {
      targets = JSON.parse(await readFile(quarterTargetsPath, "utf8"));
    } catch {
      targets = defaultTargets;
    }
  }

  // Auto-migration: if targets has old/outdated JAS_2026 configuration, update it
  let needsSave = false;
  if (
    !targets.JAS_2026 ||
    !targets.JAS_2026.ytm ||
    targets.JAS_2026.ytm.some(t => t.employee === "Atul Sharma" || t.id === "ytm_t7" || (t.channelName === "CTET Testbook" && t.viewsTarget === 13000000))
  ) {
    targets.JAS_2026 = defaultTargets.JAS_2026;
    needsSave = true;
  }

  if (needsSave) {
    await saveQuarterTargets(targets);
  }

  return targets;
}

async function saveQuarterTargets(targets) {
  if (sql) {
    await ensureStorage();
    await sql`
      insert into app_state (key, payload)
      values ('quarter_targets', ${sql.json(targets)})
      on conflict (key) do update set payload = excluded.payload, updated_at = now()
    `;
    return;
  }
  await mkdir(dataDir, { recursive: true });
  await writeFile(quarterTargetsPath, JSON.stringify(targets, null, 2), "utf8");
}

async function channelTargetAnalytics(auth, channelId, startDate, endDate, options = {}) {
  return cached(
    makeCacheKey("channel-target-analytics-v2", channelId, startDate, endDate),
    2 * 60 * 60 * 1000, // 2 hours
    async () => {
      const [dailyRows, trafficRows] = await Promise.all([
        analyticsRows(auth, {
          ids: `channel==${channelId}`,
          startDate,
          endDate,
          dimensions: "day",
          metrics: "views,subscribersGained,subscribersLost",
          sort: "day",
        }).catch((err) => {
          console.error(`dailyRows fetch error for ${channelId}:`, err);
          return [];
        }),
        analyticsRows(auth, {
          ids: `channel==${channelId}`,
          startDate,
          endDate,
          dimensions: "day,insightTrafficSourceType",
          metrics: "views",
          sort: "day",
        }).catch((err) => {
          console.error(`trafficRows fetch error for ${channelId}:`, err);
          return [];
        }),
      ]);

      let totalViews = 0;
      let subscribersGained = 0;
      let subscribersLost = 0;
      for (const row of dailyRows) {
        totalViews += Number(row[1] || 0);
        subscribersGained += Number(row[2] || 0);
        subscribersLost += Number(row[3] || 0);
      }

      let searchViews = 0;
      let adViews = 0;
      for (const row of trafficRows) {
        const source = row[1];
        const views = Number(row[2] || 0);
        if (source === "YT_SEARCH") {
          searchViews += views;
        } else if (source === "ADVERTISING") {
          adViews += views;
        }
      }

      const organicViews = Math.max(0, totalViews - adViews);
      const netSubscribers = subscribersGained - subscribersLost;
      const days = dailyRows.map(row => row[0]).filter(Boolean);
      return {
        organicViews,
        netSubscribers,
        searchViews,
        days
      };
    },
    options
  );
}

app.get("/api/targets", async (req, res, next) => {
  try {
    const today = new Date();
    const year = today.getFullYear();
    const month = today.getMonth(); // 0-11
    const force = req.query.force === "1";
    let currentQuarterKey = "";
    if (month >= 3 && month <= 5) currentQuarterKey = `AMJ_${year}`;
    else if (month >= 6 && month <= 8) currentQuarterKey = `JAS_${year}`;
    else if (month >= 9 && month <= 11) currentQuarterKey = `OND_${year}`;
    else currentQuarterKey = `JFM_${year}`;
    const quarterKey = String(req.query.quarter || currentQuarterKey);
    const data = await readQuarterTargets();
    if (!data[quarterKey]) {
      data[quarterKey] = { ytm: [], seo: [] };
    }

    const parts = quarterKey.split("_");
    const name = parts[0];
    const qYear = Number(parts[1] || year);

    let startMonth, endMonth, endDay;
    if (name === "AMJ") {
      startMonth = 3; endMonth = 5; endDay = 30;
    } else if (name === "JAS") {
      startMonth = 6; endMonth = 8; endDay = 30;
    } else if (name === "OND") {
      startMonth = 9; endMonth = 11; endDay = 31;
    } else {
      startMonth = 0; endMonth = 2; endDay = 31;
    }

    const yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    const qStart = new Date(Date.UTC(qYear, startMonth, 1));
    const qEnd = new Date(Date.UTC(qYear, endMonth, endDay));

    const startDate = qStart.toISOString().slice(0, 10);
    let endDate = new Date(Math.min(qEnd.getTime(), yesterday.getTime())).toISOString().slice(0, 10);
    if (endDate < startDate) {
      endDate = startDate;
    }

    const hasStarted = yesterday >= qStart;

    const entries = await connectedChannelEntries();
    const channelMap = new Map();
    for (const entry of entries) {
      channelMap.set(entry.channel.id, entry);
    }

    const totalDays = Math.round((qEnd - qStart) / (1000 * 60 * 60 * 24)) + 1;

    const activeTargets = data[quarterKey] || { ytm: [], seo: [] };

    const targetYtmFiltered = activeTargets.ytm;
    const targetSeoFiltered = activeTargets.seo;

    const targetChannelIds = new Set();
    for (const t of targetYtmFiltered) {
      const ids = t.channelIds || (t.channelId ? [t.channelId] : []);
      for (const id of ids) targetChannelIds.add(id);
    }
    for (const t of targetSeoFiltered) {
      const ids = t.channelIds || (t.channelId ? [t.channelId] : []);
      for (const id of ids) targetChannelIds.add(id);
    }

    const statsMap = new Map();
    const allDays = new Set();

    await Promise.all(
      Array.from(targetChannelIds).map(async (channelId) => {
        const entry = channelMap.get(channelId);
        if (!entry || !hasStarted) {
          statsMap.set(channelId, { organicViews: 0, netSubscribers: 0, searchViews: 0, days: [] });
          return;
        }
        try {
          const stats = await channelTargetAnalytics(entry.auth, channelId, startDate, endDate, { force });
          statsMap.set(channelId, stats);
          if (stats.days) {
            for (const d of stats.days) allDays.add(d);
          }
        } catch (err) {
          console.error(`Error fetching target stats for channel ${channelId}:`, err);
          statsMap.set(channelId, { organicViews: 0, netSubscribers: 0, searchViews: 0, days: [] });
        }
      })
    );

    let elapsedDays = 0;
    if (hasStarted) {
      elapsedDays = allDays.size;
    }

    const ytmResults = targetYtmFiltered.map(t => {
      const ids = t.channelIds || (t.channelId ? [t.channelId] : []);
      let actualViews = 0;
      let actualSubs = 0;
      for (const id of ids) {
        const stats = statsMap.get(id) || { organicViews: 0, netSubscribers: 0, searchViews: 0 };
        actualViews += stats.organicViews;
        actualSubs += stats.netSubscribers;
      }

      const viewsPercent = t.viewsTarget ? (actualViews / t.viewsTarget) * 100 : 0;
      const subsPercent = t.subsTarget ? (actualSubs / t.subsTarget) * 100 : 0;

      let viewsProRataPercent = 0;
      let subsProRataPercent = 0;
      if (elapsedDays > 0 && totalDays > 0) {
        const expectedViews = (t.viewsTarget * elapsedDays) / totalDays;
        const expectedSubs = (t.subsTarget * elapsedDays) / totalDays;
        viewsProRataPercent = expectedViews ? (actualViews / expectedViews) * 100 : 0;
        subsProRataPercent = expectedSubs ? (actualSubs / expectedSubs) * 100 : 0;
      }

      return {
        ...t,
        actualViews,
        actualSubs,
        viewsPercent,
        subsPercent,
        viewsProRataPercent,
        subsProRataPercent
      };
    });

    const seoResults = targetSeoFiltered.map(t => {
      const ids = t.channelIds || (t.channelId ? [t.channelId] : []);
      let actualSearchViews = 0;
      for (const id of ids) {
        const stats = statsMap.get(id) || { organicViews: 0, netSubscribers: 0, searchViews: 0 };
        actualSearchViews += stats.searchViews;
      }

      const searchPercent = t.searchViewsTarget ? (actualSearchViews / t.searchViewsTarget) * 100 : 0;

      let searchProRataPercent = 0;
      if (elapsedDays > 0 && totalDays > 0) {
        const expectedSearch = (t.searchViewsTarget * elapsedDays) / totalDays;
        searchProRataPercent = expectedSearch ? (actualSearchViews / expectedSearch) * 100 : 0;
      }

      return {
        ...t,
        actualSearchViews,
        searchPercent,
        searchProRataPercent
      };
    });

    res.json({
      quarter: quarterKey,
      startDate,
      endDate,
      hasStarted,
      elapsedDays,
      totalDays,
      ytm: ytmResults,
      seo: seoResults
    });
  } catch (error) {
    next(error);
  }
});

app.post("/api/targets/save", async (req, res, next) => {
  try {
    const { quarter, ytm, seo } = req.body;
    if (!quarter || !ytm || !seo) {
      res.status(400).json({ error: "Missing quarter, ytm, or seo configurations." });
      return;
    }
    const data = await readQuarterTargets();
    data[quarter] = { ytm, seo };
    await saveQuarterTargets(data);
    res.json({ success: true });
  } catch (error) {
    next(error);
  }
});

app.get("/api/keywords/rankings", async (req, res, next) => {
  try {
    const channelId = String(req.query.channelId || "");
    if (!channelId) {
      res.status(400).json({ error: "Missing channelId" });
      return;
    }
    const viewer = readViewerSession(req);
    const entries = await connectedChannelEntries(viewer);
    const entry = entries.find(e => e.channel.id === channelId);
    if (!entry) {
      res.status(403).json({ error: "Access denied or channel not connected." });
      return;
    }
    const data = await readKeywordRankings();
    const rankings = data[channelId] || {
      channelId,
      lastUpdated: null,
      manualKeywords: [],
      rankings: { automated: [], manual: [] }
    };
    res.json(rankings);
  } catch (error) {
    next(error);
  }
});

app.post("/api/keywords/manual", async (req, res, next) => {
  try {
    const channelId = String(req.body.channelId || "");
    const keyword = String(req.body.keyword || "").trim();
    if (!channelId || !keyword) {
      res.status(400).json({ error: "Missing channelId or keyword" });
      return;
    }
    const viewer = readViewerSession(req);
    const entries = await connectedChannelEntries(viewer);
    const entry = entries.find(e => e.channel.id === channelId);
    if (!entry) {
      res.status(403).json({ error: "Access denied or channel not connected." });
      return;
    }
    const data = await readKeywordRankings();
    if (!data[channelId]) {
      data[channelId] = {
        channelId,
        lastUpdated: null,
        manualKeywords: [],
        rankings: { automated: [], manual: [] }
      };
    }
    if ((data[channelId].manualKeywords || []).length >= 50) {
      res.status(400).json({ error: "Maximum limit of 50 manual keywords reached." });
      return;
    }
    if (!data[channelId].manualKeywords.includes(keyword)) {
      data[channelId].manualKeywords.push(keyword);
      await saveKeywordRankings(data);
    }
    const updated = await refreshKeywordRankingsInternal(channelId);
    res.json(updated);
  } catch (error) {
    next(error);
  }
});

app.delete("/api/keywords/manual", async (req, res, next) => {
  try {
    const channelId = String(req.body.channelId || "");
    const keyword = String(req.body.keyword || "").trim();
    if (!channelId || !keyword) {
      res.status(400).json({ error: "Missing channelId or keyword" });
      return;
    }
    const viewer = readViewerSession(req);
    const entries = await connectedChannelEntries(viewer);
    const entry = entries.find(e => e.channel.id === channelId);
    if (!entry) {
      res.status(403).json({ error: "Access denied or channel not connected." });
      return;
    }
    const data = await readKeywordRankings();
    if (data[channelId]) {
      data[channelId].manualKeywords = (data[channelId].manualKeywords || []).filter(k => k !== keyword);
      if (data[channelId].rankings && data[channelId].rankings.manual) {
        data[channelId].rankings.manual = data[channelId].rankings.manual.filter(r => r.keyword !== keyword);
      }
      await saveKeywordRankings(data);
    }
    const updated = data[channelId] || {
      channelId,
      lastUpdated: null,
      manualKeywords: [],
      rankings: { automated: [], manual: [] }
    };
    res.json(updated);
  } catch (error) {
    next(error);
  }
});

app.post("/api/keywords/refresh", async (req, res, next) => {
  try {
    const channelId = String(req.body.channelId || "");
    if (!channelId) {
      res.status(400).json({ error: "Missing channelId" });
      return;
    }
    const viewer = readViewerSession(req);
    const entries = await connectedChannelEntries(viewer);
    const entry = entries.find(e => e.channel.id === channelId);
    if (!entry) {
      res.status(403).json({ error: "Access denied or channel not connected." });
      return;
    }
    const updated = await refreshKeywordRankingsInternal(channelId);
    res.json(updated);
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
    const force = req.query.force === "1";
    const data = await cached(
      makeCacheKey("category-competitors", category, range, month, dates.startDate, dates.endDate),
      ttl.competitors,
      () => categoryCompetitorReport(category, mappings, dates, { force }),
      { force }
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

const candidateMappings = {
  "Vinayak": [
    "SuperCoaching MPSC by Testbook",
    "Banking Testbook",
    "Railway Testbook"
  ],
  "Mohit": [
    "Bihar Testbook",
    "Testbook",
    "Punjab Testbook"
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
  "Govardhan": [
    "Testbook Tamil",
    "Testbook Telugu"
  ],
  "Vivek": [
    "AE JE Testbook",
    "SSC Testbook",
    "Testbook - JAIIB CAIIB"
  ],
  "Aditya": [
    "Testbook Bengali",
    "WBPSC Testbook",
    "SuperCoaching Marathi by Testbook",
    "TET Factory by Testbook",
    "Odisha Testbook",
    "Odisha Teaching by Testbook"
  ]
};

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
    "UPSC PrepLab"
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
    "Testbook - JAIIB CAIIB"
  ],
  "Govardhan": [
    "Testbook Tamil",
    "Testbook Telugu"
  ]
};

app.get("/api/seo/audit", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);
    if (!isAuditAdmin(viewer)) {
      return res.status(403).json({ error: "Access denied. Only authorized admins can run SEO audits." });
    }

    const entries = await connectedChannelEntries();
    if (!entries.length) {
      return res.json({ videos: [] });
    }
    
    const candidate = req.query.candidate;
    let selectedEntries = entries;
    if (candidate && candidate !== "All") {
      const allowedTitles = candidateMappings[candidate] || [];
      const allowedTitlesLower = new Set(allowedTitles.map(t => t.toLowerCase().trim()));
      selectedEntries = entries.filter(entry => {
        const title = (entry.channel.name || "").toLowerCase().trim();
        return allowedTitlesLower.has(title);
      });
    }
    
    if (!selectedEntries.length) {
      return res.json({ videos: [] });
    }
    
    const channelIds = selectedEntries.map(e => e.channel.id).sort();
    const cacheKey = makeCacheKey("seo-audit", channelIds);
    
    const payload = await cached(
      cacheKey,
      30 * 60 * 1000, // 30 minutes
      async () => {
        const allVideos = [];
        let claudeDisabled = !process.env.ANTHROPIC_API_KEY;
        
        for (const entry of selectedEntries) {
          const auth = entry.auth;
          const channel = entry.channel;
          const channelId = channel.id;
          const channelTitle = channel.name;
          
          const playlistId = channel.uploadsPlaylistId || (channelId.startsWith("UC") ? "UU" + channelId.slice(2) : channelId);
          const youtube = google.youtube({ version: "v3", auth });
          
          let playlistItems = [];
          let pageToken = undefined;
          try {
            const res1 = await youtube.playlistItems.list({
              part: ["snippet", "contentDetails"],
              playlistId: playlistId,
              maxResults: 50,
              pageToken,
            });
            playlistItems.push(...(res1.data.items || []));
            pageToken = res1.data.nextPageToken;
            
            if (pageToken && playlistItems.length < 90) {
              const res2 = await youtube.playlistItems.list({
                part: ["snippet", "contentDetails"],
                playlistId: playlistId,
                maxResults: 40,
                pageToken,
              });
              playlistItems.push(...(res2.data.items || []));
            }
          } catch (err) {
            console.error(`Failed to fetch uploads playlist for channel ${channelId}:`, err);
            throw err;
          }
          
          const videoIds = playlistItems.map(item => item.contentDetails?.videoId).filter(Boolean);
          if (!videoIds.length) continue;
          
          const liveVideoIds = new Set();
          const livePlaylistId = channelId.startsWith("UC") ? "UULV" + channelId.slice(2) : channelId;
          try {
            const liveRes = await youtube.playlistItems.list({
              part: ["snippet", "contentDetails"],
              playlistId: livePlaylistId,
              maxResults: 50,
            });
            for (const item of liveRes.data.items || []) {
              const vId = item.contentDetails?.videoId || item.snippet?.resourceId?.videoId;
              if (vId) liveVideoIds.add(vId);
            }
          } catch (err) {
            // Ignore error if live stream playlist is empty/missing
          }
          
          const videoDetailsMap = {};
          for (let index = 0; index < videoIds.length; index += 50) {
            const chunk = videoIds.slice(index, index + 50);
            const response = await youtube.videos.list({
              part: ["snippet", "contentDetails", "liveStreamingDetails", "statistics", "status"],
              id: chunk,
              maxResults: 50,
            });
            for (const item of response.data.items || []) {
              videoDetailsMap[item.id] = item;
            }
          }
          
          const validVideos = [];
          for (const item of playlistItems) {
            const vId = item.contentDetails?.videoId;
            if (!vId) continue;
            const videoDetails = videoDetailsMap[vId];
            if (!videoDetails) continue;
            
            const privacy = videoDetails.status?.privacyStatus;
            if (privacy === "unlisted" || privacy === "private") continue;
            
            const format = classifySeoVideo(videoDetails, liveVideoIds);
            if (format === "Shorts") continue;
            
            validVideos.push({
              id: vId,
              title: videoDetails.snippet?.title || "Untitled",
              description: videoDetails.snippet?.description || "",
              tags: videoDetails.snippet?.tags || [],
              publishedAt: videoDetails.snippet?.publishedAt || "",
              views: Number(videoDetails.statistics?.viewCount || 0),
              format,
              channelId,
              channelTitle,
              privacy,
            });
          }
          
          const truncatedVideos = validVideos.slice(0, 30);
          const channelAvgViews = truncatedVideos.reduce((sum, v) => sum + v.views, 0) / Math.max(1, truncatedVideos.length);
          
          for (const video of truncatedVideos) {
            const localGaps = localSeoMetadataAudit(video);
            let gaps = [...localGaps];
            
            if (!claudeDisabled) {
              try {
                const claudeGaps = await claudeSeoMetadataAudit(video);
                const merged = new Set([...localGaps, ...claudeGaps]);
                gaps = [...merged];
              } catch (claudeErr) {
                console.error("Claude SEO audit failed, disabling Claude for remainder of audit:", claudeErr.message);
                claudeDisabled = true;
              }
            }
            
            const score = Math.max(0, 100 - gaps.length * 10);
            
            allVideos.push({
              id: video.id,
              title: video.title,
              publishedAt: video.publishedAt,
              views: video.views,
              format: video.format,
              channelId: video.channelId,
              channelTitle: video.channelTitle,
              score,
              gaps,
              channelAverageViews: channelAvgViews,
            });
          }
        }
        
        return { videos: allVideos };
      },
      { force: req.query.force === "1" }
    );
    
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.get("/api/ytm/audit", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);

    const entries = await connectedChannelEntries();
    if (!entries.length) {
      return res.json({ videos: [] });
    }
    
    const manager = req.query.manager;
    let selectedEntries = entries;
    if (manager && manager !== "All") {
      const allowedTitles = ytmMappings[manager] || [];
      const allowedTitlesLower = new Set(allowedTitles.map(t => t.toLowerCase().trim()));
      selectedEntries = entries.filter(entry => {
        const title = (entry.channel.name || "").toLowerCase().trim();
        const id = (entry.channel.id || "").toLowerCase().trim();
        return allowedTitlesLower.has(title) || allowedTitlesLower.has(id);
      });
    }
    
    if (!selectedEntries.length) {
      return res.json({ videos: [] });
    }
    
    const channelIds = selectedEntries.map(e => e.channel.id).sort().join(",");
    const cacheKey = makeCacheKey("ytm-audit", channelIds);
    
    const payload = await cached(
      cacheKey,
      30 * 60 * 1000, // 30 minutes
      async () => {
        const allVideos = [];
        
        for (const entry of selectedEntries) {
          const auth = entry.auth;
          const channel = entry.channel;
          const channelId = channel.id;
          const channelTitle = channel.name;
          
          const playlistId = channel.uploadsPlaylistId || (channelId.startsWith("UC") ? "UU" + channelId.slice(2) : channelId);
          const youtube = google.youtube({ version: "v3", auth });
          
          let playlistItems = [];
          let pageToken = undefined;
          try {
            const res1 = await youtube.playlistItems.list({
              part: ["snippet", "contentDetails"],
              playlistId: playlistId,
              maxResults: 50,
              pageToken,
            });
            playlistItems.push(...(res1.data.items || []));
            pageToken = res1.data.nextPageToken;
            
            if (pageToken && playlistItems.length < 90) {
              const res2 = await youtube.playlistItems.list({
                part: ["snippet", "contentDetails"],
                playlistId: playlistId,
                maxResults: 40,
                pageToken,
              });
              playlistItems.push(...(res2.data.items || []));
            }
          } catch (err) {
            console.error(`Failed to fetch uploads playlist for channel ${channelId}:`, err);
            throw err;
          }
          
          const videoIds = playlistItems.map(item => item.contentDetails?.videoId).filter(Boolean);
          if (!videoIds.length) continue;
          
          const liveVideoIds = new Set();
          const livePlaylistId = channelId.startsWith("UC") ? "UULV" + channelId.slice(2) : channelId;
          try {
            const liveRes = await youtube.playlistItems.list({
              part: ["snippet", "contentDetails"],
              playlistId: livePlaylistId,
              maxResults: 50,
            });
            for (const item of liveRes.data.items || []) {
              const vId = item.contentDetails?.videoId || item.snippet?.resourceId?.videoId;
              if (vId) liveVideoIds.add(vId);
            }
          } catch (err) {
            // Ignore error if live stream playlist is empty/missing
          }
          
          const videoDetailsMap = {};
          for (let index = 0; index < videoIds.length; index += 50) {
            const chunk = videoIds.slice(index, index + 50);
            const response = await youtube.videos.list({
              part: ["snippet", "contentDetails", "liveStreamingDetails", "statistics", "status"],
              id: chunk,
              maxResults: 50,
            });
            for (const item of response.data.items || []) {
              videoDetailsMap[item.id] = item;
            }
          }
          
          const validVideos = [];
          for (const item of playlistItems) {
            const vId = item.contentDetails?.videoId;
            if (!vId) continue;
            const videoDetails = videoDetailsMap[vId];
            if (!videoDetails) continue;
            
            const privacy = videoDetails.status?.privacyStatus;
            if (privacy === "unlisted" || privacy === "private") continue;
            
            const format = classifySeoVideo(videoDetails, liveVideoIds);
            if (format === "Shorts") continue;
            
            validVideos.push({
              id: vId,
              title: videoDetails.snippet?.title || "Untitled",
              description: videoDetails.snippet?.description || "",
              thumbnails: videoDetails.snippet?.thumbnails || {},
              publishedAt: videoDetails.snippet?.publishedAt || "",
              views: Number(videoDetails.statistics?.viewCount || 0),
              format,
              channelId,
              channelTitle,
              privacy,
            });
          }
          
          const truncatedVideos = validVideos.slice(0, 30);
          
          // Parallel fetch of comments for all truncated videos
          const commentThreadsPromises = truncatedVideos.map(async (video) => {
            let commentThreads = [];
            let commentsDisabled = false;
            try {
              const commentsRes = await youtube.commentThreads.list({
                part: ["snippet", "replies"],
                videoId: video.id,
                maxResults: 5,
                order: "relevance",
                auth: process.env.YOUTUBE_API_KEY
              });
              commentThreads = commentsRes.data.items || [];
            } catch (err) {
              if (err.errors && err.errors.some(e => e.reason === "commentsDisabled")) {
                commentsDisabled = true;
              } else {
                console.error(`Error fetching comments for video ${video.id}:`, err.message);
              }
            }
            return { video, commentThreads, commentsDisabled };
          });
          
          const videosWithComments = await Promise.all(commentThreadsPromises);
          
          for (const { video, commentThreads, commentsDisabled } of videosWithComments) {
            const gaps = [];
            
            // 1. Pinned comment checks
            if (commentsDisabled) {
              gaps.push("Link Missing in Pinned Comment");
            } else if (commentThreads.length === 0) {
              gaps.push("Link Missing in Pinned Comment");
            } else {
              const topThread = commentThreads[0];
              const topComment = topThread.snippet?.topLevelComment;
              const authorChannelId = topComment?.snippet?.authorChannelId?.value;
              const isOwner = authorChannelId === channelId;
              
              if (!isOwner) {
                gaps.push("Link Missing in Pinned Comment");
              } else {
                const textOriginal = topComment.snippet?.textOriginal || "";
                const textDisplay = topComment.snippet?.textDisplay || "";
                const hasTargetLink = /https?:\/\//i.test(textOriginal) || /https?:\/\//i.test(textDisplay);
                if (!hasTargetLink) {
                  gaps.push("Link Missing in Pinned Comment");
                }
              }
            }
            
            // 2. Link in Description Check
            const descText = video.description || "";
            if (!descText.includes("link.testbook.com")) {
              gaps.push("Link Missing in Description");
            }
            
            // 3. Playlist Link Check
            if (!hasPlaylistLink(descText)) {
              gaps.push("Playlist Link Missing");
            }
            
            // 4. Comment Engagement Check (Not Replied/Not Hearted)
            let hasEngagement = false;
            let hasEligibleComments = false;
            const unansweredComments = [];
            
            if (!commentsDisabled && commentThreads.length > 0) {
              for (const thread of commentThreads) {
                const topComment = thread.snippet?.topLevelComment;
                const authorChannelId = topComment?.snippet?.authorChannelId?.value;
                if (authorChannelId === channelId) continue; // Skip comments posted by the owner
                
                hasEligibleComments = true;
                const totalReplyCount = thread.snippet?.totalReplyCount || 0;
                let hasOwnerReply = false;
                if (totalReplyCount > 0) {
                  const replies = thread.replies?.comments || [];
                  hasOwnerReply = replies.some(reply => reply.snippet?.authorChannelId?.value === channelId);
                  if (hasOwnerReply) {
                    hasEngagement = true;
                  }
                }
                if (!hasOwnerReply) {
                  unansweredComments.push({
                    id: topComment.id,
                    authorName: topComment.snippet?.authorDisplayName || "User",
                    authorProfileImage: topComment.snippet?.authorProfileImageUrl || "",
                    text: topComment.snippet?.textOriginal || topComment.snippet?.textDisplay || "",
                    publishedAt: topComment.snippet?.publishedAt || "",
                  });
                }
              }
            }
            
            if (hasEligibleComments && !hasEngagement) {
              gaps.push("Not Replied/Not Hearted");
            }
            
            const score = Math.max(0, 100 - gaps.length * 25);
            
            allVideos.push({
              id: video.id,
              title: video.title,
              publishedAt: video.publishedAt,
              views: video.views,
              format: video.format,
              channelId: video.channelId,
              channelTitle: video.channelTitle,
              score,
              gaps,
              unansweredComments,
            });
          }
        }
        
        return { videos: allVideos };
      },
      { force: req.query.force === "1" }
    );
    
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

app.post("/api/seo/suggest", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);
    if (!isAuditAdmin(viewer)) {
      return res.status(403).json({ error: "Access denied. Only authorized admins can run SEO audits." });
    }

    const videoId = String(req.body.videoId || "");
    const channelId = String(req.body.channelId || "");
    if (!videoId) {
      return res.status(400).json({ error: "Missing videoId" });
    }
    if (!process.env.ANTHROPIC_API_KEY) {
      return res.status(400).json({ error: "Add an Anthropic API key before requesting suggestions." });
    }
    
    const entries = await connectedChannelEntries();
    const entry = entries.find(e => e.channel.id === channelId);
    if (!entry) {
      return res.status(404).json({ error: "Channel not found or unauthorized" });
    }
    
    const cacheKey = makeCacheKey("seo-suggest", videoId);
    const payload = await cached(
      cacheKey,
      6 * 60 * 60 * 1000, // 6 hours
      async () => {
        const youtube = google.youtube({ version: "v3", auth: entry.auth });
        const response = await youtube.videos.list({
          part: ["snippet", "contentDetails", "statistics"],
          id: [videoId],
        });
        const video = response.data.items?.[0];
        if (!video) throw new Error("Video not found on YouTube");
        
        const title = video.snippet?.title || "";
        const description = video.snippet?.description || "";
        const tags = (video.snippet?.tags || []).join(", ");
        
        const prompt = [
          `Generate optimized SEO metadata recommendations for the following YouTube video:`,
          `Title: ${title}`,
          `Current Description: ${description}`,
          `Current Tags: ${tags}`,
          ``,
          `Follow these instructions strictly:`,
          `1. Do NOT suggest any changes to the video title. Do not output title ideas.`,
          `2. Generate the optimized opening lines for the description. This description opening MUST contain relevant target keywords from the title in its first 2-3 lines (approximately 150-200 characters) to hook the viewer and help with search indexation.`,
          `3. Generate a list of recommended tags that are highly relevant, specific, and sum up to at least 450 characters (joined by commas) to maximize the tag metadata space.`,
          `4. Generate at least 5 highly relevant hashtags.`,
          `5. Provide a short reasoning explaining your optimization choices.`,
          ``,
          `Return your response as a strict JSON object with the following fields:`,
          `{`,
          `  "description": "the optimized opening lines of the description (first 2-3 lines)",`,
          `  "tags": "a comma-separated list of recommended tags that sums to at least 450 characters",`,
          `  "hashtags": "#tag1 #tag2 #tag3 #tag4 #tag5",`,
          `  "reasoning": "brief explanation of optimization choices"`,
          `}`,
          `Do not include any text other than the JSON object.`
        ].join("\n");
        
        const data = await fetchAnthropicJson({
          model: "claude-sonnet-4-20250514",
          max_tokens: 1200,
          messages: [{ role: "user", content: prompt }],
        });
        
        const text = (data.content || []).map((item) => item.text || "").join("\n").trim();
        const match = text.match(/\{[\s\S]*\}/);
        if (!match) throw new Error("Claude did not return valid JSON for suggestions.");
        const parsed = JSON.parse(match[0]);
        return {
          description: String(parsed.description || ""),
          tags: String(parsed.tags || ""),
          hashtags: String(parsed.hashtags || ""),
          reasoning: String(parsed.reasoning || ""),
        };
      }
    );
    
    res.json(payload);
  } catch (error) {
    next(error);
  }
});

// YTM Comment Reply Assistant & Competitor Outlier Alerts

const competitorOutliersPath = path.join(dataDir, "competitor_outliers.json");

async function readCompetitorOutliers() {
  if (sql) {
    await ensureStorage();
    const rows = await sql`select payload from app_state where key = 'competitor_outliers' limit 1`;
    return rows[0]?.payload || [];
  }
  try {
    return JSON.parse(await readFile(competitorOutliersPath, "utf8"));
  } catch {
    return [];
  }
}

async function saveCompetitorOutliers(outliers) {
  if (sql) {
    await ensureStorage();
    await sql`
      insert into app_state (key, payload)
      values ('competitor_outliers', ${sql.json(outliers)})
      on conflict (key) do update set payload = excluded.payload, updated_at = now()
    `;
    return;
  }
  await mkdir(dataDir, { recursive: true });
  await writeFile(competitorOutliersPath, JSON.stringify(outliers, null, 2), "utf8");
}

const keywordRankingsPath = path.join(dataDir, "keyword_rankings.json");

async function readKeywordRankings() {
  if (sql) {
    await ensureStorage();
    const rows = await sql`select payload from app_state where key = 'keyword_rankings' limit 1`;
    return rows[0]?.payload || {};
  }
  try {
    return JSON.parse(await readFile(keywordRankingsPath, "utf8"));
  } catch {
    return {};
  }
}

async function saveKeywordRankings(rankings) {
  if (sql) {
    await ensureStorage();
    await sql`
      insert into app_state (key, payload)
      values ('keyword_rankings', ${sql.json(rankings)})
      on conflict (key) do update set payload = excluded.payload, updated_at = now()
    `;
    return;
  }
  await mkdir(dataDir, { recursive: true });
  await writeFile(keywordRankingsPath, JSON.stringify(rankings, null, 2), "utf8");
}

function uniqueCompetitorsList() {
  const channelMap = new Map();
  for (const [category, groups] of Object.entries(competitorCategoryMap)) {
    for (const group of groups) {
      if (group.group.toLowerCase().includes("testbook")) continue;
      for (const id of group.ids) {
        if (!channelMap.has(id)) {
          channelMap.set(id, {
            id,
            name: group.group,
            group: group.group,
            category
          });
        }
      }
    }
  }
  return Array.from(channelMap.values());
}

async function runOutliersScanInternal() {
  if (!process.env.YOUTUBE_API_KEY) {
    console.log("Skipping outliers scan: YOUTUBE_API_KEY not configured.");
    return [];
  }
  
  console.log("Starting competitor outliers scan...");
  const youtube = google.youtube({ version: "v3", auth: process.env.YOUTUBE_API_KEY });
  const competitors = uniqueCompetitorsList();
  
  const activeOutliersMap = new Map();
  const existing = await readCompetitorOutliers();
  const now = Date.now();
  const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
  
  for (const item of existing) {
    const age = now - new Date(item.publishedAt).getTime();
    if (age <= sevenDaysMs) {
      activeOutliersMap.set(item.id, item);
    }
  }

  const batchSize = 10;
  for (let i = 0; i < competitors.length; i += batchSize) {
    const chunk = competitors.slice(i, i + batchSize);
    await Promise.all(chunk.map(async (comp) => {
      try {
        const channelId = comp.id;
        const uploadsPlaylistId = "UU" + channelId.slice(2);
        const livePlaylistId = "UULV" + channelId.slice(2);
        
        const cacheKey = makeCacheKey("competitor-baseline", channelId);
        const baseline = await cached(cacheKey, 24 * 60 * 60 * 1000, async () => {
          const res = await youtube.playlistItems.list({
            part: ["snippet", "contentDetails"],
            playlistId: uploadsPlaylistId,
            maxResults: 30,
          }).catch(() => ({ data: { items: [] } }));
          
          const items = res.data.items || [];
          const videoIds = items.map(item => item.contentDetails?.videoId).filter(Boolean);
          if (!videoIds.length) {
            return { averages: { Video: 0, Shorts: 0, Live: 0 }, overall: 0 };
          }
          
          const liveRes = await youtube.playlistItems.list({
            part: ["contentDetails"],
            playlistId: livePlaylistId,
            maxResults: 30,
          }).catch(() => ({ data: { items: [] } }));
          const liveVideoIds = new Set((liveRes.data.items || []).map(item => item.contentDetails?.videoId).filter(Boolean));
          
          const details = [];
          for (let idx = 0; idx < videoIds.length; idx += 50) {
            const videoChunk = videoIds.slice(idx, idx + 50);
            const chunkRes = await youtube.videos.list({
              part: ["snippet", "contentDetails", "statistics", "liveStreamingDetails"],
              id: videoChunk,
            });
            details.push(...(chunkRes.data.items || []));
          }
          
          const videos = details.map(item => {
            const format = classifyPublicVideo(item, liveVideoIds);
            return {
              views: Number(item.statistics?.viewCount || 0),
              format,
            };
          });
          
          const formatSum = { Video: 0, Shorts: 0, Live: 0 };
          const formatCount = { Video: 0, Shorts: 0, Live: 0 };
          let totalViews = 0;
          
          for (const v of videos) {
            formatSum[v.format] += v.views;
            formatCount[v.format]++;
            totalViews += v.views;
          }
          
          const overall = Math.round(totalViews / Math.max(1, videos.length));
          const averages = {
            Video: formatCount.Video > 0 ? Math.round(formatSum.Video / formatCount.Video) : overall,
            Shorts: formatCount.Shorts > 0 ? Math.round(formatSum.Shorts / formatCount.Shorts) : overall,
            Live: formatCount.Live > 0 ? Math.round(formatSum.Live / formatCount.Live) : overall,
          };
          
          return { averages, overall };
        });
        
        const latestRes = await youtube.playlistItems.list({
          part: ["snippet", "contentDetails"],
          playlistId: uploadsPlaylistId,
          maxResults: 5,
        }).catch(() => ({ data: { items: [] } }));
        
        const latestItems = latestRes.data.items || [];
        const latestVideoIds = latestItems.map(item => item.contentDetails?.videoId).filter(Boolean);
        if (!latestVideoIds.length) return;
        
        const liveResLatest = await youtube.playlistItems.list({
          part: ["contentDetails"],
          playlistId: livePlaylistId,
          maxResults: 10,
        }).catch(() => ({ data: { items: [] } }));
        const liveVideoIdsLatest = new Set((liveResLatest.data.items || []).map(item => item.contentDetails?.videoId).filter(Boolean));
        
        const latestDetailsRes = await youtube.videos.list({
          part: ["snippet", "contentDetails", "statistics", "liveStreamingDetails"],
          id: latestVideoIds,
        });
        
        const latestDetails = latestDetailsRes.data.items || [];
        
        for (const item of latestDetails) {
          const publishedAt = item.snippet?.publishedAt;
          const pubTime = new Date(publishedAt).getTime();
          if (now - pubTime > sevenDaysMs) continue;
          
          const format = classifyPublicVideo(item, liveVideoIdsLatest);
          const views = Number(item.statistics?.viewCount || 0);
          const baselineAvg = baseline.averages[format] || baseline.overall || 1;
          
          if (views > baselineAvg && views > 5000) {
            const outlierScore = Number((views / baselineAvg).toFixed(2));
            activeOutliersMap.set(item.id, {
              id: item.id,
              title: item.snippet?.title || "Untitled",
              channelId,
              channelTitle: item.snippet?.channelTitle || comp.name,
              category: comp.category,
              group: comp.group,
              views,
              baselineAverage: baselineAvg,
              outlierScore,
              publishedAt,
              format,
              url: `https://www.youtube.com/watch?v=${item.id}`,
              scannedAt: new Date().toISOString(),
            });
          }
        }
      } catch (err) {
        console.error(`Failed scanning competitor ${comp.name} (${comp.id}):`, err.message);
      }
    }));
  }
  
  const sortedOutliers = Array.from(activeOutliersMap.values())
    .sort((a, b) => b.outlierScore - a.outlierScore);
    
  await saveCompetitorOutliers(sortedOutliers);
  console.log(`Scan complete. Found ${sortedOutliers.length} active outliers.`);
  return sortedOutliers;
}

let isScanningOutliers = false;

function startOutliersScheduler() {
  setInterval(async () => {
    try {
      const options = { timeZone: 'Asia/Kolkata', hour: 'numeric', hour12: false };
      const formatter = new Intl.DateTimeFormat('en-US', options);
      const hourInIST = parseInt(formatter.format(new Date()), 10);
      
      if (hourInIST >= 11 && hourInIST <= 21) {
        if (isScanningOutliers) {
          console.log("Background outlier scan already in progress. Skipping.");
          return;
        }
        isScanningOutliers = true;
        await runOutliersScanInternal();
        isScanningOutliers = false;
      }
    } catch (err) {
      isScanningOutliers = false;
      console.error("Error in background outlier scan scheduler:", err);
    }
  }, 60 * 60 * 1000); // every hour
}

// Endpoints for Comment replies and Outlier alerts

app.post("/api/ytm/comment/draft", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);

    const { commentText, videoTitle, authorName } = req.body;
    if (!commentText) {
      return res.status(400).json({ error: "Missing commentText" });
    }
    if (!process.env.ANTHROPIC_API_KEY) {
      return res.status(400).json({ error: "Add an Anthropic API key before drafting replies." });
    }

    const prompt = `You are a professional YouTube community manager.
Draft a friendly, helpful, and concise response to the following user comment on our video "${videoTitle || "our video"}".

Comment from "${authorName || "User"}": "${commentText}"

Requirements:
1. Be extremely concise (1-2 sentences max).
2. Match the language and script of the comment. If the comment is in Hindi (written in Devanagari script like "बहुत बढ़िया"), reply in Hindi using Devanagari script. If the comment is in Hinglish (Hindi written in Latin/English alphabet like "bahut badhiya video"), reply in Hinglish. If the comment is in English, reply in English.
3. Be friendly, helpful, and professional.
4. Do not include any placeholder text (like "[Channel Name]") - sign off as the team or do not sign off at all if unnecessary.
5. Return only the reply text itself. No introductory or concluding remarks, no quotes.`;

    const data = await fetchAnthropicJson({
      model: "claude-sonnet-4-20250514",
      max_tokens: 300,
      messages: [{ role: "user", content: prompt }],
    });

    let draft = (data.content || []).map((item) => item.text || "").join("\n").trim();
    if (draft.startsWith('"') && draft.endsWith('"')) {
      draft = draft.slice(1, -1);
    }
    res.json({ draft });
  } catch (error) {
    next(error);
  }
});

app.post("/api/ytm/comment/reply", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);

    const { channelId, parentId, replyText } = req.body;
    if (!channelId || !parentId || !replyText) {
      return res.status(400).json({ error: "Missing channelId, parentId, or replyText" });
    }

    const entries = await connectedChannelEntries();
    const entry = entries.find(e => e.channel.id === channelId);
    if (!entry) {
      return res.status(404).json({ error: "Connected channel not found or unauthorized" });
    }

    const auth = entry.auth;
    const youtube = google.youtube({ version: "v3", auth });

    const response = await youtube.comments.insert({
      part: ["snippet"],
      requestBody: {
        snippet: {
          parentId: parentId,
          textOriginal: replyText,
        }
      }
    });

    res.json({ success: true, comment: response.data });
  } catch (error) {
    next(error);
  }
});

app.get("/api/competitors/outliers", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);
    if (!isAuditAdmin(viewer)) {
      return res.status(403).json({ error: "Access denied. Only authorized admins can view trending alerts." });
    }
    const outliers = await readCompetitorOutliers();
    const filtered = outliers.filter(item => Number(item.views || 0) > 5000);
    res.json({ outliers: filtered });
  } catch (error) {
    next(error);
  }
});

app.post("/api/competitors/outliers/scan", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);
    if (!isAuditAdmin(viewer)) {
      return res.status(403).json({ error: "Access denied. Only authorized admins can trigger a trending scan." });
    }
    const results = await runOutliersScanInternal();
    res.json({ success: true, count: results.length, outliers: results });
  } catch (error) {
    next(error);
  }
});

app.post("/api/competitors/outliers/suggest", async (req, res, next) => {
  try {
    const viewer = readViewerSession(req);
    if (!isAuditAdmin(viewer)) {
      return res.status(403).json({ error: "Access denied. Only authorized admins can request trending suggestions." });
    }
    const { title, format, category, group, views, outlierScore } = req.body;
    if (!title) {
      return res.status(400).json({ error: "Missing title" });
    }
    if (!process.env.ANTHROPIC_API_KEY) {
      return res.status(400).json({ error: "Add an Anthropic API key before requesting suggestions." });
    }

    const prompt = `Outlier video topic details:
Title: "${title}"
Format: ${format || "Video"}
Category: ${category || "General"}
Competitor Group: ${group || "Competitor"}
Current Views: ${views || 0}
Outlier Score: ${outlierScore || 1.0}x views compared to channel average.

Based on this highly successful outlier topic, generate 5 optimized title ideas and formats that our own channel can use to capitalize on this interest.

Return a strict JSON array only.
Each item must be: {"title":"...","format":"Shorts|Video|Live","strategy":"one concise sentence explaining hook/strategy"}

Provide highly engaging, clickable (but not clickbait) titles tailored to educational content/exams.`;

    const data = await fetchAnthropicJson({
      model: "claude-sonnet-4-20250514",
      max_tokens: 900,
      messages: [{ role: "user", content: prompt }],
    });

    const text = (data.content || []).map((item) => item.text || "").join("\n").trim();
    const match = text.match(/\[[\s\S]*\]/);
    if (!match) throw new Error("Claude did not return valid JSON suggestions.");
    const parsed = JSON.parse(match[0]);
    res.json({ suggestions: parsed });
  } catch (error) {
    next(error);
  }
});

function classifySeoVideo(video, liveVideoIds = new Set()) {
  const title = `${video.snippet?.title || ""} ${(video.snippet?.tags || []).join(" ")}`.toLowerCase();
  const seconds = isoDurationSeconds(video.contentDetails?.duration || "PT0S");
  
  if (/#shorts?\b|\bshorts?\b/.test(title)) return "Shorts";
  if (seconds > 0 && seconds <= 180) return "Shorts";
  
  const isCurrentlyLiveOrUpcoming = ["live", "upcoming"].includes(video.snippet?.liveBroadcastContent);
  if (isCurrentlyLiveOrUpcoming) return "Live";
  
  if (liveVideoIds.has(video.id)) {
    return "Live";
  }
  
  return "Video";
}

const stopWords = new Set([
  "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for", "with", "is", "was", "were", "of", "by", "from",
  "this", "that", "these", "those", "it", "its", "they", "them", "their", "our", "your", "my", "me", "us", "he", "she", "him", "her",
  "how", "what", "why", "who", "where", "when", "which", "about", "into", "over", "under", "again", "further", "then", "once",
  "here", "there", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "can", "will", "just", "should", "shouldn't", "don't", "doesn't", "didn't", "has", "have", "had", "does", "do", "did"
]);

function extractSeoKeywords(title) {
  const cleanTitle = (title || "").toLowerCase().replace(/[^a-z0-9\s]/g, " ");
  const words = cleanTitle.split(/\s+/);
  return [...new Set(words.filter(w => w.length >= 3 && !stopWords.has(w)))];
}

function isSeoDescriptionOptimised(description, title, titleKeywords) {
  if (titleKeywords.length === 0) return true;
  
  let cleanDesc = (description || "").trim();
  const cleanTitle = (title || "").trim().toLowerCase();
  
  // If the description starts with the video title, strip it out to evaluate the actual description body
  if (cleanTitle && cleanDesc.toLowerCase().startsWith(cleanTitle)) {
    cleanDesc = cleanDesc.substring(cleanTitle.length).trim();
  }
  
  const lines = cleanDesc.split(/\r?\n/).slice(0, 3).join(" ").toLowerCase();
  return titleKeywords.some(keyword => lines.includes(keyword));
}

function hasPlaylistLink(description) {
  return /youtube\.com\/playlist\?list=|youtube\.com\/watch\?.*list=/.test(description || "");
}

function isDefaultSeoTag(tag) {
  const genericTags = new Set(["video", "youtube", "tutorial", "shorts", "live", "stream", "channel", "vlog", "update", "new"]);
  return genericTags.has(tag.toLowerCase().trim());
}

function isSeoTitleRelated(tag, titleKeywords) {
  const cleanTag = tag.toLowerCase().trim();
  return titleKeywords.some(keyword => cleanTag.includes(keyword) || keyword.includes(cleanTag));
}

function areSeoTagsRelevant(tags, titleKeywords) {
  if (!tags || tags.length === 0) return false;
  if (titleKeywords.length === 0) return true;
  return tags.some(tag => !isDefaultSeoTag(tag) && isSeoTitleRelated(tag, titleKeywords));
}

function areHashtagsRelevant(hashtags, titleKeywords) {
  if (titleKeywords.length === 0) return true;
  if (hashtags.length === 0) return false;
  const genericHashtags = new Set(["shorts", "video", "live", "youtube", "stream", "channel", "vlog", "update", "new"]);
  return hashtags.some(h => {
    const cleanH = h.replace("#", "").toLowerCase().trim();
    if (genericHashtags.has(cleanH)) return false;
    return titleKeywords.some(keyword => cleanH.includes(keyword) || keyword.includes(cleanH));
  });
}

function getYouTubeTagCharacterCount(tags) {
  if (!tags || tags.length === 0) return 0;
  let totalLength = 0;
  for (let i = 0; i < tags.length; i++) {
    const tag = tags[i];
    let tagLength = tag.length;
    if (tag.includes(' ')) {
      tagLength += 2; // YouTube wraps tags containing spaces in double quotes
    }
    totalLength += tagLength;
  }
  // Add separating commas (number of tags - 1)
  totalLength += (tags.length - 1);
  return totalLength;
}

function localSeoMetadataAudit(video) {
  const gaps = [];
  const titleKeywords = extractSeoKeywords(video.title);
  
  if (!isSeoDescriptionOptimised(video.description, video.title, titleKeywords)) {
    gaps.push("Description First 3 Lines Optimisation Missing");
  }
  
  const tagCount = getYouTubeTagCharacterCount(video.tags);
  if (tagCount < 450) {
    if (!video.tags || video.tags.length === 0) {
      gaps.push("No Tags");
    } else {
      gaps.push(`Tags Less than 450 (${tagCount} characters)`);
    }
  }
  
  if (video.tags && video.tags.length > 0 && !areSeoTagsRelevant(video.tags, titleKeywords)) {
    gaps.push("Tags not Relevant");
  }
  
  const hashtags = (video.description || "").match(/#\w+/g) || [];
  if (hashtags.length < 5) {
    gaps.push(`Less than 5 Hashtags (${hashtags.length} Hashtags)`);
  } else if (!areHashtagsRelevant(hashtags, titleKeywords)) {
    gaps.push("Hashtags not Relevant");
  }
  
  return gaps;
}

async function claudeSeoMetadataAudit(video) {
  const prompt = [
    `Analyze the SEO metadata for this YouTube video:`,
    `Title: ${video.title}`,
    `Description: ${video.description}`,
    `Tags: ${(video.tags || []).join(", ")}`,
    ``,
    `Evaluate it against these specific 4 YouTube SEO rules:`,
    `1. Keyword inclusion: Are relevant keywords from the title present in the first 2-3 lines of the description?`,
    `2. Tag character utilization: Do the tags total at least 450 characters in length?`,
    `3. Tag relevance: Are the tags highly specific and related to the video title (not just generic default tags)?`,
    `4. Hashtags: Are there at least 5 hashtags in the description?`,
    ``,
    `Identify which rules are violated. Return a JSON object with a single field "gaps" containing an array of string descriptions of each gap found.`,
    `Example output format:`,
    `{ "gaps": ["Description First 3 Lines Optimisation Missing"] }`,
    `If all rules are met, return:`,
    `{ "gaps": [] }`,
    `Do not include any text other than the JSON object.`
  ].join("\n");
  
  const data = await fetchAnthropicJson({
    model: "claude-sonnet-4-20250514",
    max_tokens: 400,
    messages: [{ role: "user", content: prompt }],
  });
  
  const text = (data.content || []).map((item) => item.text || "").join("\n").trim();
  const match = text.match(/\{[\s\S]*\}/);
  if (!match) throw new Error("Claude did not return valid JSON for SEO audit.");
  const parsed = JSON.parse(match[0]);
  if (!parsed || !Array.isArray(parsed.gaps)) throw new Error("Claude response invalid gaps format.");
  
  const rawGaps = parsed.gaps.map(String);
  const titleKeywords = extractSeoKeywords(video.title);
  const joinedTags = (video.tags || []).join(", ");
  const hashtags = (video.description || "").match(/#\w+/g) || [];
  
  const cleanedGaps = [];
  for (const gap of rawGaps) {
    const lower = gap.toLowerCase();
    
    // 1. Hashtag count checks - Discard quantitative hashtag gaps from Claude completely
    if (lower.includes("hashtag") && (lower.includes("fewer") || lower.includes("insufficient") || lower.includes("at least 5") || lower.includes("count") || lower.includes("only") || lower.includes("less than 5"))) {
      continue;
    }
    
    // 2. Tag character count checks - Discard quantitative tag length gaps from Claude completely
    if (lower.includes("tag") && (lower.includes("character") || lower.includes("limit") || lower.includes("450") || lower.includes("underutilize") || lower.includes("under 450") || lower.includes("less than 450"))) {
      continue;
    }
    
    // 3. Playlist link checks - Discard playlist gaps from Claude SEO completely (moved to YTM Audit)
    if (lower.includes("playlist")) {
      continue;
    }
    
    // 4. Description keyword checks - Normalize to "Description First 3 Lines Optimisation Missing"
    if (lower.includes("description") && (lower.includes("keyword") || lower.includes("optimisation") || lower.includes("optimization") || lower.includes("missing") || lower.includes("first 3 lines"))) {
      if (!isSeoDescriptionOptimised(video.description, video.title, titleKeywords)) {
        cleanedGaps.push("Description First 3 Lines Optimisation Missing");
      }
      continue;
    }
    
    // 5. Hashtag relevance checks - Normalize to "Hashtags not Relevant"
    if (lower.includes("hashtag") && (lower.includes("relevant") || lower.includes("relevance") || lower.includes("relate"))) {
      if (hashtags.length >= 5 && !areHashtagsRelevant(hashtags, titleKeywords)) {
        cleanedGaps.push("Hashtags not Relevant");
      }
      continue;
    }
    
    // 6. Tag relevance checks - Normalize to "Tags not Relevant"
    if (lower.includes("tag") && (lower.includes("relevant") || lower.includes("relevance") || lower.includes("relate"))) {
      if (video.tags && video.tags.length > 0 && !areSeoTagsRelevant(video.tags, titleKeywords)) {
        cleanedGaps.push("Tags not Relevant");
      }
      continue;
    }
    
    cleanedGaps.push(gap.trim());
  }
  
  return cleanedGaps;
}

app.use((error, _req, res, _next) => {
  console.error(error);
  let message = error?.response?.data?.error?.message || error?.message || "Unknown server error";
  if (message.includes("invalid_grant")) {
    message = "Google connection expired or was revoked. Click Add channel and sign in with Google again to reconnect your YouTube channels.";
  }
  res.status(error?.code || error?.response?.status || 500).json({ error: message });
});

await hydrateEnvFromFile();

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === currentFilePath;
let isRefreshingKeywords = false;
function startKeywordScheduler() {
  setInterval(async () => {
    try {
      const options = { timeZone: 'Asia/Kolkata', hour: 'numeric', hour12: false };
      const formatter = new Intl.DateTimeFormat('en-US', options);
      const hourInIST = parseInt(formatter.format(new Date()), 10);
      
      if (hourInIST === 3) {
        if (isRefreshingKeywords) return;
        isRefreshingKeywords = true;
        console.log("Starting scheduled keyword rankings refresh...");
        const entries = await connectedChannelEntries();
        for (const entry of entries) {
          try {
            console.log(`Refreshing keyword rankings for channel ${entry.channel.name} (${entry.channel.id})...`);
            await refreshKeywordRankingsInternal(entry.channel.id);
          } catch (err) {
            console.error(`Scheduled keyword refresh failed for ${entry.channel.id}:`, err.message);
          }
        }
        isRefreshingKeywords = false;
      }
    } catch (err) {
      isRefreshingKeywords = false;
      console.error("Error in background keyword refresh scheduler:", err);
    }
  }, 60 * 60 * 1000);
}

if (isDirectRun) {
  app.listen(port, () => {
    console.log(`YouTube dashboard running at http://localhost:${port}`);
    startKeywordScheduler();
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

function isAuditAdmin(viewer) {
  if (!teamAuthEnabled()) return true;
  if (!viewer || !viewer.email) return false;
  const adminEmailsStr = process.env.AUDIT_ADMIN_EMAILS || "";
  if (!adminEmailsStr) {
    return viewer.email.toLowerCase().trim() === "ankit.khola@testbook.com";
  }
  const admins = adminEmailsStr.split(",").map(e => e.trim().toLowerCase()).filter(Boolean);
  return admins.includes(viewer.email.toLowerCase().trim()) || viewer.email.toLowerCase().trim() === "ankit.khola@testbook.com";
}

function isAllowedToAddChannel(viewer) {
  if (!teamAuthEnabled()) return true;
  if (!viewer || !viewer.email) return false;
  if (isAuditAdmin(viewer)) return true;
  const allowedEmailsStr = process.env.ALLOWED_SEO_EMAILS || process.env.ALLOWED_ADD_CHANNEL_EMAILS || "";
  if (!allowedEmailsStr) {
    return true;
  }
  const allowed = allowedEmailsStr.split(",").map(e => e.trim().toLowerCase()).filter(Boolean);
  return allowed.includes(viewer.email.toLowerCase().trim());
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

async function connectedChannelEntries(viewer = null) {
  let profiles = await readProfiles();
  if (!profiles.length) {
    profiles = await migrateLegacyToken();
  }

  if (teamAuthEnabled() && viewer) {
    if (!isAuditAdmin(viewer)) {
      profiles = profiles.filter(p => p.userEmail === viewer.email);
    }
  }

  const removedIds = await readRemovedChannelIds();
  const removedSet = new Set(removedIds);

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
      if (!removedSet.has(channel.id)) {
        entries.push({ auth, profileId: profile.id, channel: { ...channel, profileId: profile.id } });
      }
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

async function readRemovedChannelIds() {
  if (sql) {
    await ensureStorage();
    const rows = await sql`select payload from app_state where key = 'removed_channel_ids' limit 1`;
    return Array.isArray(rows[0]?.payload) ? rows[0].payload : [];
  }
  try {
    return JSON.parse(await readFile(removedChannelsPath, "utf8"));
  } catch {
    return [];
  }
}

async function saveRemovedChannelIds(ids) {
  if (sql) {
    await ensureStorage();
    await sql`
      insert into app_state (key, payload)
      values ('removed_channel_ids', ${sql.json(ids)})
      on conflict (key) do update set payload = excluded.payload, updated_at = now()
    `;
    return;
  }
  await mkdir(dataDir, { recursive: true });
  await writeFile(removedChannelsPath, JSON.stringify(ids, null, 2), "utf8");
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

async function saveGoogleProfile(tokens, channels, userEmail = null) {
  const profiles = await readProfiles();
  const channelIds = new Set(channels.map((channel) => channel.id));

  // Automatically clear these channels from blacklist if they are re-added via Google login
  const removedIds = await readRemovedChannelIds();
  const filteredRemovedIds = removedIds.filter(id => !channelIds.has(id));
  await saveRemovedChannelIds(filteredRemovedIds);

  const existing = profiles.find((profile) => (profile.channels || []).some((channel) => channelIds.has(channel.id)));
  if (existing) {
    existing.tokens = { ...existing.tokens, ...tokens };
    existing.channels = channels;
    if (userEmail) {
      existing.userEmail = userEmail;
    }
  } else {
    profiles.push({ id: randomUUID(), tokens, channels, userEmail });
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
  const items = [];
  let pageToken = undefined;

  do {
    const remaining = Math.max(1, maxChannels - items.length);
    const response = await youtube.channels.list({
      part: ["snippet", "statistics", "contentDetails"],
      mine: true,
      maxResults: Math.min(50, remaining),
      pageToken,
    });
    items.push(...(response.data.items || []));
    pageToken = response.data.nextPageToken;
  } while (pageToken && items.length < maxChannels);

  return items.map((item) => ({
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

function isBrandKeyword(keyword, channelTitle) {
  const lowerKeyword = String(keyword || "").toLowerCase().trim();
  if (!lowerKeyword) return true;
  
  const brandTerms = ["testbook", "supercoaching", "super coaching", "preplab"];
  return brandTerms.some(term => lowerKeyword.includes(term));
}

async function fetchKeywordRank(youtube, keyword, targetChannelId) {
  try {
    const res = await youtube.search.list({
      part: ["snippet"],
      q: keyword,
      type: ["video"],
      maxResults: 50,
      regionCode: "IN",
    });
    const items = res.data.items || [];
    let rank = null;
    let videoId = null;
    let videoTitle = "";
    
    for (let index = 0; index < items.length; index++) {
      if (items[index].snippet?.channelId === targetChannelId) {
        rank = index + 1;
        videoId = items[index].id?.videoId || null;
        videoTitle = items[index].snippet?.title || "";
        break;
      }
    }
    return { rank, videoId, videoTitle };
  } catch (err) {
    console.error(`Error scanning rank for keyword "${keyword}":`, err.message);
    const isQuota = String(err.message || "").toLowerCase().includes("quota");
    return { rank: isQuota ? "quota_exceeded" : null, videoId: null, videoTitle: "" };
  }
}

async function refreshKeywordRankingsInternal(channelId) {
  const entries = await connectedChannelEntries();
  const entry = entries.find(e => e.channel.id === channelId);
  if (!entry) throw new Error("Channel not connected.");
  
  const auth = entry.auth;
  const youtube = google.youtube({ version: "v3", auth });
  
  const dates = dateWindow("7");
  const rows = await analyticsRows(auth, {
    ids: `channel==${channelId}`,
    startDate: dates.startDate,
    endDate: dates.endDate,
    dimensions: "insightTrafficSourceDetail",
    metrics: "views",
    filters: "insightTrafficSourceType==YT_SEARCH",
    sort: "-views",
    maxResults: 25,
  }).catch((err) => {
    console.error("analyticsRows error in refreshKeywordRankingsInternal:", err);
    return [];
  });
  
  const channelName = entry.channel.name || "";
  const filteredQueries = [];
  for (const row of rows) {
    const keyword = String(row[0] || "").trim();
    const views = Number(row[1] || 0);
    if (keyword && !isBrandKeyword(keyword, channelName)) {
      filteredQueries.push({ keyword, views });
    }
  }
  
  const top10 = filteredQueries
    .sort((a, b) => b.views - a.views)
    .slice(0, 10);
    
  const data = await readKeywordRankings();
  const manualKeywords = data[channelId]?.manualKeywords || [];
  
  const allKeywords = [
    ...top10.map(k => ({ keyword: k.keyword, views: k.views, type: "automated" })),
    ...manualKeywords.map(k => ({ keyword: k, views: null, type: "manual" }))
  ];
  
  const results = {
    automated: [],
    manual: []
  };
  
  const batchSize = 5;
  for (let idx = 0; idx < allKeywords.length; idx += batchSize) {
    const chunk = allKeywords.slice(idx, idx + batchSize);
    await Promise.all(chunk.map(async (item) => {
      const { rank, videoId, videoTitle } = await fetchKeywordRank(youtube, item.keyword, channelId);
      
      const prevData = data[channelId]?.rankings?.[item.type]?.find(r => r.keyword === item.keyword);
      const previousRank = prevData ? prevData.currentRank : null;
      const history = prevData ? [...(prevData.history || [])] : [];
      
      const todayStr = new Date().toISOString().slice(0, 10);
      const existingHistoryIndex = history.findIndex(h => h.date === todayStr);
      if (existingHistoryIndex !== -1) {
        history[existingHistoryIndex].rank = rank;
      } else {
        history.push({ date: todayStr, rank });
        if (history.length > 7) history.shift();
      }
      
      const record = {
        keyword: item.keyword,
        views: item.views,
        currentRank: rank,
        previousRank,
        history,
        videoId,
        videoTitle
      };
      
      results[item.type].push(record);
    }));
  }
  
  results.automated.sort((a, b) => (b.views || 0) - (a.views || 0));
  
  data[channelId] = {
    channelId,
    lastUpdated: new Date().toISOString(),
    manualKeywords,
    rankings: results
  };
  
  await saveKeywordRankings(data);
  return data[channelId];
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
  const playlists = [];
  if (channel.id && channel.id.startsWith("UC")) {
    const suffix = channel.id.slice(2);
    playlists.push({ id: "UULF" + suffix, format: "Video" });
    playlists.push({ id: "UUSH" + suffix, format: "Shorts" });
    playlists.push({ id: "UULV" + suffix, format: "Live" });
  } else if (channel.uploadsPlaylistId) {
    playlists.push({ id: channel.uploadsPlaylistId, format: null });
  }

  const videosPromises = playlists.map(async (playlist) => {
    const list = [];
    let pageToken;
    let reachedOlderVideos = false;
    do {
      try {
        const response = await youtube.playlistItems.list({
          part: ["snippet", "contentDetails"],
          playlistId: playlist.id,
          maxResults: 50,
          pageToken,
        });
        const items = response.data.items || [];
        if (items.length === 0) break;
        for (const item of items) {
          const publishedAt = item.contentDetails?.videoPublishedAt || item.snippet?.publishedAt;
          if (!publishedAt) continue;
          const dateOnly = publishedAt.slice(0, 10);
          if (dateOnly < dates.startDate) {
            reachedOlderVideos = true;
            break;
          }
          if (dateOnly <= dates.endDate) {
            list.push({
              id: item.contentDetails?.videoId,
              publishedAt,
              date: dateOnly,
              title: item.snippet?.title || "Untitled",
              format: playlist.format,
            });
          }
        }
        pageToken = reachedOlderVideos ? undefined : response.data.nextPageToken;
      } catch (error) {
        console.warn(`Could not load playlist ${playlist.id} for ${channel.id}:`, error.message);
        break;
      }
    } while (pageToken && list.length < 500);
    return list;
  });

  const results = await Promise.all(videosPromises);
  const videos = results.flat();

  const details = await videoDetails(auth, videos.map((video) => video.id));
  return videos.map((video) => ({
    ...video,
    format: video.format || details[video.id]?.format || "Video",
    views: Number(details[video.id]?.views || 0),
  }));
}

async function getPlaylistVideoIds(auth, playlistId) {
  return cached(
    makeCacheKey("playlist-video-ids", playlistId),
    30 * 60 * 1000, // 30 minutes
    async () => {
      const youtube = google.youtube({ version: "v3", auth });
      const ids = new Set();
      let pageToken = undefined;
      try {
        const res1 = await youtube.playlistItems.list({
          part: ["contentDetails"],
          playlistId: playlistId,
          maxResults: 50,
          pageToken,
        });
        for (const item of res1.data.items || []) {
          const vId = item.contentDetails?.videoId;
          if (vId) ids.add(vId);
        }
        pageToken = res1.data.nextPageToken;
        if (pageToken) {
          const res2 = await youtube.playlistItems.list({
            part: ["contentDetails"],
            playlistId: playlistId,
            maxResults: 50,
            pageToken,
          });
          for (const item of res2.data.items || []) {
            const vId = item.contentDetails?.videoId;
            if (vId) ids.add(vId);
          }
        }
      } catch (err) {
        // Ignore error
      }
      return Array.from(ids);
    }
  );
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
        
        const items = response.data.items || [];
        const results = [];
        
        for (const item of items) {
          const channelId = item.snippet?.channelId;
          let format = null;
          
          if (channelId && channelId.startsWith("UC")) {
            const suffix = channelId.slice(2);
            const [shortsIds, liveIds, videoIds] = await Promise.all([
              getPlaylistVideoIds(auth, "UUSH" + suffix),
              getPlaylistVideoIds(auth, "UULV" + suffix),
              getPlaylistVideoIds(auth, "UULF" + suffix),
            ]);
            
            if (shortsIds.includes(item.id)) {
              format = "Shorts";
            } else if (liveIds.includes(item.id)) {
              format = "Live";
            } else if (videoIds.includes(item.id)) {
              format = "Video";
            }
          }
          
          if (!format) {
            format = classifyVideo(item);
          }
          
          results.push({
            id: item.id,
            title: item.snippet?.title || item.id,
            channelTitle: item.snippet?.channelTitle || "",
            format,
            views: Number(item.statistics?.viewCount || 0),
          });
        }
        
        return results;
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
  return seconds > 0 && seconds <= 60 ? "Shorts" : "Video";
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
    .slice(0, 20);
  return { series, totals, topContent };
}

function buildAllInOneDashboard(entries, reports, dates) {
  const merged = mergeReports(reports, dates.days);
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
    dailyTotals: merged.series.map((day) => ({
      date: day.date,
      label: day.label,
      organicViews: Number(day.organicViews || 0),
      subscribers: Number(day.subscribers || 0),
    })),
    channelRankings: {
      organicViews: perChannel.slice().sort((a, b) => b.organicViews - a.organicViews),
      subscribers: perChannel.slice().sort((a, b) => b.subscribers - a.subscribers),
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
  const after = `${dates.startDate}T00:00:00Z`;
  const uploadsPlaylistId = channelId.startsWith("UC") ? "UU" + channelId.slice(2) : channelId;
  const playlistUrl = `https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId=${uploadsPlaylistId}&maxResults=50&key=${process.env.YOUTUBE_API_KEY}`;
  const playlistData = await fetchJson(playlistUrl);
  const ids = [];
  for (const item of playlistData.items || []) {
    const pubAt = item.snippet?.publishedAt;
    // Early termination: playlist items are sorted newest-to-oldest, so break if older than 'after'
    if (pubAt && pubAt < after) {
      break;
    }
    const videoId = item.snippet?.resourceId?.videoId;
    if (videoId) ids.push(videoId);
  }
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

async function categoryCompetitorReport(category, mappings, dates, options = {}) {
  const connectedEntries = await connectedChannelEntries().catch(() => []);
  const ownedChannelIds = [...new Set(connectedEntries.map((entry) => entry.channel.id))];
  
  const groups = [];
  for (const mapping of mappings) {
    const channelDetails = await publicChannelsByIds(mapping.ids);
    const channelReports = [];
    for (const channelId of mapping.ids) {
      const report = await publicChannelVideos(channelId, dates, mapping.group, channelDetails[channelId]?.title || channelId, options);
      channelReports.push(report);
      // Wait 150ms between requests if bypassing cache to prevent rate-limiting
      if (options.force) {
        await new Promise((resolve) => setTimeout(resolve, 150));
      }
    }
    const videos = channelReports.flat();
    groups.push({
      name: mapping.group,
      channels: mapping.ids.map((id) => ({ id, title: channelDetails[id]?.title || id })),
      videos,
      views: videos.reduce((sum, video) => sum + video.views, 0),
      engagement: engagementRate(videos),
      uploads: formatCounts(videos),
      averageViews: formatAverages(videos),
      viewSource: "publishedContentPublic",
    });
  }

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

async function publicChannelVideos(channelId, dates, group, channelTitle, options = {}) {
  return cached(
    makeCacheKey("public-channel-videos", channelId, group, dates.startDate, dates.endDate),
    ttl.publicVideos,
    () => loadPublicChannelVideos(channelId, dates, group, channelTitle),
    { force: Boolean(options.force) }
  );
}

async function loadPublicChannelVideos(channelId, dates, group, channelTitle) {
  const after = `${dates.startDate}T00:00:00Z`;
  const beforeDate = new Date(`${dates.endDate}T00:00:00Z`);
  beforeDate.setUTCDate(beforeDate.getUTCDate() + 1);
  const before = beforeDate.toISOString();
  
  const uploadsPlaylistId = channelId.startsWith("UC") ? "UU" + channelId.slice(2) : channelId;
  const livePlaylistId = channelId.startsWith("UC") ? "UULV" + channelId.slice(2) : channelId;
  
  const fetchPlaylistVideoIds = async (playlistId) => {
    const ids = [];
    let pageToken = "";
    let keepFetching = true;
    
    while (keepFetching) {
      try {
        const params = new URLSearchParams({
          part: "snippet",
          playlistId: playlistId,
          maxResults: "50",
          key: process.env.YOUTUBE_API_KEY
        });
        if (pageToken) params.set("pageToken", pageToken);
        const url = `https://www.googleapis.com/youtube/v3/playlistItems?${params.toString()}`;
        const data = await fetchJson(url);
        const items = data.items || [];
        if (items.length === 0) break;
        
        for (const item of items) {
          const pubAt = item.snippet?.publishedAt;
          if (pubAt && pubAt < after) {
            keepFetching = false;
            break;
          }
          if (pubAt && pubAt <= before) {
            const videoId = item.snippet?.resourceId?.videoId;
            if (videoId) ids.push(videoId);
          }
        }
        
        pageToken = data.nextPageToken;
        if (!pageToken) break;
      } catch (err) {
        break;
      }
    }
    return ids;
  };

  const uploadIds = await fetchPlaylistVideoIds(uploadsPlaylistId);
  const liveIds = await fetchPlaylistVideoIds(livePlaylistId);
  
  const uniqueIds = [...new Set([...uploadIds, ...liveIds])];
  if (!uniqueIds.length) return [];
  
  const liveVideoIds = new Set(liveIds);
  
  const details = [];
  for (let index = 0; index < uniqueIds.length; index += 50) {
    const chunk = uniqueIds.slice(index, index + 50);
    const detailUrl = `https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails,liveStreamingDetails&id=${chunk.join(",")}&key=${process.env.YOUTUBE_API_KEY}`;
    const detailData = await fetchJson(detailUrl);
    details.push(...(detailData.items || []));
  }
  
  return details.map((item) => {
    const format = classifyPublicVideo(item, liveVideoIds);
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

function classifyPublicVideo(video, liveVideoIds = new Set()) {
  const title = `${video.snippet?.title || ""} ${(video.snippet?.tags || []).join(" ")}`.toLowerCase();
  const seconds = isoDurationSeconds(video.contentDetails?.duration || "PT0S");
  
  const isCurrentlyLiveOrUpcoming = ["live", "upcoming"].includes(video.snippet?.liveBroadcastContent);
  
  // 1. If currently live or upcoming
  if (isCurrentlyLiveOrUpcoming) return "Live";
  
  // 2. If we have a live streams list for this channel
  if (liveVideoIds.size > 0) {
    if (liveVideoIds.has(video.id)) {
      return "Live";
    }
  } else {
    // 3. Fallback when liveVideoIds is not provided (e.g., in keyword research across multiple channels):
    // Use the liveStreamingDetails with duration & title keywords heuristic
    const hasLiveKeywords = /\b(live|livestream|live stream|streamed)\b/.test(title);
    if (video.liveStreamingDetails && (seconds === 0 || seconds > 1800 || hasLiveKeywords)) {
      return "Live";
    }
  }
  
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
  } else if (range === "prevMonth") {
    end.setDate(0);
    start = new Date(end);
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
  if (range === "selectMonth" || range === "prevMonth") {
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
