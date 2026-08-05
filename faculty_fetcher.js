/**
 * @file faculty_fetcher.js
 * YouTube Data-Fetch Layer for Faculty Performance Dashboard.
 */

const https = require("https");
const urlModule = require("url");

/**
 * @typedef {Object} VideoItem
 * @property {string} id - Video ID.
 * @property {string} title - Video Title.
 * @property {string} publishedAt - ISO 8601 published timestamp.
 * @property {'video' | 'short' | 'live'} type - Content format classification.
 * @property {number} views - Views gained within the date range.
 * @property {number} subsGained - Subscribers gained within the date range.
 * @property {number} [peakConcurrent] - Peak concurrent viewers (live items only).
 * @property {number} [durationSec] - Video duration in integer seconds.
 */

/**
 * Placeholder hook to retrieve stored peak concurrent viewers for a live stream.
 * NOTE: The official YouTube Analytics API does not return peak concurrent viewers
 * for ended streams unless they are queried in real-time or polled during the broadcast.
 * To populate this going forward, concurrentViewers must be polled during each broadcast
 * and the max value persisted in a database.
 * 
 * @param {string} videoId 
 * @returns {number|null} The stored peak concurrent viewers, or null if not found.
 */
function getStoredPeak(videoId) {
  // TODO: Connect this to your database/persistence layer (e.g., PostgreSQL or JSON storage)
  // return database.getPeak(videoId) || null;
  return null;
}

/**
 * Helper function to parse ISO 8601 Durations (e.g., "PT1H30M15S") into seconds.
 * 
 * @param {string} durationStr 
 * @returns {number} Duration in seconds.
 */
function parseISO8601Duration(durationStr) {
  if (!durationStr) return 0;
  const match = durationStr.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!match) return 0;
  const hours = parseInt(match[1] || 0, 10);
  const minutes = parseInt(match[2] || 0, 10);
  const seconds = parseInt(match[3] || 0, 10);
  return hours * 3600 + minutes * 60 + seconds;
}

/**
 * Robust HTTP client using Node's native HTTPS module. Includes automatic retries
 * with exponential backoff on 403 (auth glitches) and 429 (rate limits).
 * 
 * @param {string} url 
 * @param {Object} options 
 * @param {number} retries 
 * @param {number} delay 
 * @returns {Promise<any>} Parsed JSON response payload.
 */
function makeRequest(url, options = {}, retries = 3, delay = 1000) {
  return new Promise((resolve, reject) => {
    const execute = (attempt) => {
      const parsedUrl = urlModule.parse(url);
      const reqOpts = {
        hostname: parsedUrl.hostname,
        path: parsedUrl.path + (parsedUrl.search || ""),
        method: options.method || "GET",
        headers: options.headers || {}
      };

      const req = https.request(reqOpts, (res) => {
        let body = "";
        res.on("data", chunk => body += chunk);
        res.on("end", () => {
          if (res.statusCode === 403 || res.statusCode === 429) {
            if (attempt < retries) {
              setTimeout(() => execute(attempt + 1), delay * Math.pow(2, attempt));
            } else {
              reject(new Error(`HTTP ${res.statusCode}: ${body}`));
            }
            return;
          }

          if (res.statusCode < 200 || res.statusCode >= 300) {
            reject(new Error(`HTTP ${res.statusCode}: ${body}`));
            return;
          }

          try {
            resolve(JSON.parse(body));
          } catch (e) {
            reject(new Error(`Failed to parse JSON response: ${e.message}`));
          }
        });
      });

      req.on("error", (err) => {
        if (attempt < retries) {
          setTimeout(() => execute(attempt + 1), delay * Math.pow(2, attempt));
        } else {
          reject(err);
        }
      });

      if (options.body) {
        req.write(typeof options.body === "string" ? options.body : JSON.stringify(options.body));
      }
      req.end();
    };

    execute(0);
  });
}

/**
 * Fetches YouTube content and analytics performance data for a channel within a date range.
 * 
 * @param {Object} params
 * @param {string} params.channelId - Target YouTube Channel ID.
 * @param {string} params.startDate - Range start date (YYYY-MM-DD).
 * @param {string} params.endDate - Range end date (YYYY-MM-DD).
 * @param {string} params.accessToken - OAuth 2.0 access token.
 * @returns {Promise<VideoItem[]>} Array of video item metrics objects.
 */
async function fetchFacultyData({ channelId, startDate, endDate, accessToken }) {
  const headers = {
    "Authorization": `Bearer ${accessToken}`,
    "Accept": "application/json"
  };

  // 1. Get the Uploads Playlist ID for the channel
  const channelUrl = `https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id=${encodeURIComponent(channelId)}`;
  const channelRes = await makeRequest(channelUrl, { headers });
  if (!channelRes.items || !channelRes.items.length) {
    throw new Error(`Channel not found: ${channelId}`);
  }
  const uploadsPlaylistId = channelRes.items[0].contentDetails?.relatedPlaylists?.uploads;
  if (!uploadsPlaylistId) {
    throw new Error(`Uploads playlist not found for channel: ${channelId}`);
  }

  // 2. Page through PlaylistItems newest-to-oldest, collecting video IDs in date range
  const videoIds = [];
  let pageToken = "";
  let reachedOlder = false;

  do {
    const playlistUrl = `https://www.googleapis.com/youtube/v3/playlistItems?part=contentDetails,snippet&playlistId=${encodeURIComponent(uploadsPlaylistId)}&maxResults=50&pageToken=${pageToken}`;
    const playlistRes = await makeRequest(playlistUrl, { headers });
    const items = playlistRes.data?.items || playlistRes.items || [];
    if (!items.length) break;

    for (const item of items) {
      const publishedAt = item.contentDetails?.videoPublishedAt || item.snippet?.publishedAt;
      if (!publishedAt) continue;

      const dateOnly = publishedAt.slice(0, 10);
      if (dateOnly < startDate) {
        reachedOlder = true;
        break; // Stop collecting, we've passed the start date
      }

      if (dateOnly <= endDate) {
        const vId = item.contentDetails?.videoId;
        if (vId) videoIds.push(vId);
      }
    }

    pageToken = reachedOlder ? "" : (playlistRes.nextPageToken || "");
  } while (pageToken);

  if (!videoIds.length) return [];

  // 3. Batch fetch video details in chunks of 50 (to stay within Data API limits)
  const videoItemsMap = {};
  for (let i = 0; i < videoIds.length; i += 50) {
    const chunkIds = videoIds.slice(i, i + 50);
    const videoUrl = `https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,liveStreamingDetails,statistics&id=${chunkIds.join(",")}`;
    const videosRes = await makeRequest(videoUrl, { headers });
    const items = videosRes.items || [];

    for (const item of items) {
      const id = item.id;
      const snippet = item.snippet || {};
      const contentDetails = item.contentDetails || {};
      const liveStreamingDetails = item.liveStreamingDetails || null;

      // Type classification:
      // - live: actualStartTime is present in liveStreamingDetails
      // - short: reuse duration <= 60s fallback.
      // NOTE: YouTube Shorts can run up to 3 minutes now; duration alone is imperfect.
      // A robust check requires a HEAD request to `youtube.com/shorts/{id}` to test redirects.
      let type = "video";
      const durationSec = parseISO8601Duration(contentDetails.duration);
      
      if (liveStreamingDetails && liveStreamingDetails.actualStartTime) {
        type = "live";
      } else if (durationSec > 0 && durationSec <= 60) {
        type = "short"; // robust check: follow-redirect on youtube.com/shorts/{id}
      }

      const publishedAt = snippet.publishedAt || new Date().toISOString();

      videoItemsMap[id] = {
        id,
        title: snippet.title || "Untitled",
        publishedAt,
        type,
        views: 0, // Will be merged from Analytics API
        subsGained: 0, // Will be merged from Analytics API
        durationSec,
        ...(type === "live" ? { peakConcurrent: getStoredPeak(id) } : {})
      };
    }
  }

  // 4. Batch query YouTube Analytics API v2 in chunks of 200 (filter count limit)
  const videoKeys = Object.keys(videoItemsMap);
  for (let i = 0; i < videoKeys.length; i += 200) {
    const chunkKeys = videoKeys.slice(i, i + 200);
    
    // Query Analytics API v2 report sorted by views
    const analyticsUrl = `https://youtubeanalytics.googleapis.com/v2/reports` +
      `?ids=channel==MINE` +
      `&startDate=${startDate}` +
      `&endDate=${endDate}` +
      `&dimensions=video` +
      `&metrics=views,subscribersGained,estimatedMinutesWatched` +
      `&filters=video==${chunkKeys.join(",")}` +
      `&maxResults=200` +
      `&sort=-views`;

    try {
      const analyticsRes = await makeRequest(analyticsUrl, { headers });
      const rows = analyticsRes.rows || [];

      for (const row of rows) {
        const [vId, views, subsGained] = row;
        if (videoItemsMap[vId]) {
          videoItemsMap[vId].views = Number(views || 0);
          videoItemsMap[vId].subsGained = Number(subsGained || 0);
        }
      }
    } catch (err) {
      // Log warning but do not crash; default views and subsGained to 0 (robustness fallback)
      console.warn(`[Faculty Analytics Fetch Warning]: Failed to fetch analytics report chunk:`, err.message);
    }
  }

  return Object.values(videoItemsMap);
}

module.exports = { fetchFacultyData };
