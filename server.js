global.g = globalThis;
const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT || 3000);
const WebSocket = require("ws");
const { spawn } = require("child_process");
const os = require("os");
const EDGE_TRUSTED_CLIENT_TOKEN = "6A5AA1D4EAFF4E9FB37E23D68491D6F4";
const EDGE_OUTPUT_FORMAT = "audio-24khz-48kbitrate-mono-mp3";
const MAX_BATCH_SIZE = 8;

let edgeTokenCache = null;
let breaker = {
  state: "closed",
  failures: 0,
  openedAt: 0,
  cooldownMs: 90_000
};

// Tracks the newest prepare-batch request per video. When the user seeks,
// the extension sends a higher generation. Older batches cannot always be
// killed mid-Edge-TTS request, but the server can stop starting the remaining
// stale items and return quickly.
const latestPrepareByVideo = new Map();

const app = express();
app.use(cors({ origin: true, credentials: false }));
app.use(express.json({ limit: "8mb" }));

app.get("/", (req, res) => {
  res.json({ ok: true, service: "One Click Dub Fast Local Server", model: "fast", tts: "edge" });
});

app.get("/health", (req, res) => {
  res.json({ ok: true, breakerState: breaker.state, tokenCached: Boolean(edgeTokenCache), captionProxy: true });
});

app.get("/api/youtube/captions", async (req, res) => {
  try {
    const url = String(req.query.url || "");
    if (!url) return res.status(400).type("text/plain").send("missing url");
    const parsed = new URL(url);
    if (!/^https?:$/.test(parsed.protocol) || !/(^|\.)youtube\.com$/.test(parsed.hostname)) {
      return res.status(400).type("text/plain").send("only youtube.com caption URLs are allowed");
    }
    const youtubeCookie = getYoutubeCookieFromRequest(req);
    const upstream = await fetchWithTimeout(parsed.toString(), {
      headers: youtubeFetchHeaders({
        accept: "application/json, text/xml, application/xml, text/plain, */*",
        referer: "https://www.youtube.com/",
        cookie: youtubeCookie
      })
    }, 15_000);
    const body = await upstream.text();
    if (!upstream.ok) {
      console.warn("caption proxy upstream failed", upstream.status, body.slice(0, 160));
      return res.status(upstream.status).type("text/plain").send(body || ("upstream HTTP " + upstream.status));
    }
    if (process.env.OCD_VERBOSE_CAPTION_PROXY === "1") console.log("caption proxy", parsed.searchParams.get("lang"), parsed.searchParams.get("fmt"), "bytes", body.length);
    res.type(body.trim().startsWith("{") ? "application/json" : "text/plain").send(body);
  } catch (error) {
    console.error("/api/youtube/captions failed", error);
    res.status(500).type("text/plain").send(error?.message || String(error));
  }
});


app.get("/api/youtube/transcript", async (req, res) => {
  try {
    const videoId = String(req.query.videoId || "").trim();
    if (!/^[a-zA-Z0-9_-]{6,20}$/.test(videoId)) {
      return res.status(400).json({ ok: false, error: "invalid or missing videoId" });
    }
    const langs = String(req.query.langs || "en").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
    const result = await resolveYoutubeTranscript(videoId, langs, getYoutubeCookieFromRequest(req));
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error("/api/youtube/transcript failed", error);
    res.status(500).json({ ok: false, error: error?.message || String(error) });
  }
});

app.post("/api/youtube/transcript", async (req, res) => {
  try {
    const body = req.body || {};
    const videoId = String(body.videoId || "").trim();
    if (!/^[a-zA-Z0-9_-]{6,20}$/.test(videoId)) {
      return res.status(400).json({ ok: false, error: "invalid or missing videoId" });
    }
    const langs = Array.isArray(body.langs)
      ? body.langs.map(x => String(x).trim().toLowerCase()).filter(Boolean)
      : String(body.langs || "en").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
    const cookie = normalizeCookieString(body.youtubeCookie || getYoutubeCookieFromRequest(req));
    const result = await resolveYoutubeTranscript(videoId, langs, cookie);
    res.json({ ok: true, ...result, usedCookie: Boolean(cookie) });
  } catch (error) {
    console.error("/api/youtube/transcript failed", error);
    res.status(500).json({ ok: false, error: error?.message || String(error) });
  }
});

async function resolveYoutubeTranscript(videoId, requestedLangs, youtubeCookie = "") {
  youtubeCookie = normalizeCookieString(youtubeCookie || getYoutubeCookieFromEnvOrFile());
  const errors = [];

  // FAST PATH: On many current YouTube videos the timedtext caption URLs return 0 bytes
  // and Innertube get_transcript returns failedPrecondition. Those retries waste time and
  // spam the terminal. Use yt-dlp first because it has been the only reliable source in
  // your logs.
  try {
    console.log("yt-dlp transcript primary running", videoId);
    const transcript = await resolveYoutubeTranscriptViaYtDlp(videoId, requestedLangs);
    if (transcript?.captions?.length) {
      console.log("yt-dlp transcript primary", videoId, transcript.sourceLanguage || "unknown", "captions", transcript.captions.length);
      return transcript;
    }
  } catch (error) {
    errors.push("yt-dlp: " + (error?.message || error));
    console.warn("yt-dlp transcript primary failed", error?.message || error);
  }

  // Slow fallbacks are disabled by default because they repeatedly return empty bodies
  // for your tested videos. Re-enable only when debugging a video where yt-dlp fails.
  if (process.env.OCD_ENABLE_SLOW_YOUTUBE_FALLBACKS !== "1") {
    throw new Error("yt-dlp transcript failed; slow timedtext/innertube fallbacks are disabled. Set OCD_ENABLE_SLOW_YOUTUBE_FALLBACKS=1 to debug. " + errors.join(" | "));
  }

  console.log("youtube auth cookie", youtubeCookie ? "present" : "missing");
  const tracks = await getYoutubeCaptionTracksServer(videoId, youtubeCookie);
  if (!tracks.length) throw new Error("no captionTracks from YouTube watch/player APIs; " + errors.join(" | "));

  const ordered = orderServerCaptionTracks(tracks, requestedLangs);
  for (const track of ordered) {
    const variants = buildServerCaptionUrlVariants(track.baseUrl, track, videoId);
    for (const url of variants) {
      try {
        const body = await fetchCaptionBody(url, youtubeCookie);
        const captions = parseCaptionBodyServer(body);
        if (captions.length) {
          console.log("transcript fallback", videoId, track.languageCode, track.kind || "manual", "captions", captions.length);
          return { videoId, sourceLanguage: track.languageCode || "unknown", sourceKind: track.kind || "manual", captions };
        }
        errors.push(`${track.languageCode || "unknown"}: len=${body.length}`);
      } catch (error) {
        errors.push(`${track.languageCode || "unknown"}: ${error?.message || error}`);
      }
    }
  }
  try {
    const transcript = await resolveYoutubeTranscriptViaInnertube(videoId, requestedLangs, youtubeCookie);
    if (transcript?.captions?.length) {
      console.log("innertube transcript fallback", videoId, transcript.sourceLanguage || "unknown", "captions", transcript.captions.length);
      return transcript;
    }
  } catch (error) {
    errors.push("innertube: " + (error?.message || error));
    console.warn("innertube transcript fallback failed", error?.message || error);
  }

  throw new Error("all transcript methods failed: " + errors.slice(-10).join(" | "));
}


async function resolveYoutubeTranscriptViaYtDlp(videoId, requestedLangs = []) {
  const videoUrl = "https://www.youtube.com/watch?v=" + encodeURIComponent(videoId);
  const tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "ocd-ytdlp-"));
  const wanted = (requestedLangs || []).map(x => String(x || "").toLowerCase()).filter(Boolean);
  const primary = wanted[0] || "en";
  const subLangs = Array.from(new Set([primary, primary + ".*", "en", "en.*"])).join(",");
  const outTpl = path.join(tmpDir, "sub.%(ext)s");

  const argsBase = [
    "-m", "yt_dlp",
    "--skip-download",
    "--ignore-no-formats-error",
    "--write-auto-subs",
    "--write-subs",
    "--sub-langs", subLangs,
    "--sub-format", "json3/vtt/srv3/ttml/best",
    "--no-playlist",
    "--no-warnings",
    "-o", outTpl,
    videoUrl
  ];

  const attempts = [
    ["py", argsBase],
    ["python", argsBase],
    ["py", ["-m", "yt_dlp", "--cookies-from-browser", "chrome", ...argsBase.slice(2)]],
    ["python", ["-m", "yt_dlp", "--cookies-from-browser", "chrome", ...argsBase.slice(2)]]
  ];

  const errors = [];
  try {
    for (const [cmd, args] of attempts) {
      try {
        console.log("yt-dlp transcript fallback running", cmd, args.includes("--cookies-from-browser") ? "with-browser-cookies" : "no-browser-cookies", videoId);
        await spawnToPromise(cmd, args, { timeoutMs: 120_000 });
        const captions = await readYtDlpCaptionFiles(tmpDir);
        if (captions.captions.length) {
          return { videoId, sourceLanguage: captions.language || primary, sourceKind: "yt-dlp", captions: captions.captions };
        }
        errors.push(cmd + ": no subtitle files/cues produced");
      } catch (error) {
        errors.push(cmd + ": " + (error?.message || error));
      }
    }
    throw new Error(errors.slice(-4).join(" | "));
  } finally {
    fs.promises.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
  }
}

async function readYtDlpCaptionFiles(tmpDir) {
  const entries = await fs.promises.readdir(tmpDir).catch(() => []);
  const files = entries
    .filter(name => /\.(json3|vtt|srv3|ttml|xml)$/i.test(name))
    .map(name => path.join(tmpDir, name));
  for (const file of files) {
    const text = await fs.promises.readFile(file, "utf8").catch(() => "");
    const captions = parseCaptionBodyServer(text);
    if (captions.length) {
      const base = path.basename(file);
      const langMatch = base.match(/\.([a-z]{2}(?:-[A-Z]{2})?(?:\.[^.]*)?)\.(?:json3|vtt|srv3|ttml|xml)$/);
      return { language: langMatch ? langMatch[1].split(".")[0] : "unknown", captions };
    }
  }
  return { language: "unknown", captions: [] };
}

function spawnToPromise(cmd, args, options = {}) {
  const timeoutMs = options.timeoutMs || 60_000;
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { windowsHide: true });
    let stdout = "";
    let stderr = "";
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(cmd + " timed out after " + timeoutMs + "ms"));
    }, timeoutMs);
    child.stdout.on("data", chunk => { stdout += String(chunk); });
    child.stderr.on("data", chunk => { stderr += String(chunk); });
    child.on("error", error => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", code => {
      clearTimeout(timer);
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(cmd + " exited " + code + ": " + (stderr || stdout).slice(-800)));
    });
  });
}

async function resolveYoutubeTranscriptViaInnertube(videoId, requestedLangs, youtubeCookie = "") {
  const htmlUrl = "https://www.youtube.com/watch?v=" + encodeURIComponent(videoId) + "&hl=en&persist_hl=1&has_verified=1&bpctr=9999999999";
  const htmlRes = await fetchWithTimeout(htmlUrl, {
    headers: {
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
      ...(youtubeCookie ? { "Cookie": youtubeCookie } : {})
    }
  }, 20_000);
  const html = await htmlRes.text();
  if (!htmlRes.ok || !html.trim()) throw new Error("watch HTML unavailable for transcript: HTTP " + htmlRes.status);

  const key = extractStringValueServer(html, "INNERTUBE_API_KEY") || extractStringValueServer(html, "innertubeApiKey");
  if (!key) throw new Error("missing INNERTUBE_API_KEY");
  const version = extractStringValueServer(html, "INNERTUBE_CLIENT_VERSION") || "2.20240601.00.00";
  const visitorData = extractStringValueServer(html, "VISITOR_DATA") || extractStringValueServer(html, "visitorData");
  const wanted = (requestedLangs || []).map(x => String(x || "").toLowerCase()).filter(Boolean);
  const hl = wanted[0] || "en";

  const paramsList = [];
  const addParams = value => {
    const p = String(value || "").trim();
    if (p && !paramsList.includes(p)) paramsList.push(p);
  };

  const initialDataJson = extractJsonAfterServer(html, "ytInitialData");
  if (initialDataJson) {
    try {
      const initialData = JSON.parse(initialDataJson);
      for (const wrapper of findObjectsByKeyServer(initialData, "getTranscriptEndpoint")) {
        addParams(wrapper?.getTranscriptEndpoint?.params);
      }
    } catch (error) {
      console.warn("ytInitialData transcript parse failed", error?.message || error);
    }
  }

  // A newer/more reliable source for transcript params is youtubei/v1/next.
  // Some watch-page params return HTTP 400, while next returns the active panel endpoint.
  try {
    const nextData = await callYoutubeNextServer({ key, videoId, version, visitorData, hl, referer: htmlUrl, cookie: youtubeCookie });
    for (const wrapper of findObjectsByKeyServer(nextData, "getTranscriptEndpoint")) {
      addParams(wrapper?.getTranscriptEndpoint?.params);
    }
  } catch (error) {
    console.warn("youtubei next transcript params failed", error?.message || error);
  }

  if (!paramsList.length) throw new Error("no getTranscriptEndpoint params in watch/next data");

  const errors = [];
  console.log("innertube transcript params", videoId, paramsList.length);

  for (const params of paramsList) {
    // Try the current WEB client first, then a conservative fallback version.
    for (const client of [
      { name: "WEB", version },
      { name: "WEB", version: "2.20240601.00.00" }
    ]) {
      try {
        const data = await callGetTranscriptServer({ key, params, clientName: client.name, clientVersion: client.version, visitorData, hl, referer: htmlUrl, cookie: youtubeCookie });
        const captions = parseInnertubeTranscriptSegmentsServer(data);
        if (captions.length) {
          return { videoId, sourceLanguage: hl, sourceKind: "innertube-transcript", captions };
        }
        errors.push(`get_transcript no segments client=${client.name}/${client.version}`);
      } catch (error) {
        errors.push(error?.message || String(error));
      }
    }
  }
  throw new Error(errors.slice(-6).join(" | ") || "get_transcript failed");
}

async function callYoutubeNextServer({ key, videoId, version, visitorData, hl, referer, cookie }) {
  const url = "https://www.youtube.com/youtubei/v1/next?key=" + encodeURIComponent(key);
  const context = buildInnertubeContextServer({ clientName: "WEB", clientVersion: version, visitorData, hl });
  const res = await fetchWithTimeout(url, {
    method: "POST",
    headers: buildYoutubeiHeadersServer({ referer, clientName: "1", clientVersion: version, visitorData, cookie }),
    body: JSON.stringify({
      context,
      videoId,
      contentCheckOk: true,
      racyCheckOk: true
    })
  }, 20_000);
  const text = await res.text();
  if (!res.ok || !text.trim()) {
    throw new Error("next HTTP " + res.status + " len=" + text.length + " body=" + text.slice(0, 220));
  }
  try { return JSON.parse(text); }
  catch { throw new Error("next non-json len=" + text.length + " body=" + text.slice(0, 220)); }
}

async function callGetTranscriptServer({ key, params, clientName, clientVersion, visitorData, hl, referer, cookie }) {
  const url = "https://www.youtube.com/youtubei/v1/get_transcript?key=" + encodeURIComponent(key);
  const context = buildInnertubeContextServer({ clientName, clientVersion, visitorData, hl });
  const res = await fetchWithTimeout(url, {
    method: "POST",
    headers: buildYoutubeiHeadersServer({ referer, clientName: "1", clientVersion, visitorData, cookie }),
    body: JSON.stringify({ context, params })
  }, 20_000);
  const text = await res.text();
  if (!res.ok || !text.trim()) {
    throw new Error("get_transcript HTTP " + res.status + " len=" + text.length + " body=" + text.slice(0, 220));
  }
  try { return JSON.parse(text); }
  catch { throw new Error("get_transcript non-json len=" + text.length + " body=" + text.slice(0, 220)); }
}

function buildInnertubeContextServer({ clientName, clientVersion, visitorData, hl }) {
  const client = {
    clientName,
    clientVersion,
    hl: hl || "en",
    gl: "US",
    utcOffsetMinutes: 0
  };
  if (visitorData) client.visitorData = visitorData;
  return {
    client,
    request: { useSsl: true, internalExperimentFlags: [], consistencyTokenJars: [] },
    user: { lockedSafetyMode: false }
  };
}

function buildYoutubeiHeadersServer({ referer, clientName, clientVersion, visitorData, cookie }) {
  const headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.youtube.com",
    "Referer": referer || "https://www.youtube.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "X-YouTube-Client-Name": clientName || "1",
    "X-YouTube-Client-Version": clientVersion || "2.20240601.00.00"
  };
  if (visitorData) headers["X-Goog-Visitor-Id"] = visitorData;
  if (cookie) headers["Cookie"] = cookie;
  return headers;
}

function parseInnertubeTranscriptSegmentsServer(data) {
  const renderers = findObjectsByKeyServer(data, "transcriptSegmentRenderer").map(x => x.transcriptSegmentRenderer).filter(Boolean);
  const out = [];
  for (const r of renderers) {
    const text = runsToTextServer(r.snippet?.runs || r.snippet || r.text?.runs || r.text).replace(/\s+/g, " ").trim();
    if (!text) continue;
    const startMs = Number(r.startMs || r.startTimeMs || 0);
    const endMs = Number(r.endMs || r.endTimeMs || 0);
    let durMs = Number(r.durationMs || 0);
    if (!durMs && endMs > startMs) durMs = endMs - startMs;
    if (!durMs) durMs = 1600;
    const start = startMs / 1000;
    out.push({ start, end: start + Math.max(0.6, durMs / 1000), dur: Math.max(0.6, durMs / 1000), text });
  }
  return mergeServerCaptions(out);
}

function runsToTextServer(value) {
  if (!value) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) return value.map(r => r?.text || r?.utf8 || "").join("");
  if (Array.isArray(value.runs)) return value.runs.map(r => r?.text || r?.utf8 || "").join("");
  if (typeof value.simpleText === "string") return value.simpleText;
  return "";
}

function findObjectsByKeyServer(root, key) {
  const found = [];
  const seen = new Set();
  const walk = value => {
    if (!value || typeof value !== "object" || seen.has(value)) return;
    seen.add(value);
    if (Object.prototype.hasOwnProperty.call(value, key)) found.push(value);
    if (Array.isArray(value)) {
      for (const item of value) walk(item);
    } else {
      for (const item of Object.values(value)) walk(item);
    }
  };
  walk(root);
  return found;
}

async function getYoutubeCaptionTracksServer(videoId, youtubeCookie = "") {
  const htmlUrl = "https://www.youtube.com/watch?v=" + encodeURIComponent(videoId) + "&hl=en&persist_hl=1&has_verified=1&bpctr=9999999999";
  const htmlRes = await fetchWithTimeout(htmlUrl, {
    headers: {
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
      ...(youtubeCookie ? { "Cookie": youtubeCookie } : {})
    }
  }, 20_000);
  const html = await htmlRes.text();
  if (!htmlRes.ok) throw new Error("watch HTML HTTP " + htmlRes.status + ": " + html.slice(0, 120));

  const tracks = [];
  const pushTracks = arr => {
    if (!Array.isArray(arr)) return;
    for (const t of arr) if (t?.baseUrl && !tracks.some(x => x.baseUrl === t.baseUrl)) tracks.push(t);
  };

  const playerJson = extractJsonAfterServer(html, "ytInitialPlayerResponse");
  if (playerJson) {
    try { pushTracks(JSON.parse(playerJson)?.captions?.playerCaptionsTracklistRenderer?.captionTracks); } catch {}
  }
  const arrJson = extractArrayAfterKeyServer(html, "captionTracks");
  if (arrJson) {
    try { pushTracks(JSON.parse(arrJson.replace(/\\u0026/g, "&"))); } catch {}
  }

  // If page parsing returns stale/empty URLs, ask YouTube's player API for fresh caption URLs.
  try {
    const key = extractStringValueServer(html, "INNERTUBE_API_KEY") || extractStringValueServer(html, "innertubeApiKey");
    const version = extractStringValueServer(html, "INNERTUBE_CLIENT_VERSION") || "2.20240601.00.00";
    const visitorData = extractStringValueServer(html, "VISITOR_DATA") || extractStringValueServer(html, "visitorData");
    if (key) {
      const playerApi = "https://www.youtube.com/youtubei/v1/player?key=" + encodeURIComponent(key);
      const apiRes = await fetchWithTimeout(playerApi, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "Origin": "https://www.youtube.com",
          "Referer": htmlUrl,
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
          ...(youtubeCookie ? { "Cookie": youtubeCookie } : {})
        },
        body: JSON.stringify({
          videoId,
          context: {
            client: {
              clientName: "WEB",
              clientVersion: version,
              hl: "en",
              gl: "US",
              visitorData
            }
          },
          playbackContext: { contentPlaybackContext: { html5Preference: "HTML5_PREF_WANTS" } },
          contentCheckOk: true,
          racyCheckOk: true
        })
      }, 20_000);
      const apiText = await apiRes.text();
      if (apiRes.ok && apiText.trim()) {
        try { pushTracks(JSON.parse(apiText)?.captions?.playerCaptionsTracklistRenderer?.captionTracks); } catch {}
      } else {
        console.warn("youtubei player returned", apiRes.status, apiText.slice(0, 120));
      }
    }
  } catch (error) {
    console.warn("youtubei player fallback failed", error?.message || error);
  }

  console.log("server caption tracks", videoId, tracks.length, tracks.map(t => `${t.languageCode || "?"}:${t.kind || "manual"}`).join(", "));
  return tracks;
}

function orderServerCaptionTracks(tracks, requestedLangs) {
  const langs = (requestedLangs || []).map(x => x.toLowerCase());
  const score = t => {
    const lang = String(t.languageCode || "").toLowerCase();
    const kind = String(t.kind || "").toLowerCase();
    let n = 0;
    if (!kind.includes("asr")) n += 30;
    const langIndex = langs.findIndex(l => lang === l || lang.startsWith(l + "-") || l.startsWith(lang + "-"));
    if (langIndex >= 0) n += 60 - langIndex;
    if (lang === "en" || lang.startsWith("en")) n += 5;
    return -n;
  };
  return [...tracks].sort((a, b) => score(a) - score(b));
}

function buildServerCaptionUrlVariants(baseUrl, track, videoId) {
  const out = [];
  const add = u => { if (u && !out.includes(u)) out.push(u); };
  const clean = decodeServerCaptionUrl(baseUrl);
  if (clean) {
    for (const fmt of ["json3", "srv3", "ttml", "srv1"]) add(setUrlParamServer(clean, "fmt", fmt));
    add(clean);
  }
  const lang = track?.languageCode || "en";
  for (const fmt of ["json3", "srv3", "ttml", "srv1"]) {
    const u = new URL("https://www.youtube.com/api/timedtext");
    u.searchParams.set("v", videoId);
    u.searchParams.set("lang", lang);
    u.searchParams.set("fmt", fmt);
    if (track?.kind) u.searchParams.set("kind", track.kind);
    add(u.toString());
  }
  return out;
}

async function fetchCaptionBody(url, youtubeCookie = "") {
  const res = await fetchWithTimeout(url, {
    headers: {
      "Accept": "application/json, text/xml, application/xml, text/plain, */*",
      "Accept-Language": "en-US,en;q=0.9",
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
      "Referer": "https://www.youtube.com/",
      ...(youtubeCookie ? { "Cookie": youtubeCookie } : {})
    }
  }, 15_000);
  const body = await res.text();
  if (!res.ok) throw new Error("HTTP " + res.status + ": " + body.slice(0, 120));
  return body || "";
}

function parseCaptionBodyServer(body) {
  const text = String(body || "").trim();
  if (!text) return [];
  if (text.startsWith("{") || text.startsWith("[")) {
    try {
      const data = JSON.parse(text);
      const out = [];
      for (const ev of data?.events || []) {
        if (!ev?.segs || typeof ev.tStartMs !== "number") continue;
        const captionText = ev.segs.map(s => s?.utf8 || "").join("").replace(/\s+/g, " ").trim();
        if (!captionText) continue;
        const start = ev.tStartMs / 1000;
        const dur = Math.max(0.6, Number(ev.dDurationMs || 1600) / 1000);
        out.push({ start, end: start + dur, dur, text: captionText });
      }
      return mergeServerCaptions(out);
    } catch {}
  }
  const out = [];
  for (const m of text.matchAll(/<text\b([^>]*)>([\s\S]*?)<\/text>/g)) {
    const attrs = m[1] || "";
    const start = Number((attrs.match(/\bstart="([^"]+)"/) || [])[1] || 0);
    const dur = Math.max(0.6, Number((attrs.match(/\bdur="([^"]+)"/) || [])[1] || 1.6));
    const captionText = decodeHtmlServer(m[2]).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    if (captionText) out.push({ start, end: start + dur, dur, text: captionText });
  }
  if (out.length) return mergeServerCaptions(out);
  for (const m of text.matchAll(/<p\b([^>]*)>([\s\S]*?)<\/p>/g)) {
    const attrs = m[1] || "";
    const startMs = Number((attrs.match(/\bt="([^"]+)"/) || [])[1] || 0);
    const durMs = Math.max(600, Number((attrs.match(/\bd="([^"]+)"/) || [])[1] || 1600));
    const captionText = decodeHtmlServer(m[2]).replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();
    if (captionText) out.push({ start: startMs / 1000, end: (startMs + durMs) / 1000, dur: durMs / 1000, text: captionText });
  }
  return mergeServerCaptions(out);
}

function mergeServerCaptions(items) {
  const merged = [];
  for (const item of items.filter(x => x.text && x.end > x.start)) {
    const last = merged[merged.length - 1];
    if (last && item.start - last.end < 0.25 && (last.text + " " + item.text).length < 110 && last.end - last.start < 1.8) {
      last.text = `${last.text} ${item.text}`.replace(/\s+/g, " ").trim();
      last.end = item.end;
      last.dur = last.end - last.start;
    } else {
      merged.push({ ...item });
    }
  }
  return merged.map((c, index) => ({ index, ...c }));
}

function decodeServerCaptionUrl(url) {
  return String(url || "").replace(/\\u0026/g, "&").replace(/&amp;/g, "&").replace(/\\\//g, "/").trim();
}

function setUrlParamServer(url, key, value) {
  try {
    const u = new URL(url);
    u.searchParams.set(key, value);
    return u.toString();
  } catch {
    const clean = String(url).replace(new RegExp("([?&])" + key + "=[^&]*", "g"), "$1").replace(/[?&]$/, "");
    return clean + (clean.includes("?") ? "&" : "?") + encodeURIComponent(key) + "=" + encodeURIComponent(value);
  }
}

function extractStringValueServer(text, key) {
  const patterns = [
    new RegExp('"' + escapeRegExpServer(key) + '"\\s*:\\s*"((?:\\\\.|[^"\\\\])*)"'),
    new RegExp(escapeRegExpServer(key) + '\\s*=\\s*"((?:\\\\.|[^"\\\\])*)"')
  ];
  for (const re of patterns) {
    const m = String(text).match(re);
    if (m) return unescapeJsonStringServer(m[1]);
  }
  return "";
}

function extractJsonAfterServer(text, varName) {
  const idx = String(text).indexOf(varName);
  if (idx < 0) return "";
  const eq = text.indexOf("=", idx);
  const colon = text.indexOf(":", idx);
  let pivot = eq >= 0 ? eq : colon;
  if (eq >= 0 && colon >= 0) pivot = Math.min(eq, colon);
  if (pivot < 0) return "";
  const start = text.indexOf("{", pivot);
  if (start < 0) return "";
  return extractBalancedServer(text, start, "{", "}");
}

function extractArrayAfterKeyServer(text, key) {
  const keyIdx = String(text).indexOf(`"${key}"`);
  if (keyIdx < 0) return "";
  const colon = text.indexOf(":", keyIdx);
  if (colon < 0) return "";
  const start = text.indexOf("[", colon);
  if (start < 0) return "";
  return extractBalancedServer(text, start, "[", "]");
}

function extractBalancedServer(text, start, open, close) {
  let depth = 0, inStr = false, esc = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inStr) {
      if (esc) esc = false;
      else if (ch === "\\") esc = true;
      else if (ch === '"') inStr = false;
    } else {
      if (ch === '"') inStr = true;
      else if (ch === open) depth++;
      else if (ch === close) {
        depth--;
        if (depth === 0) return text.slice(start, i + 1);
      }
    }
  }
  return "";
}

function decodeHtmlServer(value) {
  return String(value || "")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, n) => String.fromCharCode(parseInt(n, 16)));
}

function unescapeJsonStringServer(s) {
  try { return JSON.parse('"' + String(s).replace(/"/g, '\\"') + '"'); } catch { return String(s); }
}

function escapeRegExpServer(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}


function getYoutubeCookieFromRequest(req) {
  return normalizeCookieString(
    req?.headers?.["x-youtube-cookie"] ||
    req?.headers?.["youtube-cookie"] ||
    req?.body?.youtubeCookie ||
    getYoutubeCookieFromEnvOrFile()
  );
}

function getYoutubeCookieFromEnvOrFile() {
  const envCookie = normalizeCookieString(process.env.YOUTUBE_COOKIE || "");
  if (envCookie) return envCookie;
  for (const fileName of ["youtube.cookie.txt", "youtube_cookies.txt", "cookies.txt"]) {
    try {
      const filePath = path.join(__dirname, fileName);
      if (fs.existsSync(filePath)) {
        const cookie = normalizeCookieString(fs.readFileSync(filePath, "utf8"));
        if (cookie) return cookie;
      }
    } catch {}
  }
  return "";
}

function normalizeCookieString(value) {
  const s = String(value || "").trim();
  if (!s) return "";
  // Accept a normal Cookie header. If the user pasted Netscape cookies.txt, convert usable rows.
  if (s.includes("=") && s.includes(";")) return s.replace(/[\r\n]+/g, "; ").replace(/;\s*;/g, ";");
  const pairs = [];
  for (const line of s.split(/\r?\n/)) {
    const row = line.trim();
    if (!row || row.startsWith("#")) continue;
    const cols = row.split(/\t+/);
    if (cols.length >= 7) pairs.push(cols[5] + "=" + cols.slice(6).join("="));
    else if (/^[^=\s]+=/.test(row)) pairs.push(row.replace(/;$/, ""));
  }
  return pairs.join("; ");
}

function youtubeFetchHeaders({ accept, referer, cookie }) {
  const headers = {
    "Accept": accept || "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    "Referer": referer || "https://www.youtube.com/",
    "Origin": "https://www.youtube.com"
  };
  if (cookie) headers["Cookie"] = cookie;
  return headers;
}

app.post("/api/fast/sample", async (req, res) => {
  try {
    const payload = req.body || {};
    const batch = await prepareBatch({
      targetLanguage: payload.targetLanguage || "en-US",
      voiceName: payload.voiceName,
      subtitles: [{ index: 0, text: payload.text || "", start: 0, end: 1 }]
    });
    const first = batch.results?.[0] || {};
    res.json({
      ok: true,
      translation: first.translation || payload.text || "",
      voiceName: first.voiceName,
      audioBase64: first.audioBase64 || "",
      breakerState: breaker.state
    });
  } catch (error) {
    res.status(500).json({ ok: false, error: error?.message || String(error), breakerState: breaker.state });
  }
});

app.post("/api/fast/prepare-batch", async (req, res) => {
  try {
    const result = await prepareBatch(req.body || {});
    res.json(result);
  } catch (error) {
    console.error("/api/fast/prepare-batch failed", error);
    res.status(500).json({ ok: false, error: error?.message || String(error), breakerState: breaker.state });
  }
});

async function prepareBatch(payload) {
  const subtitles = Array.isArray(payload.subtitles) ? payload.subtitles.slice(0, MAX_BATCH_SIZE) : [];
  const targetLanguage = normalizeTargetLanguage(payload.targetLanguage || "en-US");
  const voiceName = payload.voiceName || defaultEdgeVoiceFor(targetLanguage);
  const startedAt = Date.now();

  const videoId = String(payload.videoId || payload.videoID || "global").trim() || "global";
  const generation = Number(payload.generation || 0);
  const requestId = String(payload.requestId || `${videoId}:${generation}:${Date.now()}`);
  const priority = String(payload.priority || "normal");

  const previous = latestPrepareByVideo.get(videoId);
  if (!previous || generation > Number(previous.generation || 0) || priority === "seek") {
    latestPrepareByVideo.set(videoId, { generation, requestId, priority, startedAt });
  }

  const isCurrentRequest = () => {
    const latest = latestPrepareByVideo.get(videoId);
    if (!latest) return true;
    if (Number(latest.generation || 0) > generation) return false;
    return true;
  };

  if (!subtitles.length) {
    return { ok: true, chain: "LOCAL_FAST_EDGE_TTS", model: "fast", voiceType: "free", results: [] };
  }

  ensureBreakerAllowsWork();

  // Seek requests should return quickly. Larger background batches can continue
  // only while they are still current; after a seek they stop starting new TTS.
  const defaultConcurrency = priority === "seek" ? 2 : 2;
  const CONCURRENCY = Math.min(3, Math.max(1, Number(process.env.OCD_TTS_CONCURRENCY || defaultConcurrency)));

  console.info("[OCD] prepare-batch started", {
    videoId,
    generation,
    requestId,
    priority,
    requested: subtitles.length,
    concurrency: CONCURRENCY,
    indexes: subtitles.map(s => s.index)
  });

  const results = await mapLimit(subtitles, CONCURRENCY, async (sub) => {
    if (!isCurrentRequest()) {
      return { index: sub.index, translation: "", audioBase64: "", provider: "edge-local-server-python", outcome: "stale-before-start" };
    }

    const text = String(sub.text || "").trim();
    if (!text) return { index: sub.index, translation: "", audioBase64: "", provider: "edge", voiceName, outcome: "empty" };

    const itemStartedAt = Date.now();
    try {
      const translation = await googleTranslate(text, targetLanguage);
      if (!isCurrentRequest()) {
        return { index: sub.index, translation: translation || text, audioBase64: "", provider: "edge-local-server-python", voiceName, elapsedMs: Date.now() - itemStartedAt, outcome: "stale-after-translate" };
      }

      const spokenText = translation || text;
      const audioBase64 = await edgeTts(spokenText, voiceName, "+0%", "+0Hz");

      if (!isCurrentRequest()) {
        return { index: sub.index, translation: spokenText, audioBase64: "", provider: "edge-local-server-python", voiceName, elapsedMs: Date.now() - itemStartedAt, outcome: "stale-after-tts" };
      }

      markBreakerSuccess();
      return {
        index: sub.index,
        translation: spokenText,
        audioBase64,
        provider: "edge-local-server-python",
        voiceName,
        elapsedMs: Date.now() - itemStartedAt,
        outcome: "ok"
      };
    } catch (error) {
      markBreakerFailure(error);
      throw error;
    }
  });

  const usableResults = results.filter(r => r && r.outcome !== "stale-before-start" && r.outcome !== "stale-after-translate" && r.outcome !== "stale-after-tts");

  console.info("[OCD] prepare-batch completed", {
    videoId,
    generation,
    requestId,
    priority,
    requested: subtitles.length,
    results: usableResults.length,
    stale: results.length - usableResults.length,
    elapsedMs: Date.now() - startedAt
  });

  return {
    ok: true,
    chain: "LOCAL_FAST_EDGE_TTS",
    model: "fast",
    voiceType: "free",
    targetLanguage,
    voiceName,
    breakerState: breaker.state,
    elapsedMs: Date.now() - startedAt,
    requestId,
    generation,
    priority,
    results: usableResults
  };
}

async function mapLimit(items, limit, worker) {
  const results = new Array(items.length);
  let cursor = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (cursor < items.length) {
      const i = cursor++;
      results[i] = await worker(items[i], i);
    }
  });
  await Promise.all(workers);
  return results;
}

function ensureBreakerAllowsWork() {
  // Local development mode: never block the pipeline because of a previous TTS failure.
  // We want the next request to expose the real Python edge-tts error instead of hiding it behind breaker state.
  if (breaker.state === "open") {
    console.warn("[OCD] breaker was open; resetting it for local Python edge-tts retry");
    breaker.state = "closed";
    breaker.failures = 0;
    breaker.openedAt = 0;
  }
}

function markBreakerSuccess() {
  breaker.failures = 0;
  breaker.state = "closed";
}

function markBreakerFailure(error) {
  breaker.failures += 1;
  console.warn("[OCD] TTS failure, breaker disabled for local dev:", error?.message || error);
  breaker.state = "closed";
  breaker.openedAt = 0;
}

async function googleTranslate(text, targetLanguage) {
  const tl = normalizeTranslateLanguage(targetLanguage);
  const url = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=" + encodeURIComponent(tl) + "&dt=t&q=" + encodeURIComponent(text);
  const res = await fetchWithTimeout(url, { headers: { "Accept": "application/json" } }, 15_000);
  if (!res.ok) throw new Error("Google Translate failed: HTTP " + res.status);
  const bodyText = await res.text();
  if (!bodyText.trim()) throw new Error("Google Translate returned an empty response");
  let data;
  try {
    data = JSON.parse(bodyText);
  } catch (error) {
    throw new Error("Google Translate returned non-JSON response: " + bodyText.slice(0, 120));
  }
  return (data?.[0] || []).map(x => x?.[0] || "").join("").trim();
}

function makeConnectionId() {
  return Array.from({ length: 16 }, () => Math.floor(Math.random() * 16).toString(16)).join("");
}

function edgeTimestamp() {
  return new Date().toISOString();
}

function stripBinaryHeader(buffer) {
  const marker = Buffer.from("\r\n\r\n");
  const index = buffer.indexOf(marker);
  if (index === -1) return buffer;
  return buffer.subarray(index + marker.length);
}


function runProcess(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { windowsHide: true, ...options });
    let stdout = "";
    let stderr = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { child.kill("SIGKILL"); } catch (_) {}
      reject(new Error(`${command} timed out`));
    }, options.timeoutMs || 60_000);

    child.stdout.on("data", chunk => { stdout += chunk.toString(); });
    child.stderr.on("data", chunk => { stderr += chunk.toString(); });
    child.on("error", error => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", code => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(`${command} exited with ${code}: ${stderr || stdout}`));
    });
  });
}

function normalizeEdgeRateForPython(rate) {
  const r = String(rate || "+0%").trim();
  if (/^[+-]?\d+%$/.test(r)) return r.startsWith("+") || r.startsWith("-") ? r : `+${r}`;
  return "+0%";
}

function normalizeEdgePitchForPython(pitch) {
  const p = String(pitch || "+0Hz").trim();
  if (/^[+-]?\d+Hz$/i.test(p)) return p.startsWith("+") || p.startsWith("-") ? p : `+${p}`;
  return "+0Hz";
}

async function edgeTtsPython(text, voiceName, rate, pitch) {
  const tmpFile = path.join(os.tmpdir(), `ocd-edge-${Date.now()}-${Math.random().toString(16).slice(2)}.mp3`);
  const args = [
    "-m", "edge_tts",
    "--voice", voiceName,
    "--text", String(text || "").slice(0, 4500),
    "--rate", normalizeEdgeRateForPython(rate),
    "--pitch", normalizeEdgePitchForPython(pitch),
    "--write-media", tmpFile
  ];

  const candidates = process.platform === "win32" ? ["py", "python", "python3"] : ["python3", "python"];
  const errors = [];
  for (const command of candidates) {
    try {
      await runProcess(command, args, { timeoutMs: 60_000 });
      if (!fs.existsSync(tmpFile)) throw new Error("edge-tts did not create an audio file");
      const audio = fs.readFileSync(tmpFile);
      try { fs.unlinkSync(tmpFile); } catch (_) {}
      if (!audio.length) throw new Error("edge-tts created an empty audio file");
      return audio.toString("base64");
    } catch (error) {
      errors.push(`${command}: ${String(error?.message || error).slice(0, 300)}`);
    }
  }
  try { if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile); } catch (_) {}
  throw new Error("Python edge-tts failed. Install/update it with: py -m pip install -U edge-tts. Details: " + errors.join(" | "));
}

function edgeTtsWebSocket(text, voiceName, rate, pitch) {
  return new Promise((resolve, reject) => {
    const connectionId = makeConnectionId();
    const requestId = makeConnectionId();
    const endpoint = `wss://speech.platform.bing.com/consumer/speech/synthesize/readaloud/edge/v1?TrustedClientToken=${EDGE_TRUSTED_CLIENT_TOKEN}&ConnectionId=${connectionId}`;
    const chunks = [];
    let settled = false;
    let opened = false;

    const finish = (error, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { ws.close(); } catch (_) {}
      if (error) reject(error);
      else resolve(value);
    };

    const timeout = setTimeout(() => {
      finish(new Error(opened ? "Edge TTS websocket timeout" : "Edge TTS websocket connection timeout"));
    }, 30_000);

    const ws = new WebSocket(endpoint, {
      headers: {
        "Origin": "chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0 OneClickDubLocal/1.0"
      }
    });

    ws.on("open", () => {
      opened = true;
      const speechConfig = {
        context: {
          synthesis: {
            audio: {
              metadataoptions: {
                sentenceBoundaryEnabled: false,
                wordBoundaryEnabled: false
              },
              outputFormat: EDGE_OUTPUT_FORMAT
            }
          }
        }
      };
      ws.send(`X-Timestamp:${edgeTimestamp()}\r\nContent-Type:application/json; charset=utf-8\r\nPath:speech.config\r\n\r\n${JSON.stringify(speechConfig)}`);

      const lang = voiceToLang(voiceName);
      const ssml = `
<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xmlns:mstts="https://www.w3.org/2001/mstts" xml:lang="${escapeXml(lang)}">
  <voice name="${escapeXml(voiceName)}">
    <prosody rate="${escapeXml(rate || "+0%")}" pitch="${escapeXml(pitch || "+0Hz")}" volume="+0%">${escapeXml(text)}</prosody>
  </voice>
</speak>`.trim();

      ws.send(`X-RequestId:${requestId}\r\nContent-Type:application/ssml+xml\r\nX-Timestamp:${edgeTimestamp()}\r\nPath:ssml\r\n\r\n${ssml}`);
    });

    ws.on("message", (data, isBinary) => {
      if (settled) return;
      if (isBinary || Buffer.isBuffer(data)) {
        const audio = stripBinaryHeader(Buffer.from(data));
        if (audio.length > 0) chunks.push(audio);
        return;
      }

      const message = String(data);
      if (message.includes("Path:turn.end")) {
        const audio = Buffer.concat(chunks);
        if (!audio.length) return finish(new Error("Edge TTS returned empty audio"));
        return finish(null, audio.toString("base64"));
      }
    });

    ws.on("error", (error) => {
      finish(new Error("Edge TTS websocket error: " + (error?.message || error)));
    });

    ws.on("close", (code, reason) => {
      if (settled) return;
      if (chunks.length > 0) {
        return finish(null, Buffer.concat(chunks).toString("base64"));
      }
      finish(new Error(`Edge TTS websocket closed before audio: code=${code} reason=${String(reason || "")}`));
    });
  });
}

async function edgeTts(text, voiceName, rate, pitch) {
  // Force Python edge-tts. Do not fallback to manual WebSocket because Microsoft returns 403 for that path.
  console.log("[OCD] using Python edge-tts", { voiceName, textChars: String(text || "").length });
  return await edgeTtsPython(text, voiceName, rate, pitch);
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 15_000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } catch (error) {
    if (error?.name === "AbortError") throw new Error("Request timeout");
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function normalizeTargetLanguage(code) {
  if (!code || code === "auto") return "en-US";
  const c = String(code).replace("_", "-");
  const lower = c.toLowerCase();
  if (lower === "ar") return "ar-SA";
  if (lower === "en") return "en-US";
  if (lower === "es") return "es-ES";
  if (lower === "fr") return "fr-FR";
  if (lower === "de") return "de-DE";
  if (lower === "it") return "it-IT";
  if (lower === "pt") return "pt-BR";
  if (lower === "zh") return "zh-CN";
  return c;
}

function normalizeTranslateLanguage(code) {
  const c = normalizeTargetLanguage(code).toLowerCase();
  if (c.startsWith("zh-tw")) return "zh-TW";
  if (c.startsWith("zh")) return "zh-CN";
  return c.split("-")[0] || "en";
}

function defaultEdgeVoiceFor(lang) {
  const l = normalizeTargetLanguage(lang).toLowerCase();
  if (l.startsWith("ar-eg")) return "ar-EG-ShakirNeural";
  if (l.startsWith("ar")) return "ar-SA-HamedNeural";
  if (l.startsWith("es")) return "es-ES-AlvaroNeural";
  if (l.startsWith("fr")) return "fr-FR-HenriNeural";
  if (l.startsWith("de")) return "de-DE-ConradNeural";
  if (l.startsWith("it")) return "it-IT-DiegoNeural";
  if (l.startsWith("pt")) return "pt-BR-AntonioNeural";
  if (l.startsWith("ru")) return "ru-RU-DmitryNeural";
  if (l.startsWith("ja")) return "ja-JP-KeitaNeural";
  if (l.startsWith("ko")) return "ko-KR-InJoonNeural";
  if (l.startsWith("zh")) return "zh-CN-YunxiNeural";
  if (l.startsWith("hi")) return "hi-IN-MadhurNeural";
  if (l.startsWith("tr")) return "tr-TR-AhmetNeural";
  return "en-US-RogerNeural";
}

function voiceToLang(voiceName) {
  const m = String(voiceName || "en-US-RogerNeural").match(/^([a-z]{2,3}-[A-Z]{2})-/);
  return m ? m[1] : "en-US";
}

function escapeXml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

app.listen(PORT, () => {
  console.log(`One Click Dub Fast Local Server running on http://localhost:${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
});
