const express = require("express");
const { Storage } = require("@google-cloud/storage");
const fetch = require("node-fetch");
const crypto = require("crypto");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 3000;

// GCS setup — support env var (for Railway) or local file
let storage;
if (process.env.GCS_SERVICE_ACCOUNT) {
  const credentials = JSON.parse(process.env.GCS_SERVICE_ACCOUNT);
  storage = new Storage({ credentials });
} else {
  const keyFilePath = process.env.GOOGLE_APPLICATION_CREDENTIALS || path.join(__dirname, "service-account.json");
  storage = new Storage({ keyFilename: keyFilePath });
}
const BUCKET_NAME = "cdn.twinklingtree.com";
const CDN_BASE = "https://cdn.twinklingtree.com";
const bucket = storage.bucket(BUCKET_NAME);

// Track used IDs to guarantee uniqueness within a session
const usedIds = new Set();

function generateUniqueId() {
  let id;
  do {
    id = crypto.randomInt(1000000000, 9999999999).toString();
  } while (usedIds.has(id));
  usedIds.add(id);
  return id;
}

app.use(express.json({ limit: "10mb" }));
app.use(express.static(path.join(__dirname, "public")));

// Process a single link: download + upload to GCS
async function processLink(url) {
  const uniqueId = generateUniqueId();

  // Download the file
  const response = await fetch(url, { timeout: 60000 });
  if (!response.ok) {
    throw new Error(`Failed to download: ${response.status} ${response.statusText}`);
  }
  const buffer = await response.buffer();

  // Detect content type from response or URL
  let contentType = response.headers.get("content-type") || "application/octet-stream";

  // Upload to GCS with no extension
  const file = bucket.file(uniqueId);
  await file.save(buffer, {
    metadata: {
      contentType: contentType,
      cacheControl: "public, max-age=31536000",
    },
    resumable: false,
  });

  return {
    originalUrl: url,
    newUrl: `${CDN_BASE}/${uniqueId}`,
    uniqueId,
  };
}

// API endpoint to process links
app.post("/api/process", async (req, res) => {
  const { links } = req.body;

  if (!links || !Array.isArray(links) || links.length === 0) {
    return res.status(400).json({ error: "No links provided" });
  }

  // Set up SSE for progress updates
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const results = [];
  const errors = [];
  const CONCURRENCY = 20;

  // Process in batches for controlled parallelism
  for (let i = 0; i < links.length; i += CONCURRENCY) {
    const batch = links.slice(i, i + CONCURRENCY);
    const batchResults = await Promise.allSettled(
      batch.map((url) => processLink(url.trim()))
    );

    for (let j = 0; j < batchResults.length; j++) {
      const result = batchResults[j];
      const linkIndex = i + j;
      if (result.status === "fulfilled") {
        results.push(result.value);
        res.write(`data: ${JSON.stringify({ type: "progress", index: linkIndex, total: links.length, result: result.value })}\n\n`);
      } else {
        const errResult = { originalUrl: batch[j], error: result.reason.message };
        errors.push(errResult);
        res.write(`data: ${JSON.stringify({ type: "error", index: linkIndex, total: links.length, result: errResult })}\n\n`);
      }
    }
  }

  res.write(`data: ${JSON.stringify({ type: "done", results, errors })}\n\n`);
  res.end();
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
