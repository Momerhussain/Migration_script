#!/usr/bin/env node
/**
 * Robust Mongo → CSV exporter with:
 * - Fixed-size file rotation (every 500,000 rows)
 * - network_time fallback to accepted_time
 * - Auto-retry on disconnect/timeouts (resume-safe via _id)
 * - Streaming I/O (low memory)
 */

const fs = require('fs');
const path = require('path');
const { MongoClient, ReadPreference, ObjectId } = require('mongodb');
const csv = require('fast-csv');
const CryptoJS = require('crypto-js');

// -------------------- CONFIG --------------------
// -------------------- CONFIG --------------------
const CONFIG = {
  uri: "mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin",
  db: "jasmin",
  collection: "messages",
  start: "2025-08-26T07:34:21.293Z",
  end:   "2025-09-01T00:00:00Z",
  outBase: "failed2.csv",          // base file name
  outDir: "./exports",             // ✅ new directory for all outputs
  fields: [
    "_id","message_id","created_at","destination_addr","ip_address","hlr_id","channel",
    "protocol","routed_cid","short_message","source_addr","uid","packet_count",
    "handset_time","network_time","operator_id","original_status","status"
  ],
  secondary: true,
  batchSize: 1000,
  MAX_ROWS_PER_FILE: 500_000,
  RETRY_BASE_MS: 5000,
  RETRY_MAX_MS: 300000,
};


// AES key for decrypting short_message (ECB/PKCS7)
const SECRET_KEY = "WTnxc6QyUX4awrj1wVKr3iVFxPAmO8Ft";

// -------------------- HELPERS --------------------
function isoOrNull(v) {
  if (!v) return null;
  const d = (v instanceof Date) ? v : new Date(v);
  if (isNaN(d.getTime())) return null;
  return d.toISOString();
}

function decryptAndMaskShortMessage(encryptedBase64) {
  if (!encryptedBase64) return '';
  try {
    const keyUtf8 = CryptoJS.enc.Utf8.parse(SECRET_KEY);
    const bytes = CryptoJS.AES.decrypt(encryptedBase64, keyUtf8, {
      mode: CryptoJS.mode.ECB,
      padding: CryptoJS.pad.Pkcs7,
    });
    const plaintext = bytes.toString(CryptoJS.enc.Utf8);
    return plaintext || '[DECRYPT_FAIL]';
  } catch {
    return '[DECRYPT_FAIL]';
  }
}

function makePartName(base, index) {
  // ensure output directory exists
  fs.mkdirSync(CONFIG.outDir, { recursive: true });

  const { name, ext } = path.parse(base);
  const part = String(index).padStart(3, '0');
  return path.join(CONFIG.outDir, `${name}_part${part}${ext || '.csv'}`);
}


function isTransientMongoError(err) {
  const name = err?.name || '';
  const labels = err?.errorLabels || err?.errorLabelSet || [];
  const hasLabel = (lbl) =>
    (Array.isArray(labels) && labels.includes(lbl)) ||
    (labels?.has && labels.has(lbl));

  return (
    name === 'MongoNetworkError' ||
    name === 'MongoNetworkTimeoutError' ||
    name === 'MongoServerSelectionError' ||
    name === 'PoolClearedOnNetworkError' ||
    hasLabel('RetryableWriteError') ||
    hasLabel('RetryableReadError') ||
    hasLabel('ResetPool') ||
    hasLabel('InterruptInUseConnections')
  );
}

// -------------------- STATE --------------------
let totalWritten = 0;
let rowsInCurrentFile = 0;
let partIndex = 1;
let lastProcessedId = null;

let outStream = null;
let csvStream = null;

// -------------------- FILE HANDLING --------------------
async function openNewFile() {
  const outName = makePartName(CONFIG.outBase, partIndex);
  console.error(`➡️ Opening ${outName}`);
  outStream = fs.createWriteStream(outName, { flags: 'w' });
  csvStream = csv.format({ headers: true });
  csvStream.pipe(outStream);
  rowsInCurrentFile = 0;
}

async function closeCurrentFile() {
  if (!csvStream) return;
  const finished = new Promise((resolve) => outStream.on('finish', resolve));
  csvStream.end();
  await finished;
  console.error(`✅ Closed part ${String(partIndex).padStart(3, '0')} (rows: ${rowsInCurrentFile})`);
  csvStream = null;
  outStream = null;
  partIndex += 1;
}

// -------------------- MAIN LOOP --------------------
(async () => {
  let delay = CONFIG.RETRY_BASE_MS;

  // open first file immediately
  await openNewFile();

  while (true) {
    const client = new MongoClient(CONFIG.uri, {
      maxPoolSize: 20,
      socketTimeoutMS: 180_000,
      serverSelectionTimeoutMS: 30_000,
      retryReads: true,
      readPreference: CONFIG.secondary ? ReadPreference.secondaryPreferred : ReadPreference.primary,
    });

    try {
      await client.connect();
      await client.db(CONFIG.db).command({ ping: 1 });
      console.error('✅ MongoDB connected');

      const coll = client.db(CONFIG.db).collection(CONFIG.collection);

      // Build resume-safe query
      const q = {
        created_at: { $gte: new Date(CONFIG.start), $lt: new Date(CONFIG.end) },
        status: { $in: ["UNDELIV", "ESME_RSUBMITFAIL", "FAILED", "REJECTD"] },
      };
      // if (CONFIG.routed) q.routed_cid = CONFIG.routed;
      // if (CONFIG.source) q.source_addr = CONFIG.source;
      if (lastProcessedId) q._id = { $gt: lastProcessedId };

      const projection = {};
      for (const f of CONFIG.fields) projection[f] = 1;

      const cursor = coll.find(q, {
        projection,
        batchSize: CONFIG.batchSize,
        noCursorTimeout: false,
        sort: { _id: 1 }, // deterministic order for resume
      });

      // For-await keeps things simple and backpressure-friendly
      for await (const doc of cursor) {
        // build CSV row
        const row = {};
        for (const f of CONFIG.fields) {
          let val = doc[f];

          if (f === 'short_message') {
            val = decryptAndMaskShortMessage(val);
          } else if (f === 'network_time') {
            // fallback to accepted_time if network_time empty
            val = val || doc.accepted_time || null;
            val = isoOrNull(val);
          } else if (['created_at', 'accepted_time', 'handset_time'].includes(f)) {
            val = isoOrNull(val);
          } else if (val == null) {
            val = '';
          }

          row[f] = val;
        }

        // write CSV
        const ok = csvStream.write(row);
        if (!ok) {
          await new Promise((r) => csvStream.once('drain', r));
        }

        totalWritten += 1;
        rowsInCurrentFile += 1;
        lastProcessedId = doc._id;

        // progress log
        if (totalWritten % 50_000 === 0) {
          console.error(`Progress: ${totalWritten} (current part rows: ${rowsInCurrentFile})`);
        }

        // rotate if needed
        if (rowsInCurrentFile >= CONFIG.MAX_ROWS_PER_FILE) {
          await closeCurrentFile();
          await openNewFile();
        }
      }

      // finished current range
      await client.close();

      console.error(`🎉 Completed. Total rows written: ${totalWritten}. Parts: ${partIndex - 1}`);
      // close the last open file if it has content
      if (csvStream) await closeCurrentFile();
      process.exit(0);

    } catch (err) {
      try { await client.close(); } catch {}

      // log the error
      console.error(`⚠️ Mongo error: ${err?.name || 'Error'}: ${err?.message || err}`);

      // If file streams somehow got closed, re-open for safety (rare)
      if (!csvStream || !outStream) {
        try { await openNewFile(); } catch (e) { console.error('Failed to reopen output file:', e.message); }
      }

      // Backoff + retry forever (until DB becomes reachable)
      console.error(`↻ Will retry in ${Math.floor(delay/1000)}s... (resume after _id: ${lastProcessedId || 'none'})`);
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, CONFIG.RETRY_MAX_MS);
      continue;
    }
  }
})();

// -------------------- GRACEFUL SHUTDOWN --------------------
process.on('SIGINT', async () => {
  console.error('\n🛑 Caught SIGINT, closing files...');
  try { if (csvStream) await closeCurrentFile(); } catch {}
  process.exit(0);
});
process.on('SIGTERM', async () => {
  console.error('\n🛑 Caught SIGTERM, closing files...');
  try { if (csvStream) await closeCurrentFile(); } catch {}
  process.exit(0);
});
