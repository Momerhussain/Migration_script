#!/usr/bin/env node
/**
 * Export filtered MongoDB messages to CSV:
 * - Retries transient errors with backoff and resumes after last _id
 * - Splits output **day-wise** (based on accepted_time || created_at)
 * - Also rotates by MAX_ROWS_PER_FILE within a day (adds _partNNN)
 * - Decrypts short_message (AES/ECB/PKCS7) via CryptoJS
 *
 * Run: node export_messages_csv.js
 */

const fs = require('fs');
const path = require('path');
const { once } = require('events');
const { MongoClient, ReadPreference, ObjectId } = require('mongodb');
const csv = require('fast-csv');
const CryptoJS = require('crypto-js');

// ========== CONFIG ==========
const CONFIG = {
  uri: "mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin",
  db: "jasmin",
  collection: "messages",

  // Date range (UTC)
  start: "2025-07-01T00:00:00.000Z",
  end:   "2025-07-30T23:59:59.000Z",

  routed: undefined,
  status: undefined,
  source: undefined,
  uid: 'ablwapper',

  // Base path/name; script will append _YYYY-MM-DD[_partNNN].csv
  out: "./ABLexportsJULY_2/ablJune.csv",

  fields: [
    "uid",
    "source_addr",
    "destination_addr",
    "accepted_time",
    "network_time",   // falls back to accepted_time if missing
    "handset_time",
    "msg_id",
    "status",         // mapped: DTH / FAILED / DTN
    "packet_count"
  ],

  secondary: true,
  batchSize: 1000,
  MAX_ROWS_PER_FILE: 500_000
};

// ========== DATE OUTPUT (UTC, no T/Z) ==========
const pad2 = (n) => String(n).padStart(2, "0");

function formatDateSimple(value) {
  if (!value) return "";
  const d = value instanceof Date ? value : new Date(value);
  if (isNaN(d.getTime())) return "";

  const Y = d.getUTCFullYear();
  const M = d.getUTCMonth() + 1;
  const D = d.getUTCDate();
  const h = d.getUTCHours();
  const m = d.getUTCMinutes();
  const s = d.getUTCSeconds();

  return `${Y}-${pad2(M)}-${pad2(D)} ${pad2(h)}:${pad2(m)}:${pad2(s)}`;
}

function getDayKey(value) {
  if (!value) return "unknown";
  const d = value instanceof Date ? value : new Date(value);
  if (isNaN(d.getTime())) return "invalid";
  const Y = d.getUTCFullYear();
  const M = pad2(d.getUTCMonth() + 1);
  const D = pad2(d.getUTCDate());
  return `${Y}-${M}-${D}`; // e.g., 2025-07-15
}

// ensure output directory exists
const { dir: OUT_DIR } = path.parse(CONFIG.out);
if (OUT_DIR) fs.mkdirSync(OUT_DIR, { recursive: true });

// ========== CRYPTO ==========
const SECRET_KEY = "WTnxc6QyUX4awrj1wVKr3iVFxPAmO8Ft";
function decryptAndMaskShortMessage(encryptedBase64) {
  if (!encryptedBase64) return '';
  try {
    const keyUtf8 = CryptoJS.enc.Utf8.parse(SECRET_KEY);
    const decrypted = CryptoJS.AES.decrypt(encryptedBase64, keyUtf8, {
      mode: CryptoJS.mode.ECB,
      padding: CryptoJS.pad.Pkcs7,
    });
    const plaintext = decrypted.toString(CryptoJS.enc.Utf8);
    if (!plaintext) return '[DECRYPT_FAIL]';
    return plaintext;
  } catch {
    return '[DECRYPT_FAIL]';
  }
}

// ========== RETRY / RESUME TOOLKIT ==========
const RETRY = { base: 5_000, max: 300_000 }; // 5s → 5min
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function isTransientMongoError(err) {
  const name = err?.name || '';
  return (
    name === 'PoolClearedOnNetworkError' ||
    name === 'MongoNetworkTimeoutError' ||
    name === 'MongoServerSelectionError' ||
    name === 'MongoNetworkError' ||
    (err?.errorLabelSet && (
      err.errorLabelSet.has('ResetPool') ||
      err.errorLabelSet.has('InterruptInUseConnections') ||
      err.errorLabelSet.has('RetryableWriteError') ||
      err.errorLabelSet.has('RetryableReadError')
    ))
  );
}

// ========== QUERY / PROJECTION ==========
function buildQuery(lastId) {
  const startDate = new Date(CONFIG.start);
  const endDate = new Date(CONFIG.end);

  const q = { created_at: { $gte: startDate, $lte: endDate } };

  if (CONFIG.routed != null) q.routed_cid = CONFIG.routed;
  if (CONFIG.source != null) q.source_addr = CONFIG.source;
  if (CONFIG.uid != null) q.uid = CONFIG.uid;

  if (CONFIG.status != null) {
    q.status = Array.isArray(CONFIG.status)
      ? { $in: CONFIG.status }
      : (CONFIG.status === 'FAILED'
          ? { $in: ["UNDELIV", "ESME_RSUBMITFAIL", "FAILED", "REJECTD"] }
          : CONFIG.status);
  }

  if (lastId) q._id = { $gt: lastId }; // resume point
  return q;
}

function buildProjection() {
  const p = {};
  for (const f of CONFIG.fields) p[f] = 1;
  return p;
}

// ========== FILE ROTATION (DAY-WISE + PARTS) ==========
function makeOutName(base, dayKey, partIdx) {
  const { dir, name, ext } = path.parse(base);
  const part = partIdx > 1 ? `_part${String(partIdx).padStart(3, '0')}` : '';
  return path.format({
    dir: dir || '.',
    name: `${name}_${dayKey}${part}`,
    ext: ext || '.csv'
  });
}

let currentDay = null;
let partIndex = 1;
let rowsInCurrentFile = 0;
let totalRows = 0;
let outStream = null;
let csvStream = null;

async function openNewFile(dayKey) {
  const outName = makeOutName(CONFIG.out, dayKey, partIndex);
  const outPath = path.resolve(outName);
  console.error(`➡️ Opening ${outName}`);
  outStream = fs.createWriteStream(outPath, { flags: 'w' });
  csvStream = csv.format({ headers: true });
  csvStream.pipe(outStream);
  rowsInCurrentFile = 0;
}

async function closeCurrentFile() {
  if (!csvStream) return;
  const finished = once(outStream, 'finish');
  csvStream.end();
  await finished;
  outStream = null;
  csvStream = null;
  console.error(`✅ Closed ${currentDay} (part ${String(partIndex).padStart(3, '0')})`);
}

async function writeRow(row) {
  const ok = csvStream.write(row);
  if (!ok) await once(csvStream, 'drain');
}

// ========== STATUS + NETWORK_TIME RULES ==========
function mapStatus(s) {
  if (s === 'DELIVRD') return 'DTH';
  if (['UNDELIV', 'ESME_RSUBMITFAIL', 'FAILED', 'REJECTD'].includes(s)) return 'FAILED';
  return 'DTN';
}

// ========== STATE FOR RESUME ==========
let lastProcessedId = null;

// ========== MAIN WITH RETRY/RESUME ==========
(async () => {
  let delayMs = RETRY.base;

  for (;;) { // retry loop; break on success
    const client = new MongoClient(CONFIG.uri, {
      maxPoolSize: 20,
      retryReads: true,
      socketTimeoutMS: 180_000,
      serverSelectionTimeoutMS: 30_000,
      readPreference: CONFIG.secondary
        ? ReadPreference.secondaryPreferred
        : ReadPreference.primary,
    });

    try {
      await client.connect();
      await client.db(CONFIG.db).command({ ping: 1 });
      console.error("✅ MongoDB connection successful");

      const coll = client.db(CONFIG.db).collection(CONFIG.collection);
      const projection = buildProjection();
      const query = buildQuery(lastProcessedId);
      console.error("Query:", JSON.stringify(query));

      const cursor = coll.find(query, {
        projection,
        batchSize: CONFIG.batchSize,
        noCursorTimeout: false,
        sort: { _id: 1 } // deterministic order for resume + day split
      });

      for await (const doc of cursor) {
        // Decide split-day from accepted_time (fallback to created_at)
        const dayKey = getDayKey(doc.accepted_time || doc.created_at);

        // Open/rotate by day
        if (!csvStream || currentDay !== dayKey) {
          if (csvStream) {
            await closeCurrentFile();
            // new day -> reset part index
            partIndex = 1;
          }
          currentDay = dayKey;
          await openNewFile(currentDay);
        }

        // Rotate by max rows within the same day
        if (rowsInCurrentFile >= CONFIG.MAX_ROWS_PER_FILE) {
          await closeCurrentFile();
          partIndex += 1;
          await openNewFile(currentDay);
        }

        // ===== Build output row with rules =====
        const row = {};
        const effectiveNetworkTime = doc.network_time ?? doc.accepted_time ?? null;
        const mapped = mapStatus(doc.status);

        for (const f of CONFIG.fields) {
          let val = doc[f];

          if (f === 'short_message') {
            val = decryptAndMaskShortMessage(val);
          } else if (f === 'network_time') {
            val = formatDateSimple(effectiveNetworkTime);
          } else if (f === 'status') {
            val = mapped;
          } else if (['created_at','accepted_time','handset_time'].includes(f)) {
            val = formatDateSimple(val);
          } else if (val == null) {
            val = '';
          }

          // Replace uid value "ablwapper" → "eocean9080"
          if (f === 'uid' && val === 'ablwapper') {
            val = 'eocean9080';
          }

          row[f] = val;
        }
        // =======================================

        await writeRow(row);
        rowsInCurrentFile++;
        totalRows++;
        lastProcessedId = doc._id;

        if (totalRows % 50_000 === 0) {
          console.error(`Progress: ${totalRows} rows... (day ${currentDay}, part ${partIndex}, rows in part ${rowsInCurrentFile})`);
        }
      }

      // Clean close if we wrote anything
      if (csvStream) await closeCurrentFile();
      await client.close();
      console.error(`🎉 Done. Total rows written: ${totalRows}. Total files: ${/* parts per days not tracked globally */ 'see logs'}`);
      break; // success → exit retry loop

    } catch (err) {
      try { await client.close(); } catch {}

      // Always retry (resume-safe)
      const transient = isTransientMongoError(err);
      console.error(`⚠️ ${transient ? 'Transient ' : ''}Error: ${err?.name || 'Unknown'}: ${err?.message || err}`);
      console.error(`↻ Retrying in ${Math.floor(delayMs / 1000)}s... (resume after _id: ${lastProcessedId || 'none'})`);
      await sleep(delayMs);
      delayMs = Math.min(delayMs * 2, RETRY.max);
      continue;
    }
  }
})();
