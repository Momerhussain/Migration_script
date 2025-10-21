#!/usr/bin/env node
/**
 * Export filtered MongoDB messages to CSV with file rotation, retry & resume-safe.
 * - Retries transient driver errors (network/monitor/pool-cleared) with backoff.
 * - Resumes exactly after the last written _id (no dupes/gaps).
 * - Rotates CSV after MAX_ROWS_PER_FILE.
 * - Decrypts short_message (AES/ECB/PKCS7) via CryptoJS.
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
  collection: "hourly_billings",

  // All of July 2025 (UTC)
  start: "2025-05-31T19:00:00.000Z",
  end: "2025-06-30T19:00:00.000Z",

  routed: undefined,
  status: undefined,
  source: undefined,
  username: 'ablwapper',

  out: "./ABLexportsJUNE/ablJuneSummary.csv",
  fields: [
    "date",
    "sms_type",
    "username",
    "channel",
    "shortcode",
    "operator",
    "packets",
    "sms_count"
  ],

  secondary: true,
  batchSize: 1000,
  MAX_ROWS_PER_FILE: 500_000
};

// ========== DATE OUTPUT ==========
const pad2 = (n) => String(n).padStart(2, "0");

// function formatDateSimple(value) {
//   if (!value) return "";
//   const d = value instanceof Date ? value : new Date(value);
//   if (isNaN(d.getTime())) return "";
//   const Y = d.getUTCFullYear();
//   const M = d.getUTCMonth() + 1;
//   const D = d.getUTCDate();
//   const h = d.getUTCHours();
//   const m = d.getUTCMinutes();
//   const s = d.getUTCSeconds();
//   return `${Y}-${pad2(M)}-${pad2(D)} ${pad2(h)}:${pad2(m)}:${pad2(s)}`;
// }


function formatDateSimple(value) {
  if (!value) return "";

  const d = value instanceof Date ? value : new Date(value);
  if (isNaN(d.getTime())) return "";

  // Shift by +5 hours for PKT (UTC+5)
  const shifted = new Date(d.getTime() + 5 * 60 * 60 * 1000);

  const Y = shifted.getUTCFullYear();
  const M = shifted.getUTCMonth() + 1;
  const D = shifted.getUTCDate();
  const h = shifted.getUTCHours();
  const m = shifted.getUTCMinutes();
  const s = shifted.getUTCSeconds();

  return `${Y}-${pad2(M)}-${pad2(D)} ${pad2(h)}:${pad2(m)}:${pad2(s)}`;
}

function computeTotalPackets(packets) {
  if (!packets || typeof packets !== 'object') return 0;

  let total = 0;
  for (const [k, v] of Object.entries(packets)) {
    const size = Number(k);      // e.g. "1","2","3","4","5","6"
    const count = Number(v) || 0;
    if (!Number.isNaN(size)) total += size * count;
  }
  return total;
}




// Ensure output directory exists
const { dir } = path.parse(CONFIG.out);
if (dir) fs.mkdirSync(dir, { recursive: true });

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
const RETRY = { base: 5000, max: 300000 };
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
  const q = { date: { $gte: startDate, $lte: endDate } };

  if (CONFIG.routed != null) q.routed_cid = CONFIG.routed;
  if (CONFIG.source != null) q.source_addr = CONFIG.source;
  if (CONFIG.username != null) q.username = CONFIG.username;

  if (CONFIG.status != null) {
    q.status = Array.isArray(CONFIG.status)
      ? { $in: CONFIG.status }
      : (CONFIG.status === 'FAILED'
        ? { $in: ["UNDELIV", "ESME_RSUBMITFAIL", "FAILED", "REJECTD"] }
        : CONFIG.status);
  }

  if (lastId) q._id = { $gt: lastId };
  return q;
}

function buildProjection() {
  const p = {};
  for (const f of CONFIG.fields) p[f] = 1;
  return p;
}

// ========== FILE ROTATION ==========
function makePartName(base, index) {
  const { dir, name, ext } = path.parse(base);
  const part = String(index).padStart(3, '0');
  return path.format({ dir: dir || '.', name: `${name}_part${part}`, ext: ext || '.csv' });
}

let partIndex = 1;
let rowsInCurrentFile = 0;
let totalRows = 0;
let outStream = null;
let csvStream = null;

async function openNewFile() {
  const outName = makePartName(CONFIG.out, partIndex);
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
  console.error(`✅ Closed part ${String(partIndex).padStart(3, '0')}`);
  partIndex += 1;
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

// ========== MAIN ==========
(async () => {
  let delayMs = RETRY.base;

  for (;;) {
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
      if (!csvStream || !outStream) await openNewFile();

      const projection = buildProjection();
      const query = buildQuery(lastProcessedId);
      console.error("Query:", JSON.stringify(query));

      const cursor = coll.find(query, {
        projection,
        batchSize: CONFIG.batchSize,
        noCursorTimeout: false,
      });

      for await (const doc of cursor) {
        if (rowsInCurrentFile >= CONFIG.MAX_ROWS_PER_FILE) {
          await closeCurrentFile();
          await openNewFile();
        }

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

        } else if (['created_at', 'accepted_time', 'handset_time','date'].includes(f)) {
            val = formatDateSimple(val);

        } else if (f === 'packets') {
            // 👉 derive from packets if present; fallback to existing field
            const fromPackets = computeTotalPackets(doc.packets);
            val = (fromPackets && Number.isFinite(fromPackets)) ? fromPackets : (val ?? 0);

        } else if (val == null) {
            val = '';
        }

        if (f === 'uid' && val === 'ablwapper') {
            val = 'eocean9080';
        }

        row[f] = val;
        }


        await writeRow(row);
        console.log(row?.date, "date");
        rowsInCurrentFile++;
        totalRows++;
        lastProcessedId = doc._id;

        if (totalRows % 50_000 === 0) {
          console.error(`Progress: ${totalRows} rows... (current part: ${rowsInCurrentFile})`);
        }
      }

      await client.close();
      console.error(`🎉 Done. Total rows written: ${totalRows}. Total parts: ${partIndex - 1}`);
      break;

    } catch (err) {
      try { await client.close(); } catch {}
      console.error(`⚠️ Error: ${err?.name || 'Unknown'}: ${err?.message || err}`);
      console.error(`↻ Retrying in ${Math.floor(delayMs / 1000)}s... (resume after _id: ${lastProcessedId || 'none'})`);
      await sleep(delayMs);
      delayMs = Math.min(delayMs * 2, RETRY.max);
      continue;
    }
  }
})();
