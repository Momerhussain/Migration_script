#!/usr/bin/env node
/**
 * Multi-day CSV exporter (one file per day).
 * Filters: created_at (per-day window) + routed_cid (string)
 * Decrypts + masks short_message (AES-256-ECB via Node crypto)
 */

const fs = require('fs');
const path = require('path');
const { once } = require('events');
const { MongoClient, ReadPreference } = require('mongodb');
const csv = require('fast-csv');
const crypto = require('crypto');

// -------- CONFIG --------
const CONFIG = {
  uri: "mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin",
  db: "jasmin",
  collection: "messages",
  routed: "Mobilink_cmt",                       // single routed_cid string
  outDir: ".",                                  // output directory
  fields: [
    "_id", "message_id", "created_at", "destination_addr", "ip_address",
    "protocol", "routed_cid", "short_message", "source_addr", "uid",
    "handset_time", "network_time", "operator_id", "original_status", "status"
  ],
  batchSize: 10000,                             // tune 5k–20k
  useSecondary: true,                           // prefer reading from secondary
  fileHighWaterMark: 1 << 20,                   // 1 MB buffer
  // If you want "day" windows in a local timezone, shift by minutes here.
  // e.g., Asia/Karachi = +300. For UTC days keep 0.
  ZONE_OFFSET_MINUTES: 0
};

// Dates you requested — interpreted as UTC calendar days by default.
// If you need local (Asia/Karachi) days, set ZONE_OFFSET_MINUTES = 300 above.
const DAY_LIST = [
  "7/9/2025","7/10/2025","7/11/2025","7/12/2025",
  "7/14/2025","7/15/2025","7/16/2025","7/17/2025","7/18/2025",
  "7/21/2025","7/22/2025","7/23/2025","7/24/2025","7/25/2025",
  "7/28/2025","7/29/2025","7/30/2025","7/31/2025"
];

// AES key (32 bytes). Prefer ENV in prod.
const SECRET_KEY = process.env.SECRET_KEY || "WTnxc6QyUX4awrj1wVKr3iVFxPAmO8Ft";

// -------- HELPERS --------
function maskMessageBody(s) {
  if (s == null) return '';
  const str = String(s);
  if (str.length <= 6) return '*'.repeat(str.length);
  return str.slice(0, 3) + '*'.repeat(str.length - 6) + str.slice(-3);
}

// Fast AES-256-ECB decrypt (base64 input)
function decryptAndMaskShortMessage(encryptedB64) {
  if (!encryptedB64) return '';
  try {
    const keyBuf = Buffer.from(SECRET_KEY, 'utf8');
    if (keyBuf.length !== 32) return '[DECRYPT_FAIL:KEYLEN]';
    const decipher = crypto.createDecipheriv('aes-256-ecb', keyBuf, null);
    decipher.setAutoPadding(true);
    const out = Buffer.concat([
      decipher.update(Buffer.from(encryptedB64, 'base64')),
      decipher.final(),
    ]);
    const plaintext = out.toString('utf8');
    if (!plaintext) return '[DECRYPT_FAIL]';
    return plaintext
  } catch {
    return '[DECRYPT_FAIL]';
  }
}

const isoOrNull = v => (v instanceof Date ? v.toISOString() : v ? new Date(v).toISOString() : '');

function formatYMD(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

// Parse "M/D/YYYY" into a UTC start/end window, optionally shifted by ZONE_OFFSET_MINUTES
function dayWindowFromMDY(mdy, zoneOffsetMinutes = 0) {
  const [m, d, y] = mdy.split('/').map(s => parseInt(s, 10));
  const startUTC = new Date(Date.UTC(y, m - 1, d, 0, 0, 0, 0)); // UTC midnight
  // Shift to local day if offset provided
  const start = new Date(startUTC.getTime() - zoneOffsetMinutes * 60_000);
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);
  return { start, end, label: formatYMD(startUTC) }; // label is UTC date for file name
}

// Try to pick a good index hint if present
async function pickHintIfAvailable(coll) {
  const desired = [
    { routed_cid: 1, created_at: 1 },
    { created_at: 1, routed_cid: 1 },
  ];
  const idx = await coll.indexes(); // [{key:{...}, ...}]
  for (const want of desired) {
    if (idx.some(i => JSON.stringify(i.key) === JSON.stringify(want))) return want;
  }
  return null;
}

// Export one day -> one file
async function exportOneDay(coll, routed, start, end, fields, outFile, batchSize, hintKey) {
  const projection = {};
  for (const f of fields) projection[f] = 1;

  const query = { routed_cid: routed, created_at: { $gte: start, $lt: end } };

  const findOpts = {
    projection,
    batchSize,
    noCursorTimeout: false,
  };
  if (hintKey) findOpts.hint = hintKey;

  const outPath = path.resolve(outFile);
  const outStream = fs.createWriteStream(outPath, { flags: 'w', highWaterMark: CONFIG.fileHighWaterMark });
  const csvStream = csv.format({ headers: true });
  csvStream.pipe(outStream);

  let wrote = 0;
  const t0 = Date.now();

  try {
    const cursor = coll.find(query, findOpts); // no .sort() — index order

    for await (const doc of cursor) {
      const row = {};
      for (const f of fields) {
        let val = doc[f];
        if (f === 'short_message') {
          val = decryptAndMaskShortMessage(val);
        } else if (f === 'created_at' || f === 'handset_time' || f === 'network_time' || f === 'accepted_time') {
          val = isoOrNull(val);
        } else if (val == null) {
          val = '';
        }
        row[f] = val;
      }
      if (!csvStream.write(row)) await once(outStream, 'drain');
      wrote++;
      if (wrote % 100000 === 0) {
        const dt = ((Date.now() - t0) / 1000).toFixed(1);
        console.error(`[${path.basename(outFile)}] ${wrote.toLocaleString()} rows in ${dt}s`);
      }
    }

    csvStream.end();
    await once(outStream, 'finish');
    const dt = ((Date.now() - t0) / 1000).toFixed(1);
    console.error(`✅ ${path.basename(outFile)} — ${wrote.toLocaleString()} rows in ${dt}s`);
  } catch (e) {
    csvStream.end();
    outStream.destroy();
    throw e;
  }
}

async function main() {
  const client = new MongoClient(CONFIG.uri, {
    maxPoolSize: 40,
    waitQueueTimeoutMS: 30_000,
    socketTimeoutMS: 300_000,
    serverSelectionTimeoutMS: 30_000,
    readPreference: CONFIG.useSecondary ? ReadPreference.secondaryPreferred : ReadPreference.primary,
    compressors: ['zlib'] // keep only zlib to avoid optional snappy dep
  });

  try {
    await client.connect();
    await client.db(CONFIG.db).command({ ping: 1 });
    console.error('✅ MongoDB connection successful');

    const coll = client.db(CONFIG.db).collection(CONFIG.collection);
    let hintKey = null;
    try {
      hintKey = await pickHintIfAvailable(coll);
      if (hintKey) console.error('Using index hint:', hintKey);
      else console.error('No matching compound index found; proceeding without hint (slower).');
    } catch {
      console.error('Could not inspect indexes; proceeding without hint.');
    }

    // Process days SEQUENTIALLY to avoid hammering the cluster
    for (const mdy of DAY_LIST) {
      const { start, end, label } = dayWindowFromMDY(mdy, CONFIG.ZONE_OFFSET_MINUTES);
      const safeOp = CONFIG.routed.replace(/[^a-zA-Z0-9_-]+/g, '_');
      const outFile = path.join(CONFIG.outDir, `messages_${safeOp}_${label}.csv`);
      console.error(`\n▶ Exporting ${mdy} (${label}) → ${outFile}`);
      try {
        await exportOneDay(coll, CONFIG.routed, start, end, CONFIG.fields, outFile, CONFIG.batchSize, hintKey);
      } catch (e) {
        console.error(`❌ Failed ${mdy}:`, e?.message || e);
      }
    }

    console.error('\nAll done.');
  } catch (err) {
    console.error('❌ Fatal error:', err?.stack || err);
    process.exitCode = 1;
  } finally {
    try { await client.close(); } catch {}
  }
}

main().catch(e => {
  console.error('Unhandled:', e);
  process.exit(1);
});
