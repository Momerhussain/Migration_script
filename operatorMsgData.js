#!/usr/bin/env node
/**
 * Export filtered MongoDB messages to CSV.
 * Filters: created_at date range + routed_cid.
 * Decrypts + masks short_message before writing.
 *
 * Run: node export_messages_csv.js
 */

const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream');
const { MongoClient, ReadPreference } = require('mongodb');
const csv = require('fast-csv');
const CryptoJS = require('crypto-js');

// ---------- CONFIGURATION ----------
const CONFIG = {
  uri: "mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin", // <-- your Mongo URL
  db: "jasmin",                                  // <-- database name
  collection: "messages",                          // <-- collection
  start: "2025-07-08T00:00:00Z",
  end:   "2025-08-01T00:00:00Z",                 // <-- end datetime (exclusive)
  routed: "telenor81190",                          // <-- routed_cid filter (array)
  out: "messages_export_telenor81190.csv",                      // <-- output file name
  fields: [
    "_id", "message_id", "created_at", "destination_addr", "ip_address",
    "protocol", "routed_cid", "short_message", "source_addr", "uid",
    "handset_time", "network_time", "operator_id", "original_status", "status"
  ],
  secondary: true,  // use secondaryPreferred
  batchSize: 1000
};

// AES key for decryption
const SECRET_KEY =  "WTnxc6QyUX4awrj1wVKr3iVFxPAmO8Ft";

// ---------- HELPERS ----------
const isoOrNull = (v) => (v instanceof Date ? v.toISOString() : v ? new Date(v).toISOString() : null);

// function maskMessageBody(msg) {
//   if (msg == null) return '';
//   const s = String(msg);
//   if (s.length <= 6) return '*'.repeat(s.length);
//   return s.slice(0, 3) + '*'.repeat(s.length - 6) + s.slice(-3);
// }

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
  } catch (err) {
    return '[DECRYPT_FAIL]';
  }
}

// ---------- MAIN ----------
(async () => {
    const startDate = new Date(CONFIG.start);
    const endDate = new Date(CONFIG.end);
  
    // Use equality (not $in) because routed_cid is a single string
    const query = {
    created_at: { $gte: startDate, $lt: endDate },
      routed_cid: CONFIG.routed,
    };
  
    const projection = {};
    for (const f of CONFIG.fields) projection[f] = 1;
  
    const client = new MongoClient(CONFIG.uri, {
      maxPoolSize: 20,
      socketTimeoutMS: 180_000,
      serverSelectionTimeoutMS: 30_000,
      readPreference: CONFIG.secondary ? ReadPreference.secondaryPreferred : ReadPreference.primary,
    });
  
    const outPath = path.resolve(CONFIG.out);
    const outStream = fs.createWriteStream(outPath, { flags: 'w' });
    const csvStream = csv.format({ headers: true });
  
    let wrote = 0;
  
    try {
      await client.connect();

          // ✅ Ping test to verify connection
    await client.db(CONFIG.db).command({ ping: 1 });
    console.error("✅ MongoDB connection successful");
      const db = client.db(CONFIG.db);
      const coll = db.collection(CONFIG.collection);
  
    //   console.error('Running query:', JSON.stringify(query));
      console.error('Writing to:', outPath);
  
      const cursor = coll.find(query, {
        projection,
        batchSize: CONFIG.batchSize,
        noCursorTimeout: false,
        // sort: { created_at: 1, _id: 1 },
      });
  
      const srcStream = cursor.stream();
  
      srcStream.on('data', (doc) => {
        const row = {};
        for (const f of CONFIG.fields) {
          let val = doc[f];
          if (f === 'short_message') {
            val = decryptAndMaskShortMessage(val);
          } else if (['created_at','accepted_time','handset_time','network_time'].includes(f)) {
            val = isoOrNull(val);
          } else if (val == null) {
            val = '';
          }
          row[f] = val;
        }
        csvStream.write(row);
        console.log(row,'----')
        wrote++;
        if (wrote % 50000 === 0) console.error(`Progress: ${wrote} rows...`);
      });
  
      srcStream.on('end', async () => {
        csvStream.end();
        await cursor.close();
        console.error(`Done. Wrote ${wrote} rows.`);
        await client.close();
      });
  
      srcStream.on('error', async (err) => {
        console.error('Cursor stream error:', err);
        csvStream.end();
        try { await cursor.close(); } catch {}
        await client.close();
        process.exit(1);
      });
  
      pipeline(csvStream, outStream, (err) => {
        if (err) {
          console.error('CSV pipeline error:', err);
          process.exit(1);
        }
      });
    } catch (err) {
      console.error('Fatal error:', err);
      await client.close();
      process.exit(1);
    }
  })();