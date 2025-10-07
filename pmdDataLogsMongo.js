// import_pmd_csv.js
// Run: node import_pmd_csv.js
// npm install mongodb csv-parser

const fs = require('fs');
const csv = require('csv-parser');
const { MongoClient } = require('mongodb');

// =============================
// 🔧 Configuration
// =============================
const MONGO_URI = "mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin?retryWrites=true&w=majority&readPreference=secondaryPreferred";
const DATABASE_NAME = "jasmin";
const COLLECTION_NAME = "pmd_api_logs";
const CSV_FILE = "registry_check.csv";
const BATCH_SIZE = 1000;
// =============================

function rand1to10() {
  return Math.floor(Math.random() * 10) + 1; // 1..10 (not 1..100)
}

function parseBool(v) {
  if (v === true || v === false) return v;
  if (v == null) return false;
  const s = String(v).trim().toLowerCase();
  return ["true", "1", "yes", "y", "found"].includes(s);
}

async function run() {
  const client = new MongoClient(MONGO_URI, { maxPoolSize: 5 });
  await client.connect();
  const db = client.db(DATABASE_NAME);
  const col = db.collection(COLLECTION_NAME);

  let buffer = [];
  let total = 0;

  async function flush() {
    if (buffer.length === 0) return;
    const ops = buffer.map(doc => ({ insertOne: { document: doc } }));
    // ordered:false so aik fail se sab stop na ho
    await col.bulkWrite(ops, { ordered: false });
    total += buffer.length;
    buffer = [];
    process.stdout.write(`Inserted: ${total}\r`);
  }

  // Stream ko handle karein: pause -> flush -> resume
  const readStream = fs.createReadStream(CSV_FILE);
  const parser = csv({ mapHeaders: ({ header }) => header.trim() });

  await new Promise((resolve, reject) => {
    readStream
      .pipe(parser)
      .on('data', (row) => {
        // ✅ Naya plain object per row (no reuse)
        const doc = {
          customerId: "eocean-admin",
          msisdn: String(row.msisdn || "").trim(),
          operatorId:row?.operator ? `${row.operator}` : null,   // ya null chahiye to null kar dein
          responseTime: rand1to10(),
          timestamp: new Date(),
          success: parseBool(row.found),
          username: "eocean-admin",
        };

        if (!doc.msisdn) return;

        buffer.push(doc);

        if (buffer.length >= BATCH_SIZE) {
          // ⚠️ pause before flush
          parser.pause();
          flush()
            .then(() => parser.resume())
            .catch(err => {
              parser.removeAllListeners();
              reject(err);
            });
        }
      })
      .on('end', async () => {
        try {
          await flush();
          resolve();
        } catch (e) {
          reject(e);
        }
      })
      .on('error', reject);
  });

  console.log(`\n✅ Import complete. Total inserted: ${total}`);
  await client.close();
}

run().catch(err => {
  console.error("❌ Import failed:", err);
  process.exit(1);
});
