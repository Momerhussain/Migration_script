// import_hourly_counts_summary.js
// Goal: Insert ONLY hourly summary docs. Yesterday's date (UTC midnight).
// npm i mongodb csv-parser

const fs = require('fs');
const csv = require('csv-parser');
const { MongoClient } = require('mongodb');

// =============================
// 🔧 Configuration
// =============================
const MONGO_URI = "mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin?retryWrites=true&w=majority&readPreference=secondaryPreferred";
const DATABASE_NAME = "jasmin";
const COLLECTION_NAME = "pmd_hourly_summary";
const CSV_FILE = "registry_check.csv";
const PER_HOUR = 100000; // 1 lac/hour

// Optional: fix a custom start hour (UTC). If empty => use "yesterday 00:00:00.000Z"
// e.g. "2025-07-24T11:00:00.000Z"
const START_HOUR_ISO = "";
// =============================

function yesterdayStartUTC() {
  const now = new Date();
  return new Date(Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate() - 1, // yesterday
    0, 0, 0, 0
  ));
}

function truncateToHourUTC(d) {
  const t = new Date(d);
  t.setUTCMinutes(0, 0, 0);
  return t;
}

function getStartHour() {
  if (START_HOUR_ISO) {
    const s = new Date(START_HOUR_ISO);
    if (!isNaN(s.getTime())) return truncateToHourUTC(s);
  }
  return yesterdayStartUTC(); // default: yesterday 00:00 UTC
}

async function countCsvRows(filePath) {
  return new Promise((resolve, reject) => {
    let total = 0;
    fs.createReadStream(filePath)
      .pipe(csv({ mapHeaders: ({ header }) => header.trim() }))
      .on('data', (row) => {
        const hasData = Object.values(row).some(v => String(v ?? '').trim().length > 0);
        if (hasData) total++;
      })
      .on('end', () => resolve(total))
      .on('error', reject);
  });
}

async function run() {
  const totalRows = await countCsvRows(CSV_FILE);
  console.log(`Total CSV rows: ${totalRows}`);

  if (totalRows === 0) {
    console.log("CSV empty hai, exiting.");
    return;
  }

  const startHour = getStartHour();
  const docs = [];
  let remaining = totalRows;
  let hourIndex = 0;

  while (remaining > 0) {
    const count = Math.min(PER_HOUR, remaining);
    const hour = new Date(startHour.getTime() + hourIndex * 3600 * 1000);

    docs.push({
      customerId: "65a8567e-881a-4604-afef-a91a300ccc71",
      username: "Bank_Alfalah",
      hour,
      count
    });

    remaining -= count;
    hourIndex++;
  }

  const client = new MongoClient(MONGO_URI, { maxPoolSize: 10 });
  await client.connect();
  const col = client.db(DATABASE_NAME).collection(COLLECTION_NAME);

  const res = await col.insertMany(docs, { ordered: true });
  console.log(`✅ Inserted hourly summaries: ${res.insertedCount}`);
  console.log("First 2 docs:", docs.slice(0, 2));
  console.log("Last doc:", docs[docs.length - 1]);

  await client.close();
  console.log("Done.");
}

run().catch(err => {
  console.error("❌ Failed:", err);
  process.exit(1);
});
