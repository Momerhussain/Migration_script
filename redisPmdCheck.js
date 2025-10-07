const fs = require("fs");
const path = require("path");
const xlsx = require("xlsx");
const { createClient } = require("redis");
const { format: csvFormat } = require("fast-csv");

// ---------- CONFIGURATION ----------
const CONFIG = {
  redis: {
    host: "192.168.10.44",   // Redis host
    port: 6379,          // Redis port
    password: "eocean31303"         // Redis password (leave "" if none)
  },
  hashName: "registry",               // Redis hash name
  batchSize: 1000,                    // Batch size for lookups
  inputFiles: [                       // List of Excel input files
    "BAF_Failed_Part_4.xlsx",
    "BAF_Failed_Part_5.xlsx",
    "BAF_Failed_Part_6.xlsx",
    "BAF_3lac.xlsx"
  ],
  outputFile: "registry_check.csv",   // Final merged CSV file
  phoneColumn: "phoneNumber",         // Column name in Excel
  sheetName: null                     // null = first sheet
};
// -----------------------------------

function readNumbers(filePath, sheetName, colName) {
  const wb = xlsx.readFile(filePath);
  const wsName = sheetName || wb.SheetNames[0];
  const ws = wb.Sheets[wsName];
  if (!ws) throw new Error(`Sheet ${wsName} not found.`);
  const rows = xlsx.utils.sheet_to_json(ws, { defval: "" });

  return rows
    .map(r => {
      const raw = r[colName];
      if (!raw) return null;
      return { input_msisdn: String(raw).trim() };
    })
    .filter(Boolean);
}

async function main() {
  // Redis connect
  const client = createClient({
    socket: {
      host: CONFIG.redis.host,
      port: CONFIG.redis.port,
    },
    password: CONFIG.redis.password || undefined,
  });

  client.on("error", (err) => console.error("Redis Error:", err));
  await client.connect();
  console.log(`Connected to Redis ${CONFIG.redis.host}:${CONFIG.redis.port}, hash=${CONFIG.hashName}`);

  // CSV writer (single file)
  const ws = fs.createWriteStream(path.resolve(CONFIG.outputFile));
  const csv = csvFormat({ headers: true });
  csv.pipe(ws);

  const parseValue = (val) => {
    if (!val) return {};
    try { return JSON.parse(val); } catch { return { _raw: val }; }
  };

  let totalProcessed = 0;

  // Process each Excel file
  for (const file of CONFIG.inputFiles) {
    const inputPath = path.resolve(file);
    if (!fs.existsSync(inputPath)) {
      console.error(`❌ File not found: ${inputPath}`);
      continue;
    }

    const numbers = readNumbers(inputPath, CONFIG.sheetName, CONFIG.phoneColumn);
    console.log(`Loaded ${numbers.length} numbers from ${file}.`);

    for (let i = 0; i < numbers.length; i += CONFIG.batchSize) {
      const batch = numbers.slice(i, i + CONFIG.batchSize);
      const results = await Promise.all(
        batch.map(n => client.hGet(CONFIG.hashName, n.input_msisdn))
      );

      for (let j = 0; j < batch.length; j++) {
        const { input_msisdn } = batch[j];
        const value = results[j];
        const found = value !== null;
        const parsed = parseValue(value);

        csv.write({
          msisdn: input_msisdn,
          found,
          operator: parsed.operator ?? "",
          lastPmdCheck: parsed.lastPmdCheck ?? "",
          block_on: parsed.block_on ?? "",
          block_on_sc: parsed.block_on_sc ?? "",
          raw_value: found ? value : ""
        });
      }

      totalProcessed += batch.length;
      process.stdout.write(`Processed so far: ${totalProcessed}\r`);
    }
  }

  csv.end();
  await new Promise((res) => ws.on("finish", res));
  await client.disconnect();

  console.log(`\n✅ Done. Final CSV saved: ${CONFIG.outputFile}`);
}

main().catch(err => {
  console.error("Error:", err);
  process.exit(1);
});
