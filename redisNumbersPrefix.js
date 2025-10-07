// extract-and-delete.js
const Redis     = require('ioredis');
const csvWriter = require('csv-write-stream');
const fs        = require('fs');

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const SCAN_COUNT        = 10000;        // Number of entries per HSCAN
const OUTPUT_FILE       = 'others.csv'; // CSV for MSISDNs not in default prefixes
const HASH_NAME         = 'registry';   // Redis hash key

const SENTINEL_HOST     = '192.168.10.106'; // Redis Sentinel host
const SENTINEL_PORT     = 26379;            // Redis Sentinel port
const REDIS_MASTER_NAME = 'mymaster';       // Redis master name in Sentinel
const REDIS_PASSWORD    = 'eocean31303';    // Redis auth password

// ─── DEFAULT PREFIXES TO EXCLUDE ────────────────────────────────────────────────
// Jazz:    030x, 032x -> international 9230x, 9232x
// Zong:    031x, 037x -> international 9231x, 9237x
// Telenor: 034x       -> international 9234x
// Ufone:   033x       -> international 9233x
// SCO:     0355x      -> international 92355x
const defaultPrefixes = [
  '9230',  // Jazz
  '9232',  // Jazz
  '9231',  // Zong
  '9237',  // Zong
  '9234',  // Telenor
  '9233',  // Ufone
  '92355'  // SCO
];

// ─── CSV SETUP ───────────────────────────────────────────────────────────────────
const writer = csvWriter({
  headers:     ['msisdn','operator','block_on','block_on_sc'],
  sendHeaders: !fs.existsSync(OUTPUT_FILE)
});
writer.pipe(fs.createWriteStream(OUTPUT_FILE, { flags: 'a' }));

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function run() {
  // 1) Create Redis client via Sentinel
  const client = new Redis({
    sentinels: [{ host: SENTINEL_HOST, port: SENTINEL_PORT }],
    name:      REDIS_MASTER_NAME,
    password:  REDIS_PASSWORD,
  });
  client.on('error', err => console.error('Redis error:', err));

  try {
    let cursor = '0';
    do {
      // 2) Scan all entries in the hash
      const [ nextCursor, entries ] = await client.hscan(
        HASH_NAME,
        cursor,
        'COUNT', SCAN_COUNT
      );
      cursor = nextCursor;

      const pipeline = client.pipeline();

      // 3) Process each entry
      for (let i = 0; i < entries.length; i += 2) {
        const field = entries[i];
        const json  = entries[i + 1];
        let obj;
        try {
          obj = JSON.parse(json);
          console.log(obj,'----');
        } catch {
          console.warn(`Skipping invalid JSON for key ${field}`);
          continue;
        }

        // Normalize MSISDN (strip leading '+' or '00')
const raw = (obj.msisdn || field).toString();
const msisdn = raw.replace(/^\+|^00/, '');

        // Only consider numbers starting with '923'
        if (!msisdn.startsWith('923')) {
          continue;
        }

        // Exclude default operator prefixes
        const isDefault = defaultPrefixes.some(prefix => msisdn.startsWith(prefix));
        if (isDefault) {
          continue;
        }

        // This is an "other" number: write it and schedule deletion
        writer.write({
          msisdn:      msisdn,
          operator:    obj.operator    || '',
          block_on:    obj.block_on    || '',
          block_on_sc: obj.block_on_sc || ''
        });
        // pipeline.hdel(HASH_NAME, field);
      }

      // 4) Execute deletions
      await pipeline.exec();
    } while (cursor !== '0');

    console.log(`✅ Extraction complete: ${OUTPUT_FILE}`);
  } catch (err) {
    console.error('Error during run:', err);
  } finally {
    writer.end();
    await client.quit();
  }
}

run().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
