// extract-and-delete.js
const Redis       = require('ioredis');
const csvWriter   = require('csv-write-stream');
const fs          = require('fs');

// ─── CONFIG ───────────────────────────────────────────────────────────────────

const MATCH_PATTERN     = '9234*';
const SCAN_COUNT        = 10_000;
const OUTPUT_FILE       = 'matches.csv';
const HASH_NAME         = 'registry';

const SENTINEL_HOST     = '192.168.10.140';
const SENTINEL_PORT     = 26379;
const REDIS_MASTER_NAME = 'mymaster';
const REDIS_PASSWORD    = 'eocean31303';

// ─── CSV SETUP ─────────────────────────────────────────────────────────────────

const fileExists = fs.existsSync(OUTPUT_FILE);
const writer     = csvWriter({
   headers:[ 'msisdn', 'operator', 'block_on', 'block_on_sc' ],
  sendHeaders: !fileExists
});
const out = fs.createWriteStream(OUTPUT_FILE, { flags: 'a' });
writer.pipe(out);

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function run() {
  // 1) Create your Sentinel‐aware client
  const client = new Redis({
    sentinels: [{ host: SENTINEL_HOST, port: SENTINEL_PORT }],
    name:      REDIS_MASTER_NAME,
    password:  REDIS_PASSWORD,
  });
  client.on('error', err => console.error('Redis error:', err));

  // 2) SCAN + PIPELINE + CSV‐STREAM
  let cursor = '0';
  do {
    // use client.hscan (lowercase) in ioredis
    const [ nextCursor, entries ] = await client.hscan(
      HASH_NAME,
      cursor,
      'MATCH', MATCH_PATTERN,
      'COUNT', SCAN_COUNT
    );
    cursor = nextCursor;

    // build a pipeline of deletes
    const pipeline = client.pipeline();
    for (let i = 0; i < entries.length; i += 2) {
      const field = entries[i], json = entries[i+1];
      let obj;
      try { obj = JSON.parse(json);console.log(obj,'obj************') }
      catch { console.warn(`Skipping bad JSON in ${field}`); continue; }

      // write one row
    //   writer.write({
    //     MSISDN:    obj.msisdn   || field,
    //     Operator:  obj.operator || '',
    //     BlockOn:   obj.block_on || '',
    //     BlockOnSC: obj.block_on_sc || ''
    //   });

    writer.write(obj);

      // queue the delete
    //   pipeline.hdel(HASH_NAME, field);
    }

    // exec all deletes in one round‑trip
    await pipeline.exec();
  } while (cursor !== '0');

  // 3) cleanup
  writer.end();
  await client.quit();
  console.log('✅ Done.');
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
