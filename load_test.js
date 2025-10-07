// loadtest.js
const axios = require('axios');

// ─── CONFIGURE HERE ───────────────────────────────────────────────────────────
// Your Tyk Gateway listen path for jasmin-send3:
const TARGET_URL = 'http://localhost:8080/api/v1/send';
// The key you inserted into Redis:
const API_KEY = 'loadtestkey123';

// Total requests to send and concurrency per batch:
const TOTAL_REQUESTS = 10000;
const CONCURRENCY = 200;

// Timeout before considering a single request failed:
const REQUEST_TIMEOUT = 5000;
// ───────────────────────────────────────────────────────────────────────────────

let successCount = 0;
let statusCounts = {};

async function sendRequest(index) {
  try {
    const resp = await axios.get(TARGET_URL, {
      headers: { 'X-API-Key': API_KEY },
      timeout: REQUEST_TIMEOUT,
    });
    // Any 2xx is “success”
    if (resp.status >= 200 && resp.status < 300) {
      successCount++;
    } else {
      statusCounts[resp.status] = (statusCounts[resp.status] || 0) + 1;
    }
    console.log(`Req #${index} → ${resp.status}`);
  } catch (err) {
    if (err.response) {
      // e.g. 403 or other non-2xx from Tyk or backend
      const code = err.response.status;
      statusCounts[code] = (statusCounts[code] || 0) + 1;
      console.log(`Req #${index} → ${code}`);
    } else {
      // network error or timeout
      statusCounts['ERR'] = (statusCounts['ERR'] || 0) + 1;
      console.log(`Req #${index} → Error (${err.message})`);
    }
  }
}

async function runBatch(startIndex) {
  const batch = [];
  for (let i = startIndex; i < Math.min(startIndex + CONCURRENCY, TOTAL_REQUESTS); i++) {
    batch.push(sendRequest(i + 1));
  }
  await Promise.all(batch);
}

(async () => {
  console.log('Starting load test...');
  const start = Date.now();

  for (let i = 0; i < TOTAL_REQUESTS; i += CONCURRENCY) {
    await runBatch(i);
  }

  const duration = ((Date.now() - start) / 1000).toFixed(2);
  console.log('\n─── Test Complete ───────────────────────────────────────────────');
  console.log(`Total time: ${duration}s`);
  console.log(`Successful (2xx) responses: ${successCount}`);
  console.log('Non-2xx / Errors breakdown:');
  Object.entries(statusCounts).forEach(([code, count]) => {
    console.log(`  • ${code}: ${count}`);
  });
})();
