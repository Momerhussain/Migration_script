// send-loop-numeric-id.js
// Node 18+ (uses global fetch). No deps. All config inline.

const CONFIG = {
  url: "http://192.168.10.39:8080/sms/v3/send",
  apiKey: "1eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6ImhhbXphXzEiLCJpYXQiOjE3NTgwOTcwMjd9.BPKR3uOTusbui45ex-wrWxkj6FOe5DC-Dc01Cv2y6WU",

  // Payload defaults
  from: "0100",
  text: "test-05",
  to: "+923347292897",

  // Load pattern
  totalRequests: 5000,       // how many to send
  concurrency: 25,          // parallelism
  delayMsBetweenStarts: 0, // stagger between starts (ms)

  // messageId controls
  messageIdDigits: 22,     // total digits (13 from timestamp + the rest random/seq)
  sendMessageIdAsNumber: false, // true = JSON number (⚠ up to 15-16 digits safe), false = string
};

// --- numeric-only unique ID generator (string of digits) ---
let lastMs = 0;
let seq = 0;
function numericId(digits = 22) {
  // 13-digit ms timestamp
  const ms = Date.now();
  if (ms === lastMs) seq = (seq + 1) % 1000;
  else { lastMs = ms; seq = 0; }

  const ts = String(ms);                 // 13 digits
  const seq3 = String(seq).padStart(3, "0"); // 3 digits for uniqueness within same ms
  const needed = Math.max(0, digits - (ts.length + seq3.length));

  // add 'needed' random digits
  const rand = Math.random().toString().slice(2); // variable-length digits
  const pad = (rand.length >= needed) ? rand.slice(0, needed)
                                      : (rand + "0".repeat(needed - rand.length));

  return ts + seq3 + pad; // all digits
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function sendOnce(payload) {
  const res = await fetch(CONFIG.url, {
    method: "POST",
    headers: {
      "x-api-key": CONFIG.apiKey,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const body = await res.text().catch(() => "");
  return { ok: res.ok, status: res.status, body };
}

async function runPool(total, concurrency, worker) {
  let inFlight = 0, started = 0, finished = 0;
  return new Promise((resolve, reject) => {
    const results = new Array(total);
    const launch = async () => {
      while (inFlight < concurrency && started < total) {
        const i = started++;
        inFlight++;
        if (CONFIG.delayMsBetweenStarts) await sleep(CONFIG.delayMsBetweenStarts);
        worker(i)
          .then(r => results[i] = r)
          .catch(e => results[i] = { ok: false, status: 0, body: e?.message || String(e) })
          .finally(() => {
            inFlight--; finished++;
            if (finished === total) resolve(results);
            else launch().catch(reject);
          });
      }
    };
    launch().catch(reject);
  });
}

// -------- main --------
(async () => {
  console.log(`Sending ${CONFIG.totalRequests} requests with concurrency=${CONFIG.concurrency}`);
  const t0 = Date.now();

  const results = await runPool(CONFIG.totalRequests, CONFIG.concurrency, async (i) => {
    const idStr = numericId(CONFIG.messageIdDigits);

    const payload = {
      from: CONFIG.from,
      text: `${CONFIG.text} ${i}`,
      to: CONFIG.to,
      messageId: CONFIG.sendMessageIdAsNumber ? Number(idStr) : idStr, // digits-only
      // channel: "Notification", // uncomment if needed
    };

    // If you set sendMessageIdAsNumber=true AND id has > 15-16 digits,
    // JS will lose precision. Keep default (string) unless your API forces number.
    const r = await sendOnce(payload);
    if (r.ok) console.log(`[OK  ] #${i + 1} id=${idStr} status=${r.status}`);
    else      console.error(`[ERR ] #${i + 1} id=${idStr} status=${r.status} body=${(r.body || "").slice(0, 180)}`);
    return r;
  });

  const ok = results.filter(r => r && r.ok).length;
  const fail = results.length - ok;
  console.log(`Done in ${((Date.now() - t0) / 1000).toFixed(1)}s → success=${ok}, failed=${fail}/${results.length}`);
})();
