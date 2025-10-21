// Node >= 18
// Strategy:
// 1) Try consumer-side webhook to NACK UNACKED in-flight messages (keeps queue running).
// 2) Fallback: close connection(s) that hold UNACK -> requeue, then optional purge READY.

const CONFIG = {
  PROTOCOL: "http",
  HOST: "192.168.10.39",
  PORT: 15672,
  USERNAME: "amarwk",
  PASSWORD: "Amar@123-",
  VHOST: "/",
  TARGET_QUEUE: "submit.sm.Telco_2",

  // ---- Preferred (no-interruption) path: consumer admin webhook ----
  // Implement a tiny endpoint in your consumer that nacks its own UNACK for this queue.
  // Example contract (POST):
  //   URL:   http://127.0.0.1:8089/admin/queues/:queue/unacked/clear
  //   Auth:  Authorization: Bearer <token>
  //   Body:  { older_than_sec?: number }  // optional age filter
  CONSUMER_ADMIN_URL: "",              // e.g. "http://127.0.0.1:8089"
  CONSUMER_ADMIN_TOKEN: "SUPER-SECRET",
  CLEAR_ONLY_OLDER_SEC: 0,             // 0 = clear all current UNACK, else min age
  // If multiple consumer instances, list them all; the script will iterate
  CONSUMER_ADMIN_HOSTS: [],            // e.g. ["http://10.0.0.11:8089","http://10.0.0.12:8089"]

  // ---- Fallback (with brief interruption) ----
  DRY_RUN: false,
  PURGE_AFTER_REQUEUE: true,
  WAIT_MS_AFTER_CLOSE: 2000,
  ALLOWED_USERS: [],                   // e.g. ["consumer_user"]
  MAX_READY_PURGE: 250000,
  FORCE_CLOSE_CONNECTION: false,
};

const enc = encodeURIComponent;
const BASE = `${CONFIG.PROTOCOL}://${CONFIG.HOST}:${CONFIG.PORT}/api`;
const AUTH = "Basic " + Buffer.from(`${CONFIG.USERNAME}:${CONFIG.PASSWORD}`).toString("base64");

async function http(method, path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { "Authorization": AUTH, ...(body ? { "Content-Type": "application/json" } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const t = await res.text().catch(()=>"");
    throw new Error(`${method} ${path} -> ${res.status} ${res.statusText} ${t}`);
  }
  const ct = res.headers.get("content-type") || "";
  return ct.includes("application/json") ? res.json() : res.text();
}

// API helpers
const getQueue = (vh, q) => http("GET", `/queues/${enc(vh)}/${enc(q)}`);
const getConsumers = (vh) => http("GET", `/consumers/${enc(vh)}`);
const deleteConnection = (name) => http("DELETE", `/connections/${enc(name)}`);
const purgeQueue = (vh, q) => http("DELETE", `/queues/${enc(vh)}/${enc(q)}/contents`);

// --- Consumer webhook helpers (preferred) ---
async function consumerWebhookClear(queue) {
  const hosts = CONFIG.CONSUMER_ADMIN_HOSTS.length
    ? CONFIG.CONSUMER_ADMIN_HOSTS
    : (CONFIG.CONSUMER_ADMIN_URL ? [CONFIG.CONSUMER_ADMIN_URL] : []);

  if (!hosts.length) return { tried: 0, ok: 0 };

  let ok = 0;
  for (const base of hosts) {
    const url = `${base.replace(/\/$/, "")}/admin/queues/${encodeURIComponent(queue)}/unacked/clear`;
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${CONFIG.CONSUMER_ADMIN_TOKEN}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          older_than_sec: CONFIG.CLEAR_ONLY_OLDER_SEC > 0 ? CONFIG.CLEAR_ONLY_OLDER_SEC : undefined
        })
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json().catch(() => ({}));
      console.log(`[webhook] ${url} ->`, data);
      ok++;
    } catch (e) {
      console.error(`[webhook] ${url} failed:`, e.message);
    }
  }
  return { tried: hosts.length, ok };
}

(async () => {
  console.log(`[i] Queue = ${CONFIG.TARGET_QUEUE} @ vhost "${CONFIG.VHOST}"`);
  console.log(`[i] DRY_RUN=${CONFIG.DRY_RUN} PURGE_AFTER_REQUEUE=${CONFIG.PURGE_AFTER_REQUEUE}`);

  const q0 = await getQueue(CONFIG.VHOST, CONFIG.TARGET_QUEUE);
  console.log(`[i] Current -> ready=${q0.messages_ready ?? 0}, unacked=${q0.messages_unacknowledged ?? 0}, consumers=${q0.consumers}`);

  // 1) Try consumer-side clear (no interruption)
  if (q0.messages_unacknowledged > 0 && (CONFIG.CONSUMER_ADMIN_URL || CONFIG.CONSUMER_ADMIN_HOSTS.length)) {
    console.log(`[i] Trying consumer-admin webhook to clear UNACK without interrupting consumers...`);
    const res = await consumerWebhookClear(CONFIG.TARGET_QUEUE);
    console.log(`[i] Webhook results: tried=${res.tried}, ok=${res.ok}`);
    // Recheck
    const qa = await getQueue(CONFIG.VHOST, CONFIG.TARGET_QUEUE);
    console.log(`[i] After webhook -> ready=${qa.messages_ready ?? 0}, unacked=${qa.messages_unacknowledged ?? 0}`);
    if (qa.messages_unacknowledged === 0) {
      // Optional purge of READY
      if (CONFIG.PURGE_AFTER_REQUEUE && (qa.messages_ready ?? 0) > 0) {
        if ((qa.messages_ready ?? 0) > CONFIG.MAX_READY_PURGE) {
          console.error(`[ABORT] READY=${qa.messages_ready} exceeds MAX_READY_PURGE=${CONFIG.MAX_READY_PURGE}. Not purging.`);
        } else if (!CONFIG.DRY_RUN) {
          await purgeQueue(CONFIG.VHOST, CONFIG.TARGET_QUEUE);
          console.log("[ok] Purge done (webhook path).");
        } else {
          console.log("[i] DRY_RUN: would purge queue (webhook path).");
        }
      }
      const qf = await getQueue(CONFIG.VHOST, CONFIG.TARGET_QUEUE);
      console.log(`[i] Final -> ready=${qf.messages_ready ?? 0}, unacked=${qf.messages_unacknowledged ?? 0}, consumers=${qf.consumers}`);
      return;
    }
    console.log(`[i] Webhook path did not fully clear UNACK; falling back to connection close...`);
  }

  // 2) Fallback: close only the connection(s) consuming this queue (brief interruption)
  const consumers = await getConsumers(CONFIG.VHOST);
  const forQueue = consumers.filter(c => c.queue?.name === CONFIG.TARGET_QUEUE);
  const byConn = new Map();
  for (const c of forQueue) {
    if (CONFIG.ALLOWED_USERS.length && !CONFIG.ALLOWED_USERS.includes(c.channel_details?.user)) continue;
    const connName = c.channel_details?.connection_name;
    if (!connName) continue;
    if (!byConn.has(connName)) byConn.set(connName, new Set());
    byConn.get(connName).add(c.queue?.name);
  }

  const q1 = await getQueue(CONFIG.VHOST, CONFIG.TARGET_QUEUE);
  const unacked1 = q1.messages_unacknowledged ?? 0;

  if (unacked1 > 0) {
    if (byConn.size === 0) {
      console.warn("[!] No consumer connections found (or excluded by ALLOWED_USERS).");
    } else {
      console.log(`[i] Candidate connection(s) to close:`);
      for (const [conn, queues] of byConn) {
        const others = [...queues].filter(n => n !== CONFIG.TARGET_QUEUE);
        const also = others.length ? ` (also consuming: ${others.join(", ")})` : "";
        console.log(`    - ${conn}${also}`);
        if (others.length && !CONFIG.FORCE_CLOSE_CONNECTION) {
          console.error(`[ABORT] Connection also consumes other queues. Set FORCE_CLOSE_CONNECTION=true to proceed.`);
          process.exit(3);
        }
      }

      if (CONFIG.DRY_RUN) {
        console.log("[i] DRY_RUN: would close the above connection(s).");
      } else {
        for (const conn of byConn.keys()) {
          try { await deleteConnection(conn); console.log(`    [ok] closed connection ${conn}`); }
          catch (e) { console.error(`    [err] ${conn}: ${e.message}`); }
        }
      }

      if (CONFIG.WAIT_MS_AFTER_CLOSE > 0) {
        console.log(`[i] Waiting ${CONFIG.WAIT_MS_AFTER_CLOSE}ms for UNACKED to requeue…`);
        await new Promise(r => setTimeout(r, CONFIG.WAIT_MS_AFTER_CLOSE));
      }
    }
  } else {
    console.log("[i] No UNACK to requeue on fallback path.");
  }

  // Optional purge on fallback
  const qNow = await getQueue(CONFIG.VHOST, CONFIG.TARGET_QUEUE);
  const readyNow = qNow.messages_ready ?? 0;
  const unackedNow = qNow.messages_unacknowledged ?? 0;
  console.log(`[i] After fallback -> ready=${readyNow}, unacked=${unackedNow}`);

  if (CONFIG.PURGE_AFTER_REQUEUE) {
    if (readyNow > CONFIG.MAX_READY_PURGE) {
      console.error(`[ABORT] READY=${readyNow} exceeds MAX_READY_PURGE=${CONFIG.MAX_READY_PURGE}. Not purging.`);
    } else if (CONFIG.DRY_RUN) {
      console.log("[i] DRY_RUN: would purge queue.");
    } else {
      await purgeQueue(CONFIG.VHOST, CONFIG.TARGET_QUEUE);
      console.log("[ok] Purge done (fallback path).");
    }
  } else {
    console.log("[i] Purge disabled.");
  }

  const qFinal = await getQueue(CONFIG.VHOST, CONFIG.TARGET_QUEUE);
  console.log(`[i] Final -> ready=${qFinal.messages_ready ?? 0}, unacked=${qFinal.messages_unacknowledged ?? 0}, consumers=${qFinal.consumers}`);
})().catch(e => { console.error("[fatal]", e.message); process.exit(2); });




// test-consume.js  (Node 18+, npm i amqplib)



// import amqplib from 'amqplib';

// const url = 'amqp://amarwk:Amar@123-@192.168.10.39/%2F';
// const queue = 'submit.sm.Telco_2';

// const run = async () => {
//   const conn = await amqplib.connect(url);
//   const ch = await conn.createChannel();
//   await ch.assertQueue(queue, { durable: true }); // no harm; uses existing
//   await ch.prefetch(50);                          // reasonable prefetch
//   console.log('Consuming...');
//   await ch.consume(queue, msg => {
//     if (!msg) return;
//     // process...
//     ch.ack(msg);
//   }, { noAck: false });
// };
// run().catch(console.error);
