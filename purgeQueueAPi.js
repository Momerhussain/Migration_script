// consumer-with-unacked-admin.js
// Node 18+, CommonJS. npm i amqplib express
'use strict';

const amqplib = require('amqplib');
const express = require('express');

// ---------- CONFIG ----------
const RMQ_URL = 'amqp://amarwk:Amar@123-@192.168.10.39/%2F';
const QUEUE = 'submit.sm.Telco_2';
const PREFETCH = 100;                 // limit inflight/UNACK
const ADMIN_PORT = 8089;              // bind to localhost or keep behind VPN
const ADMIN_TOKEN = 'SUPER-SECRET';   // bearer token for admin endpoint
const CLEAR_ONLY_OLDER_SEC = 0;       // e.g. 120 to drop only UNACK older than 2 minutes
// --------------------------------

let conn; // amqplib.Connection
let ch;   // amqplib.Channel

// Track current UNACKed for this queue (message references + timestamp)
const unackedByQueue = new Map(); // Map<string, Map<number, {msg, ts}>>

function ensureQueueMap(q) {
  if (!unackedByQueue.has(q)) unackedByQueue.set(q, new Map());
  return unackedByQueue.get(q);
}

async function startConsumer() {
  conn = await amqplib.connect(RMQ_URL);
  conn.on('close', () => console.error('[amqp] connection closed'));
  conn.on('error', err => console.error('[amqp] connection error:', err && err.message));

  ch = await conn.createChannel();
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.prefetch(PREFETCH);

  await ch.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;

      // mark this delivery as UNACKed/in-flight
      const tag = msg.fields.deliveryTag;
      ensureQueueMap(QUEUE).set(tag, { msg, ts: Date.now() });

      try {
        // ---- YOUR BUSINESS LOGIC HERE ----
        // e.g., parse/process msg.content, call downstream, etc.

        // when successfully processed:
        ch.ack(msg);

      } catch (e) {
        // On processing error, decide what to do:
        //  - requeue = true will put it back to READY
        //  - requeue = false will drop/DLX
        ch.nack(msg, false, true);
      } finally {
        // remove from tracking (acked or nacked)
        ensureQueueMap(QUEUE).delete(tag);
      }
    },
    { noAck: false }
  );

  console.log(`[consumer] consuming ${QUEUE} with prefetch=${PREFETCH}`);
}

// Admin server to clear UNACK without stopping the consumer
function startAdmin() {
  const app = express();
  app.use(express.json());

  // very simple auth; prefer IP allowlist or mTLS in prod
  app.use((req, res, next) => {
    const auth = req.headers.authorization || '';
    if (auth !== `Bearer ${ADMIN_TOKEN}`) return res.status(401).json({ error: 'unauthorized' });
    next();
  });

  // Health/info
  app.get('/admin/queues/:queue/unacked/info', (req, res) => {
    const q = req.params.queue;
    const m = unackedByQueue.get(q);
    res.json({ queue: q, unacked_count: m ? m.size : 0, prefetch: PREFETCH });
  });

  // Clear current UNACK (drop or DLX) on this queue only — no connection/channel close
  app.post('/admin/queues/:queue/unacked/clear', async (req, res) => {
    const q = req.params.queue;
    if (q !== QUEUE) return res.status(400).json({ error: `this worker is for ${QUEUE}` });

    const m = unackedByQueue.get(q);
    if (!m || m.size === 0) return res.json({ queue: q, cleared: 0 });

    const minAge = Number(req.body && req.body.older_than_sec != null
      ? req.body.older_than_sec
      : CLEAR_ONLY_OLDER_SEC);

    const now = Date.now();
    let cleared = 0;

    for (const [tag, pending] of m) {
      const ageSec = (now - pending.ts) / 1000;
      if (minAge > 0 && ageSec < minAge) continue;

      try {
        // Drop it: requeue=false -> delete or DLX
        ch.nack(pending.msg, false, false);
        m.delete(tag);
        cleared++;
      } catch (e) {
        console.error(`[admin] nack failed tag=${tag}:`, e && e.message);
      }
    }

    res.json({ queue: q, cleared, filter_age_sec: minAge });
  });

  app.listen(ADMIN_PORT, () =>
    console.log(`[admin] listening on :${ADMIN_PORT} (Bearer ${ADMIN_TOKEN})`)
  );
}

// graceful shutdown
function onExit(err) {
  if (err) console.error('[fatal]', err);
  Promise.resolve()
    .then(() => ch && ch.close().catch(() => {}))
    .then(() => conn && conn.close().catch(() => {}))
    .finally(() => process.exit(err ? 1 : 0));
}
process.on('SIGINT', () => onExit());
process.on('SIGTERM', () => onExit());
process.on('uncaughtException', onExit);
process.on('unhandledRejection', onExit);

// run
(async () => {
  await startConsumer();
  startAdmin();
})().catch(onExit);
