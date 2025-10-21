// consumer-with-unacked-admin.js
'use strict';

const amqplib = require('amqplib');
const express = require('express');

const RMQ_URL = 'amqp://amarwk:Amar@123-@192.168.10.39/%2F';
const QUEUE = 'submit.sm.Telco_2';
const PREFETCH = 100;
const ADMIN_PORT = 8089;
const ADMIN_TOKEN = 'SUPER-SECRET';
const CLEAR_ONLY_OLDER_SEC = 0;

// OPTIONAL: process only re-deliveries (leave false if you don't want this behavior)
const ONLY_REDELIVERED = true;
const REQUEUE_DELAY_MS = 0;
let MAINTENANCE_MODE = false;

let conn;   // amqplib.Connection
let ch;     // amqplib.Channel
let consumerTag = null;   // NEW: keep the consumer tag for cancel/restart

const unackedByQueue = new Map(); // Map<string, Map<number, {msg, ts}>>
const ensureQueueMap = (q) => (unackedByQueue.has(q) ? unackedByQueue.get(q) : (unackedByQueue.set(q, new Map()), unackedByQueue.get(q)));

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// --- factored out so we can reuse after cancel/restart ---const QUEUE = 
async function onMessage(msg) {
  const tag = msg.fields.deliveryTag;
  // at the very top of the handler, right after you add it to unacked map:
if (MAINTENANCE_MODE) {
  // immediately put it back; READY ends up unchanged after we finish
  ch.nack(msg, false, true);           // requeue
  ensureQueueMap(QUEUE).delete(tag);   // not in our in-flight set anymore
  return;
}
  if (!msg) return;
  
  ensureQueueMap(QUEUE).set(tag, { msg, ts: Date.now() });

  try {
if (ONLY_REDELIVERED && !msg.fields.redelivered) {
  if (REQUEUE_DELAY_MS > 0) await sleep(REQUEUE_DELAY_MS);
  ch.nack(msg, false, true);            // requeue first-time deliveries
  ensureQueueMap(QUEUE).delete(tag);    // remove from our in-flight map
  return;                               // skip processing
}

    // ---- YOUR BUSINESS LOGIC HERE ----
    // ... process msg.content ...
    ch.ack(msg);

  } catch (e) {
    ch.nack(msg, false, true);                   // your error policy
  } finally {
    ensureQueueMap(QUEUE).delete(tag);
  }
}

async function startConsumer() {
  conn = await amqplib.connect(RMQ_URL);
  conn.on('close', () => console.error('[amqp] connection closed'));
  conn.on('error', err => console.error('[amqp] connection error:', err && err.message));

  ch = await conn.createChannel();
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.prefetch(PREFETCH);

  const res = await ch.consume(QUEUE, onMessage, { noAck: false }); // NEW: capture consumerTag
  consumerTag = res.consumerTag;
  console.log(`[consumer] consuming ${QUEUE} prefetch=${PREFETCH} tag=${consumerTag}`);
}

// Admin server
function startAdmin() {
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    const auth = req.headers.authorization || '';
    if (auth !== `Bearer ${ADMIN_TOKEN}`) return res.status(401).json({ error: 'unauthorized' });
    next();
  });

  app.get('/admin/queues/:queue/unacked/info', (req, res) => {
    const q = req.params.queue;
    const m = unackedByQueue.get(q);
    res.json({ queue: q, unacked_count: m ? m.size : 0, prefetch: PREFETCH });
  });

  // NEW: requeue ONLY current UNACK by cancelling & restarting this consumer (READY untouched)
  app.post('/admin/queues/:queue/unacked/requeue', async (req, res) => {
    const q = req.params.queue;
    if (q !== QUEUE) return res.status(400).json({ error: `this worker is for ${QUEUE}` });
    if (!consumerTag) return res.status(409).json({ error: 'consumer not started yet' });

    try {
      await ch.cancel(consumerTag);                        // requeues UNACK on this consumer
      unackedByQueue.set(QUEUE, new Map());               // clear local tracking
      await sleep(50);                                    // tiny pause
      const r = await ch.consume(QUEUE, onMessage, { noAck: false });  // resume
      consumerTag = r.consumerTag;
      return res.json({ ok: true, action: 'requeue_unacked', restarted: true, consumerTag });
    } catch (e) {
      return res.status(500).json({ ok: false, error: e.message });
    }
  });

  // Existing: drop/DLX in-flight without stopping (kept as-is)
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
        ch.nack(pending.msg, false, false); // drop/DLX
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
