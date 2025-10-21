#!/usr/bin/env node
'use strict';

const amqplib = require('amqplib');
const express = require('express');
const fs = require('fs');
const path = require('path');
const { once } = require('events');

// ========== CONFIG ==========
const RMQ_URL       = process.env.RMQ_URL       || 'amqp://amarwk:Amar@123-@192.168.10.39/%2F';
const QUEUE         = process.env.RMQ_QUEUE     || 'submit.sm.Telco_2';
const PREFETCH      = Number(process.env.PREFETCH || 100);
const ADMIN_PORT    = Number(process.env.ADMIN_PORT || 8089);
const ADMIN_TOKEN   = process.env.ADMIN_TOKEN   || 'SUPER-SECRET';

let STALE_UNACK_SEC = Number(process.env.STALE_UNACK_SEC || 10);
const ONLY_REDELIVERED = true;
const REQUEUE_DELAY_MS = 0;

// — Auto-exit watcher tuning —
const POLL_INTERVAL_MS    = Number(process.env.POLL_INTERVAL_MS || 1000); // how often to check
const QUIET_AFTER_ZERO_MS = Number(process.env.QUIET_AFTER_ZERO_MS || 3000); // stability window before exit

// ========== FILE CONFIG ==========
const OUTPUT_DIR  = process.env.OUTPUT_DIR  || path.resolve(__dirname, 'out');
const OUTPUT_FILE = process.env.OUTPUT_FILE || 'unacked_minimal.jsonl';
fs.mkdirSync(OUTPUT_DIR, { recursive: true });
const OUT_PATH = path.join(OUTPUT_DIR, OUTPUT_FILE);
const outStream = fs.createWriteStream(OUT_PATH, { flags: 'a' });

async function writeJsonl(obj) {
  const line = JSON.stringify(obj) + '\n';
  if (!outStream.write(line)) await once(outStream, 'drain');
}

// ========== STATE ==========
let MAINTENANCE_MODE = false;
let conn, ch, consumerTag;

const unackedByQueue = new Map();
const ensureQueueMap = (q) => (
  unackedByQueue.has(q)
    ? unackedByQueue.get(q)
    : (unackedByQueue.set(q, new Map()), unackedByQueue.get(q))
);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// watcher state
let zeroSince = null;
let pollBusy = false;
let pollTimer = null;

// ========== CONSUMER ==========
async function onMessage(msg) {
  if (!msg) return;
  const tag = msg.fields.deliveryTag;
  const map = ensureQueueMap(QUEUE);
  map.set(tag, { msg, ts: Date.now() });

  if (MAINTENANCE_MODE) {
    try { ch.nack(msg, false, true); } catch {}
    map.delete(tag);
    return;
  }

  if (ONLY_REDELIVERED && !msg.fields.redelivered) {
    if (REQUEUE_DELAY_MS > 0) await sleep(REQUEUE_DELAY_MS);
    try { ch.nack(msg, false, true); } catch {}
    map.delete(tag);
    return;
  }

  const rec = map.get(tag);
  const ageMs = Date.now() - rec.ts;
  const needMs = Math.max(0, STALE_UNACK_SEC * 1000 - ageMs);
  if (needMs > 0) await sleep(needMs);

  if (MAINTENANCE_MODE) {
    try { ch.nack(msg, false, true); } catch {}
    map.delete(tag);
    return;
  }

  try {
    // === BUSINESS LOGIC: WRITE MINIMAL INFO TO FILE ===
    const record = {
      ts_iso: new Date().toISOString(),
      queue: QUEUE,
      properties: {
        messageId: msg.properties?.messageId || null
      }
    };

    await writeJsonl(record);
    ch.ack(msg);
  } catch (err) {
    console.error('[error] writing file failed; requeue:', err.message);
    try { ch.nack(msg, false, true); } catch {}
  } finally {
    map.delete(tag);
  }
}

async function startConsumer() {
  conn = await amqplib.connect(RMQ_URL);
  conn.on('close', () => console.error('[amqp] connection closed'));
  conn.on('error', err => console.error('[amqp] connection error:', err.message));

  ch = await conn.createChannel();
  await ch.assertQueue(QUEUE, { durable: true });
  await ch.prefetch(PREFETCH);

  const res = await ch.consume(QUEUE, onMessage, { noAck: false });
  consumerTag = res.consumerTag;
  console.log(`[consumer] consuming ${QUEUE}, stale=${STALE_UNACK_SEC}s`);

  startZeroWatcher(); // <-- start the auto-exit watcher
}

// ========== ZERO-UNACK WATCHER (auto-exit) ==========
function startZeroWatcher() {
  if (pollTimer) return;
  pollTimer = setInterval(async () => {
    if (pollBusy || !ch) return;
    pollBusy = true;
    try {
      const m = unackedByQueue.get(QUEUE);
      const unackedCount = m ? m.size : 0;

      // also check broker READY count to ensure queue really drained
      let messageCount = -1;
      try {
        const qinfo = await ch.checkQueue(QUEUE);
        messageCount = qinfo.messageCount;
      } catch (e) {
        // checkQueue can fail if channel is closing; ignore here
      }

      const allClear = unackedCount === 0 && messageCount === 0;
      if (allClear) {
        if (!zeroSince) zeroSince = Date.now();
        const elapsed = Date.now() - zeroSince;
        if (elapsed >= QUIET_AFTER_ZERO_MS) {
          console.log(`[watcher] all clear for ${elapsed}ms (unacked=0, ready=0). Exiting...`);
          clearInterval(pollTimer);
          pollTimer = null;
          onExit(); // graceful shutdown
        }
      } else {
        zeroSince = null; // reset stability window
      }
    } finally {
      pollBusy = false;
    }
  }, POLL_INTERVAL_MS);
}

// ========== ADMIN HTTP ==========
function startAdmin() {
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => {
    if ((req.headers.authorization || '') !== `Bearer ${ADMIN_TOKEN}`)
      return res.status(401).json({ error: 'unauthorized' });
    next();
  });

  app.get('/admin/info', async (_req, res) => {
    const m = unackedByQueue.get(QUEUE);
    let messageCount = null;
    try { messageCount = (await ch.checkQueue(QUEUE)).messageCount; } catch {}
    res.json({
      queue: QUEUE,
      unacked_count: m ? m.size : 0,
      ready_count: messageCount,
      stale_unack_sec: STALE_UNACK_SEC,
      maintenance: MAINTENANCE_MODE,
      output_path: OUT_PATH,
      zero_since_ms: zeroSince,
      poll_interval_ms: POLL_INTERVAL_MS,
      quiet_after_zero_ms: QUIET_AFTER_ZERO_MS
    });
  });

  app.post('/admin/config/stale', (req, res) => {
    const val = Number(req.body?.stale_unack_sec);
    if (!Number.isFinite(val) || val < 0)
      return res.status(400).json({ error: 'invalid stale_unack_sec' });
    STALE_UNACK_SEC = val;
    res.json({ ok: true, stale_unack_sec: STALE_UNACK_SEC });
  });

  app.post('/admin/pause', (_req, res) => {
    MAINTENANCE_MODE = true;
    res.json({ ok: true, maintenance: MAINTENANCE_MODE });
  });
  app.post('/admin/resume', (_req, res) => {
    MAINTENANCE_MODE = false;
    res.json({ ok: true, maintenance: MAINTENANCE_MODE });
  });

  app.listen(ADMIN_PORT, () =>
    console.log(`[admin] listening on :${ADMIN_PORT} (Bearer ${ADMIN_TOKEN})`)
  );
}

// ========== SHUTDOWN ==========
function onExit(err) {
  if (err) console.error('[fatal]', err);
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  Promise.resolve()
    .then(() => { try { outStream.end(); } catch {} })
    .then(() => ch && ch.close().catch(() => {}))
    .then(() => conn && conn.close().catch(() => {}))
    .finally(() => process.exit(err ? 1 : 0));
}
process.on('SIGINT', () => onExit());
process.on('SIGTERM', () => onExit());
process.on('uncaughtException', onExit);
process.on('unhandledRejection', onExit);

// ========== RUN ==========
(async () => {
  await startConsumer();
  startAdmin();
})().catch(onExit);
