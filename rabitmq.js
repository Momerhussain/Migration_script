#!/usr/bin/env node
// Small RabbitMQ queue alert app using Management HTTP API (Node 18+ global fetch)

const cron = require('node-cron'); // npm i node-cron

const RABBIT_URL   = process.env.RABBIT_URL   || 'http://192.168.253.14:15672';
const RABBIT_USER  = process.env.RABBIT_USER  || 'amarwk';
const RABBIT_PASS  = process.env.RABBIT_PASS  || 'Amar@123-';
const VHOST        = encodeURIComponent(process.env.VHOST || '/');

const THRESHOLD    = Number(process.env.THRESHOLD_PCT || 0.7); // 70%
const FALLBACK_ABS = Number(process.env.FALLBACK_ABS || 500);    // alert quickly while testing
const PAGE_SIZE    = Number(process.env.PAGE_SIZE || 10);
const WEBHOOK_URL  = process.env.WEBHOOK_URL || '';
const QUEUE_REGEX  = process.env.QUEUE_REGEX ? new RegExp(process.env.QUEUE_REGEX) : null;

// schedule: every second by default (6-field cron)
const SCHEDULE     = process.env.SCHEDULE || '* * * * * *';
const TIMEZONE     = process.env.TIMEZONE || 'Asia/Karachi';
const POLL_MS      = Number(process.env.POLL_MS || 20); // used only if SCHEDULE not set

// extra flags
const USE_READY    = process.env.USE_READY === 'true'; // use messages_ready instead of messages
const DEBUG        = process.env.DEBUG === 'true';

const AUTH = 'Basic ' + Buffer.from(`${RABBIT_USER}:${RABBIT_PASS}`).toString('base64');
const alerted = new Map(); // queueName -> { over: boolean, lastMsg: string, lastAt: number }

async function fetchJSON(url) {
  const res = await fetch(url, { headers: { Authorization: AUTH, Accept: 'application/json' } });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
  return res.json();
}

// Get all queues with counts; backfill per-queue if needed
async function getAllQueues() {
  let page = 1, pageCount = 1;
  const items = [];

  do {
    const url = `${RABBIT_URL}/api/queues/${VHOST}?` +
      `columns=name,messages,messages_ready,messages_unacknowledged,arguments,consumers` +
      `&disable_stats=true&enable_queue_totals=true&pagination=true&page_size=${PAGE_SIZE}&page=${page}`;
    const data = await fetchJSON(url);
    console.log(data,'data---');
    if (Array.isArray(data.items)) items.push(...data.items);
    pageCount = Number(data.page_count || 1);
    page++;
  } while (page <= pageCount);

  // Backfill any queue that still has no counts
  for (const it of items) {
    const hasCounts = (typeof it.messages === 'number') || (typeof it.messages_ready === 'number');
    if (!hasCounts) {
      const detailUrl = `${RABBIT_URL}/api/queues/${VHOST}/${encodeURIComponent(it.name)}?` +
                        `columns=name,messages,messages_ready,arguments&disable_stats=false`;
      try {
        const d = await fetchJSON(detailUrl);
        it.messages = d.messages ?? 0;
        it.messages_ready = d.messages_ready ?? 0;
        it.arguments = it.arguments || d.arguments || {};
      } catch {
        it.messages = it.messages ?? 0;
        it.messages_ready = it.messages_ready ?? 0;
      }
    }
  }

  return items;
}

async function notify(msg) {
  console.warn(msg);
  if (WEBHOOK_URL) {
    try {
      await fetch(WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: msg })
      });
    } catch (e) {
      console.error('Webhook error:', e.message);
    }
  }
}

async function checkAlarms() {
  try {
    const alarms = await fetchJSON(`${RABBIT_URL}/api/health/checks/alarms`);
    if (alarms.status !== 'ok') await notify(`[ALERT] Cluster alarms: ${JSON.stringify(alarms)}`);
  } catch {
    // ignore on older versions
  }
}

async function pollOnce() {
  try {
    // Reachability
    await fetchJSON(`${RABBIT_URL}/api/overview`);

    await checkAlarms();

    const queues = await getAllQueues();

    if (DEBUG) {
      for (const d of queues) {
        console.log(`[DEBUG] q=${d.name} ready=${d.messages_ready} total=${d.messages} max=${d.arguments?.['x-max-length'] ?? 'none'}`);
      }
    }

    const now = Date.now();

    for (const qq of queues) {
      if (QUEUE_REGEX && !QUEUE_REGEX.test(qq.name)) continue;

      const messages = Number((USE_READY ? qq.messages_ready : qq.messages) || 0);
      const maxLen = qq.arguments?.['x-max-length'];

      let over, detail;
      if (Number.isInteger(maxLen) && maxLen > 0) {
        const pct = messages / maxLen;
        over = pct >= THRESHOLD;
        detail = `msgs=${messages} / max=${maxLen} (${(pct * 100).toFixed(1)}%)`;
      } else {
        over = messages >= FALLBACK_ABS;
        detail = `msgs=${messages} (>= ${FALLBACK_ABS})`;
      }

      const prev = alerted.get(qq.name) || { over: false };

      if (over && !prev.over) {
        const msg = `[ALERT] queue=${qq.name} vhost=${decodeURIComponent(VHOST)} ${detail}`;
        await notify(msg);
        alerted.set(qq.name, { over: true, lastMsg: msg, lastAt: now });
      } else if (!over && prev.over) {
        const msg = `[RECOVERY] queue=${qq.name} back under threshold (${detail})`;
        await notify(msg);
        alerted.set(qq.name, { over: false, lastMsg: msg, lastAt: now });
      }
    }
  } catch (e) {
    console.error('Poll error:', e.stack || e);
  }
}

// ---- scheduler (no-overlap guard)
let running = false;
async function trigger() {
  if (running) { console.warn('Previous run still in progress — skipping'); return; }
  running = true;
  try { await pollOnce(); } finally { running = false; }
}

async function main() {
  if (SCHEDULE) {
    console.log(`RabbitMQ Queue Alert scheduled at "${SCHEDULE}" (${TIMEZONE}). Threshold=${THRESHOLD*100}%`);
    console.log(`Server=${RABBIT_URL} vhost=${decodeURIComponent(VHOST)} regex=${QUEUE_REGEX || 'ALL'}`);
    await trigger(); // immediate run
    cron.schedule(SCHEDULE, trigger, { timezone: TIMEZONE });
  } else {
    console.log(`RabbitMQ Queue Alert started. Poll every ${POLL_MS}ms. Threshold=${THRESHOLD*100}%`);
    console.log(`Server=${RABBIT_URL} vhost=${decodeURIComponent(VHOST)} regex=${QUEUE_REGEX || 'ALL'}`);
    await trigger();
    setInterval(trigger, POLL_MS);
  }
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
