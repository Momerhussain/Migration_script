#!/usr/bin/env node
/**
 * Aggregate failed message statuses from jasmin.messages into jasmin.messages_failed_summary
 * Groups by uid, routed_cid, source_addr, status, and date (day in Asia/Karachi)
 *
 * Env:
 *   MONGODB_URI=mongodb://user:pass@host:27017
 *   MONGODB_DB=jasmin           (defaults to 'jasmin')
 *
 * Usage:
 *   node aggregate_failed_status.js
 *   node aggregate_failed_status.js --from 2025-05-01 --to 2025-05-16
 *
 * Output docs (messages_failed_summary):
 *   { uid, source_addr, routed_cid, status, sms_count, date }
 */

const { MongoClient } = require("mongodb");

// ------- config -------
const FAIL_STATUSES = ['UNDELIV','ESME_RSUBMITFAIL','FAILED','REJECTD'];

// ------- tiny args parser -------
function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--from' && argv[i+1]) { args.from = argv[++i]; }
    else if (a === '--to' && argv[i+1]) { args.to = argv[++i]; }
  }
  return args;
}

// ISO date-only -> start/end Date (inclusive/exclusive)
function parseDateBounds({ from, to }) {
    console.log(from,to);
  let gte, lt;
  if (from) {
    // start of day in Asia/Karachi is handled by $dateTrunc; here we just set UTC boundaries
    gte = new Date(from + 'T00:00:00.000Z');
  }
  if (to) {
    // make 'to' inclusive by advancing one day and using $lt
    const d = new Date(to + 'T00:00:00.000Z');
    lt = new Date(d.getTime() + 24*60*60*1000);
  }
  return { gte, lt };
}

async function ensureIndexes(messages, summary) {
  // For match speed
  await messages.createIndex({ status: 1, created_at: 1 });
  await messages.createIndex({ uid: 1, routed_cid: 1, source_addr: 1 });

  // Ensure uniqueness per day & keys in summary
  await summary.createIndex(
    { uid: 1, source_addr: 1, routed_cid: 1, status: 1, date: 1 },
    { unique: true, name: "uniq_uid_src_route_status_date" }
  );
}

(async () => {
  const args = parseArgs(process.argv);
  const { gte, lt } = parseDateBounds(args);

  const uri = process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017';
  const dbName = process.env.MONGODB_DB || 'jasmin';

  const client = new MongoClient(uri, { maxPoolSize: 10 });
  try {
    await client.connect();
    const db = client.db(dbName);
    const messages = db.collection('messages');
    const summary  = db.collection('messages_failed_summary');

    await ensureIndexes(messages, summary);

    const match = { status: { $in: FAIL_STATUSES } };
    // optional date constraint using created_at if provided
    if (gte || lt) {
      match.created_at = {};
      if (gte) match.created_at.$gte = gte;
      if (lt)  match.created_at.$lt  = lt;
    }

    const pipeline = [
        { $match: { status: { $in: FAIL_STATUSES } } },
      
        // 1) Convert created_at to real Date (null if bad)
        {
          $addFields: {
            _createdAtDate: {
              $cond: [
                { $eq: [{ $type: "$created_at" }, "date"] },
                "$created_at",
                { $convert: { input: "$created_at", to: "date", onError: null, onNull: null } }
              ]
            }
          }
        },
      
        // 2) Keep only docs with a valid created_at
        { $match: { _createdAtDate: { $ne: null } } },
      
        // 3) If --from/--to were provided, apply them to created_at (UTC day bounds)
        ...(gte || lt
          ? [{
              $match: {
                _createdAtDate: {
                  ...(gte ? { $gte: gte } : {}),
                  ...(lt  ? { $lt:  lt } : {})
                }
              }
            }]
          : []),
      
        // 4) Bucket by the SAME calendar day as created_at (UTC).
        //    (Omit timezone => UTC; or use timezone: "UTC")
        {
          $addFields: {
            date: {
              $dateTrunc: {
                date: "$_createdAtDate",
                unit: "day"
                // timezone: "UTC" // optional; default is UTC
              }
            }
          }
        },
      
        // 5) Group and count
        {
          $group: {
            _id: {
              uid: "$uid",
              source_addr: "$source_addr",
              routed_cid: "$routed_cid",
              status: "$status",
              date: "$date"
            },
            sms_count: { $sum: 1 }
          }
        },
      
        // 6) Shape for upsert
        {
          $project: {
            _id: 0,
            uid: "$_id.uid",
            source_addr: "$_id.source_addr",
            routed_cid: "$_id.routed_cid",
            status: "$_id.status",
            date: "$_id.date",
            sms_count: 1
          }
        }
      ];

    const cursor = messages.aggregate(pipeline, { allowDiskUse: true });
    const ops = [];
    let seen = 0, upserts = 0;

    while (await cursor.hasNext()) {
      const doc = await cursor.next();
      seen++;

      ops.push({
        updateOne: {
          filter: {
            uid: doc.uid,
            source_addr: doc.source_addr,
            routed_cid: doc.routed_cid,
            status: doc.status,
            date: doc.date
          },
          update: {
            $set: {
              uid: doc.uid,
              source_addr: doc.source_addr,
              routed_cid: doc.routed_cid,
              status: doc.status,
              date: doc.date,
              sms_count: doc.sms_count,
              updatedAt: new Date()
            }
          },
          upsert: true
        }
      });

      if (ops.length >= 1000) {
        const res = await summary.bulkWrite(ops, { ordered: false });
        upserts += (res.upsertedCount || 0);
        ops.length = 0;
      }
    }

    if (ops.length) {
      const res = await summary.bulkWrite(ops, { ordered: false });
      upserts += (res.upsertedCount || 0);
    }

    console.log(`[OK] Aggregated ${seen} group(s). Upserts: ${upserts}.`);
    if (gte || lt) {
      console.log(`Range: ${gte ? gte.toISOString().slice(0,10) : '…'} → ${lt ? new Date(lt - 1).toISOString().slice(0,10) : '…'} (Asia/Karachi day buckets)`);
    } else {
      console.log('Range: all-time (Asia/Karachi day buckets)');
    }
  } catch (err) {
    console.error('[ERROR]', err?.message, err);
    process.exitCode = 1;
  } finally {
    await client.close().catch(()=>{});
  }
})();
