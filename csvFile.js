// mapCsv.js
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const CryptoJS = require('crypto-js');

// ——— Encryption helper ———
function encrypt(text) {
  const keyString = process.env.SECRET_KEY || 'WTnxc6QyUX4awrj1wVKr3iVFxPAmO8Ft';
  const key = CryptoJS.enc.Utf8.parse(keyString);
  return CryptoJS.AES.encrypt(text, key, { mode: CryptoJS.mode.ECB }).toString();
}

// ——— Helpers ———
function localNumber(raw) {
  return "0" + raw.slice(2);  // strip "92" and prepend "0"
}

function formatIsoPlusOffset(dateStr) {
  return new Date(dateStr).toISOString().replace('Z', '+00:00');
}

// Formats as "Fri May 16 2025 12:23:14 GMT+0000 (Coordinated Universal Time)"
function formatAcceptedTime(dateStr) {
  const d = new Date(dateStr);
  const days   = ["Sun","Mon","Tue","Wed","Thu","Fri","Sat"];
  const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const day    = days[d.getUTCDay()];
  const month  = months[d.getUTCMonth()];
  const date   = String(d.getUTCDate()).padStart(2, '0');
  const year   = d.getUTCFullYear();
  const hh     = String(d.getUTCHours()).padStart(2, '0');
  const mm     = String(d.getUTCMinutes()).padStart(2, '0');
  const ss     = String(d.getUTCSeconds()).padStart(2, '0');
  return `${day} ${month} ${date} ${year} ${hh}:${mm}:${ss} GMT+0000 (Coordinated Universal Time)`;
}

// ——— Output keys ———
const keys = [
  "message_id",
  "accepted_time",
  "created_at",
  "destination_addr",
  "ip_address",
  "msg_id",
  "protocol",
  "channel",
  "api_version",
  "encoding",
  "short_message",
  "source_addr",
  "uid",
  "handset_time",
  "network_time",
  "operator_id",
  "hlr_id",
  "routed_cid",
  "original_status",
  "status"
];

// ——— CSV→field mapping ———
const mapping = {
  uid:               "uid",
  source_addr:       "source_addr",
  destination_addr:  "destination_addr",
  createdAt:         "created_at",
  message_id:        "message_id",
  protocol:          "protocol",
  ip_address:        "ip_address"
};

// ——— Operator prefix maps ———
const hlrMap = { "030": 1, "033": 2, "031": 3, "034": 4 };
const routedMap = {
  "030": "mobilink-ubl",
  "033": "ufone-ubl",
  "031": "zong-ubl",
  "034": "telenor-8657"
};

const results = [];
fs.createReadStream(path.resolve(__dirname, "data.csv"))
  .pipe(csv())
  .on("data", row => {
    // init all keys to null
    const mapped = keys.reduce((o, k) => { o[k] = null; return o; }, {});

    // map CSV columns (except short_message)
    for (let [csvKey, outKey] of Object.entries(mapping)) {
      if (csvKey === "createdAt" && row[csvKey]) {
        mapped[outKey] = formatIsoPlusOffset(row[csvKey]);
      } else if (row[csvKey] !== undefined) {
        mapped[outKey] = row[csvKey];
      }
    }

    // set accepted_time in "Fri May 16 2025 12:23:14 GMT+0000 (Coordinated Universal Time)" format
    if (row.createdAt) {
      mapped.accepted_time = formatAcceptedTime(row.createdAt);
    }

    // encrypt short_message
    if (typeof row.short_message === "string") {
      mapped.short_message = encrypt(row.short_message);
    }

    // force constant fields
    mapped.status      = "FAILED";
    mapped.channel     = "Default";
    mapped.api_version = "V2";
    mapped.encoding    = "7bit";

    // compute hlr_id & routed_cid
    const local = localNumber(row.destination_addr);
    const prefix = local.slice(0, 3);
    mapped.hlr_id     = hlrMap[prefix]    || null;
    mapped.routed_cid = routedMap[prefix] || null;

    results.push(mapped);
  })
  .on("end", () => {
    fs.writeFileSync(
      path.resolve(__dirname, "output.json"),
      JSON.stringify(results, null, 2),
      "utf-8"
    );
    console.log("✅ Done – output.json written with formatted accepted_time.");
  })
  .on("error", err => {
    console.error("❌ CSV read error:", err);
  });
