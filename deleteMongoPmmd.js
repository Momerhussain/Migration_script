// delete_pmd_by_customer.js
// Run: node delete_pmd_by_customer.js
// Install once: npm i mongodb

const { MongoClient } = require('mongodb');

// ===== Config (inline) =====
const MONGO_URI = "mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin?retryWrites=true&w=majority&readPreference=secondaryPreferred";
const DATABASE_NAME = "jasmin";
const COLLECTION_NAME = "pmd_hourly_summary";
// ===========================

async function run() {
  const client = new MongoClient(MONGO_URI, { maxPoolSize: 10 });
  await client.connect();
  const col = client.db(DATABASE_NAME).collection(COLLECTION_NAME);

  const filter = { customerId: "eocean-admin" };

  const toDelete = await col.countDocuments(filter);
  console.log(`Milay huay docs (customerId="eocean-admin"): ${toDelete}`);

  if (toDelete === 0) {
    console.log("Koi document match nahi hua. Exiting.");
    await client.close();
    return;
  }

  const res = await col.deleteMany(filter);
  console.log(`✅ Deleted: ${res.deletedCount}`);

  await client.close();
  console.log("Done.");
}

run().catch((e) => {
  console.error("❌ Delete failed:", e);
  process.exit(1);
});
