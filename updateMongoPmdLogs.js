// update_pmd_customer.js
// Run: node update_pmd_customer.js
// npm i mongodb

const { MongoClient } = require('mongodb');

// ===== Config =====
const MONGO_URI = "mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin?retryWrites=true&w=majority&readPreference=secondaryPreferred";
const DATABASE_NAME = "jasmin";
const COLLECTION_NAME = "pmd_api_logs";
// ==================

async function run() {
  const client = new MongoClient(MONGO_URI, { maxPoolSize: 10 });
  await client.connect();
  const col = client.db(DATABASE_NAME).collection(COLLECTION_NAME);

  const filter = { customerId: "eocean-admin" };
  const update = {
    $set: {
      customerId: "65a8567e-881a-4604-afef-a91a300ccc71",
      username: "Bank_Alfalah",
    },
  };

  // (Optional but recommended) ensure index for speed
  // await col.createIndex({ customerId: 1 });

  const before = await col.countDocuments(filter);
  console.log(`Will update ${before} docs...`);

  const res = await col.updateMany(filter, update);
  console.log(`Matched: ${res.matchedCount}, Modified: ${res.modifiedCount}`);

  await client.close();
  console.log("✅ Done");
}

run().catch((e) => {
  console.error("❌ Update failed:", e);
  process.exit(1);
});
