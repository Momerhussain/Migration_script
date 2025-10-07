// delete-by-csv.js

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const { MongoClient } = require('mongodb');

async function run() {
  // — CONFIGURATION —  
  const MONGO_URI    = process.env.MONGO_URI    || 'mongodb+srv://eocean:ZpUozwwszNOB6QQX@sms-gateway.qvex3.mongodb.net/jasmin?retryWrites=true&w=majority&readPreference=secondaryPreferred';
  const DB_NAME      = process.env.DB_NAME      || 'jasmin';
  const COLL_NAME    = process.env.COLL_NAME    || 'rejected_messages';
  const CSV_PATH     = process.argv[2]          || path.resolve(__dirname, 'data.csv');

  // Load message_ids from CSV
  const messageIds = [];
  await new Promise((resolve, reject) => {
    fs.createReadStream(CSV_PATH)
      .pipe(csv())
      .on('data', row => {
        // adjust the header name if different
        if (row.message_id) {
          messageIds.push(row.message_id);
        }
      })
      .on('end', resolve)
      .on('error', reject);
  });

  if (messageIds.length === 0) {
    console.log('No message_id values found in CSV. Exiting.');
    return;
  }

  // Connect to MongoDB
  const client = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect();
  const db  = client.db(DB_NAME);
  const col = db.collection(COLL_NAME);

  // Delete matching documents
  const result = await col.deleteMany({
    message_id: { $in: messageIds }
  });

  console.log(`Deleted ${result.deletedCount} documents matching message_id from CSV.`);
  await client.close();
}

run().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
