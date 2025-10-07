const { MongoClient } = require('mongodb');

const uri = 'mongodb+srv://KSA-UAT:gII3vDL3osTudcmL@ksa-uat-pl-1.irpns.mongodb.net/'; // Replace with your MongoDB connection string if different
const dbName = 'auth-management';
const collectionName = 'users';

async function clearDeviceTokens() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB');

    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const result = await collection.updateMany(
      {},
      { $set: { deviceTokens: [] } }
    );

    console.log(`✅ Updated ${result.modifiedCount} documents`);
  } catch (err) {
    console.error('❌ Error updating documents:', err);
  } finally {
    await client.close();
    console.log('🔌 Disconnected from MongoDB');
  }
}

clearDeviceTokens();
