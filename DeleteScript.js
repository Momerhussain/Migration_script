const mongoose = require('mongoose');

const userSchema = new mongoose.Schema({
email: { type: String, required: true, unique: true },
}, { collection: 'users' });

const roleSchema = new mongoose.Schema({
name: { type: String, required: true, unique: true },
}, { collection: 'roles' });

const historySchema = new mongoose.Schema({
emailAddress: { type: String, required: true },
}, { collection: 'history' });

const User = mongoose.model('User', userSchema);
const Role = mongoose.model('Role', roleSchema);

// Separate connection for history DB
const historyConnection = mongoose.createConnection(
'mongodb+srv://Eocean:LQbUasSRBBxe3zbI@uat-eocean.xukr8.mongodb.net/history-management?retryWrites=true&w=majority&appName=UAT-Eocean'
);

const History = historyConnection.model('History', historySchema);

async function deleteUsersRolesAndHistory() {
const authUri = 'mongodb+srv://Eocean:LQbUasSRBBxe3zbI@uat-eocean.xukr8.mongodb.net/auth-management?retryWrites=true&w=majority&appName=UAT-Eocean';

try {
await mongoose.connect(authUri);
await historyConnection.asPromise();
console.log('Connected to both MongoDB databases');

const userEmailsToDelete = [
'auto-login-user@yopmail.com',
'auto-user001@yopmail.com',
'locked-account01@yopmail.com',
'auto-update1@yopmail.com',
'auto-invite-user001@yopmail.com',
'auto-revoked-user01@yopmail.com',
];

const uniqueUserEmails = [...new Set(userEmailsToDelete)];

// Delete users
const deleteUsersResult = await User.deleteMany({
emailAddress: { $in: uniqueUserEmails }
});
console.log(`Deleted ${deleteUsersResult.deletedCount} user(s) from auth-management.`);

// Delete roles
const rolesToDelete = ['Auto role-02', 'Auto role-01','Delete-role'];
const uniqueRoles = [...new Set(rolesToDelete)];

const deleteRolesResult = await Role.deleteMany({
name: { $in: uniqueRoles }
});
console.log(`Deleted ${deleteRolesResult.deletedCount} role(s) from auth-management.`);

// Delete history records
const deleteHistoryResult = await History.deleteMany({
emailAddress: { $in: uniqueUserEmails }
});
console.log(`Deleted ${deleteHistoryResult.deletedCount} history record(s) from history-management.`);

} catch (error) {
console.error('An error occurred:', error);
} finally {
await mongoose.disconnect();
await historyConnection.close();
console.log('Disconnected from both MongoDB databases');
}
}

deleteUsersRolesAndHistory();
