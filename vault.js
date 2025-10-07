// vaultClient.js
const vault = require('node-vault')({
    endpoint: process.env.VAULT_ADDR || 'http://127.0.0.1:8200',
    token:    process.env.VAULT_TOKEN,           // your Vault auth token
    // optional: namespace, proxy, agent, timeout, etc.
  });
  


async function demo() {
  // Read a secret from the KV engine at path “secret/data/my-app”
  const secret = await vault.read('secret/data/my-app');
  console.log('My secret value:', secret.data.data);

  // Encrypt data via the Transit engine
  const enc = await vault.write('transit/encrypt/my-key', {
    plaintext: Buffer.from('hello world', 'utf8').toString('base64'),
  });
  console.log('Ciphertext:', enc.data.ciphertext);

  // Decrypt it back
  const dec = await vault.write('transit/decrypt/my-key', {
    ciphertext: enc.data.ciphertext,
  });
  const clear = Buffer.from(dec.data.plaintext, 'base64').toString('utf8');
  console.log('Decrypted:', clear);
}

demo().catch(console.error);
