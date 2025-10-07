const CryptoJS = require('crypto-js');

function encryptKey(otp) {
  const key = CryptoJS.enc.Utf8.parse("vOVH6sdmpNWjRRIqCc7rdxs01lwHzfr3");
  const encrypted = CryptoJS.AES.encrypt(otp, key, {
    mode: CryptoJS.mode.ECB,
  });
  return encrypted.toString();
}

function hexToDecimal (hex) {
    return Buffer.from(hex, 'hex').toString('utf8');
}
console.log(encryptKey("UzeH9IhFhm9Os4aXfBVSKA=="));