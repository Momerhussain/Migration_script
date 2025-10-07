// // pmd_check.js

// /**
//  * Reads a TXT file ("Rawalpindi - 06-12-24.txt") of mobile numbers (one per line),
//  * removes duplicates, strips any leading '0', calls the PMD Check API for each unique number
//  * (using a hard‐coded Bearer token), and writes an output TXT file ("output.txt") where each line is:
//  *   <originalNumber>,<code>,<name>
//  * or, if the API call fails:
//  *   <originalNumber>,ERROR,<errorMessage>
//  *
//  * To run:
//  *   1. Replace the placeholder token below with your actual Bearer token.
//  *   2. Ensure "Rawalpindi - 06-12-24.txt" lives in the same folder as this script.
//  *   3. Run:
//  *        node pmd_check.js
//  */

// pmd_check.js

/**
 * Reads a CSV file ("Rawalpindi-06-12-24.csv") of mobile numbers (one per line, CSV format),
 * strips any leading '0' from each number, calls the PMD Check API for each number
 * (using a hard-coded Bearer token), and writes an output CSV file ("output.csv") where each line is:
 *   <originalNumber>,<code>,<name>
 * or, if the API call fails:
 *   <originalNumber>,ERROR,<errorMessage>
*
* Usage:
*   1. Replace the placeholder token below with your actual Bearer token.
 *   2. Ensure "Rawalpindi-06-12-24.csv" is in the same folder as this script.
 *   3. Run:
 *        node pmd_check.js
*/

// pmd_check.js

/**
 * Reads a CSV file ("test2.csv") of mobile numbers (first column, one per line),
 * strips any leading '92' from each number before calling the API,
 * then writes an output CSV ("output.csv") with lines:
 *   <originalNumber>,<code>,<name>
 * (or <originalNumber>,ERROR,<errorMessage> if that call fails).
 *
 * Usage:
 *   1. Put this file next to "test2.csv".
 *   2. Replace the bearerToken below with your real PMD token.
 *   3. Run: node pmd_check.js
 */

// pmd_check.js

/**
 * Reads a CSV file ("test2.csv") of mobile numbers (first column, one per line),
 * skips the header row, strips any leading '92' from each subsequent number before calling the API,
 * then writes an output CSV ("output.csv") with lines:
 *   <originalNumber>,<code>,<name>
 * (or <originalNumber>,ERROR,<errorMessage> if that call fails).
 *
 * Usage:
 *   1. Put this file next to "test2.csv".
 *   2. Replace the bearerToken below with your real PMD token.
 *   3. Run: node pmd_check.js
 */

// pmd_check.js

/**
 * Reads a CSV file ("test2.csv") of mobile numbers (first column, one per line),
 * skips the header row, strips any leading '92' from each number before calling the API,
 * maps the API response code according to PmdEnum, then writes an output CSV ("output.csv")
 * with lines:
 *   <originalNumber>,<mappedCode>,<name>
 * (or <originalNumber>,ERROR,<errorMessage> if that call fails).
 *
 * Usage:
 *   1. Put this file next to "test2.csv".
 *   2. Replace the bearerToken below with your real PMD token.
 *   3. Run: node pmd_check.js
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

async function main() {
  // ── Hard-coded config ──
  const inputFile  = 'filtered_new_text_document.csv';
  const outputFile = 'output4.csv';

  // ← Replace this with your actual Bearer token:
  const bearerToken = 'ebb4cec3f22f32fc21b0709e8fbfb432';
  // ────────────────────────

  // PmdEnum mapping
  const codeMapping = {
    "01": "1",
    "03": "2",
    "04": "3",
    "06": "4",
    "07": "1"
  };

  const inputPath  = path.resolve(inputFile);
  const outputPath = path.resolve(outputFile);

  // 1. Read the entire CSV file
  let rawText;
  try {
    rawText = fs.readFileSync(inputPath, 'utf-8');
  } catch (err) {
    console.error(`❌ Could not read "${inputPath}": ${err.message}`);
    process.exit(1);
  }

  // 2. Split into lines, trim whitespace, discard empty lines
  const allLines = rawText
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(line => line.length > 0);

  // 3. Skip the first line (header) and process only data rows
  const dataLines = allLines.slice(1);

  // Prepare a write stream for the output CSV
  const writer = fs.createWriteStream(outputPath, { encoding: 'utf-8' });

  for (let i = 0; i < dataLines.length; i++) {
    // Each line may contain commas. We take only the first column as the number.
    const cols = dataLines[i].split(',');
    const originalNumber = cols[0].trim();
    if (!originalNumber) {
      continue; // skip if first column is empty
    }

    // 4. Strip leading '92' for API only
    let msisdn = originalNumber;
    if (msisdn.startsWith('92')) {
      msisdn = msisdn.substring(2);
    }

    // Generate a random 9-digit tID
    const tID = Math.floor(100000000 + Math.random() * 900000000);

    // Build API payload using the computed msisdn
    const payload = {
      msisdn: msisdn,
      tID: tID
    };

    // 5. Call PMD Check API
    try {
      const resp = await axios.post(
        'https://mni.pmd.org.pk/api/v1/rest/CheckMobileNumberNetwork',
        payload,
        {
          headers: {
            'Content-Type': 'application/json',
            'Authentication': `Bearer ${bearerToken}`
          },
          httpsAgent: new https.Agent({ rejectUnauthorized: false }),
          timeout: 3000
        }
      );

      // Original response code and name
      const originalCode = resp.data.code || "";
      const name = resp.data.name || "";

      // Map the response code according to PmdEnum
      const mappedCode = codeMapping[originalCode] || originalCode;

      // Write a line: <originalNumber>,<mappedCode>,<name>
      writer.write(`${originalNumber},${mappedCode},${name}\n`);
      console.log(`✔ [row ${i + 1}] ${originalNumber} → ${name} (mapped code: ${mappedCode})`);
    } catch (err) {
      let errorMessage;
      if (err.response) {
        errorMessage = `HTTP ${err.response.status} ${JSON.stringify(err.response.data)}`;
      } else {
        errorMessage = err.message;
      }
      writer.write(`${originalNumber},ERROR,${errorMessage}\n`);
      console.error(`✖ [row ${i + 1}] ${originalNumber} → Error: ${errorMessage}`);
    }
  }

  writer.end(() => {
    console.log(`\n✅ Done. Output written to "${outputPath}".`);
  });
}

main();



// pmd_single.js

/**
 * Simple test: Call the PMD Check API for a single hard-coded number.
 * Replace the `singleNumber` and `bearerToken` values below, then run:
 *   node pmd_single.js
 */

// const axios = require('axios');
// // import * as https from 'https';
// const https = require('https');
// async function checkSingle() {
//   // ── Hard-coded config ──
//   let singleNumber = '3238537940';         // <— replace with the number you want to test
//   const bearerToken  = '712c53744ea9211ad05baf46faab674b'; // <— replace with your real token
//   // ────────────────────────

//   // Remove leading '0' if present
//   if (singleNumber.startsWith('0')) {
//     singleNumber = singleNumber.substring(1);
//   }

//   const payload = {
//     msisdn: singleNumber,
//     tID: 3
//   };

//   try {
//     // const resp = await axios.post(
//     //   'https://mni.pmd.org.pk/api/v1/rest/CheckMobileNumberNetwork',
//     //   payload,
//     //   {
//     //     headers: {
//     //       'Content-Type': 'application/json',
//     //       'Authorization': `Bearer ${bearerToken}`
//     //     }
//     //   }
//     // );

//     // const resp =await axios.post(
//     //     'https://mni.pmd.org.pk/api/v1/rest/CheckMobileNumberNetwork',
//     //     null,  // No body needed
//     //     {
//     //       headers: {
//     //         'Authorization': `712c53744ea9211ad05baf46faab674b`,
//     //       },
//     //       httpsAgent: new https.Agent({ rejectUnauthorized: false }),
//     //       timeout: 3000,
//     //     }
//     // );

//     const resp =await axios.post(
//         'https://mni.pmd.org.pk/api/v1/rest/CheckMobileNumberNetwork',
//         payload,
//         {
//           headers: {
//             'Authentication': `Bearer ${bearerToken}`,
//             'Content-Type': 'application/json',
//           },
//           httpsAgent: new https.Agent({ rejectUnauthorized: false }),
//           timeout: 3000,
//         }
//     );
//     console.log(resp,'resp');
//     console.log(`Result for original number "${payload.msisdn}":`);
//     console.log(`  code: ${resp.data.code}`);
//     console.log(`  name: ${resp.data.name}`);
//   } catch (err) {
//     if (err.response) {
//       console.error(`API Error (${err.response.status}):`, err.response.data);
//     } else {
//       console.error('Network/Error:', err.message);
//     }
//   }
// }

// checkSingle();

