// pmd_check_excel.js
// Reads an XLSX file, takes numbers from a chosen column, strips leading '92' ONLY for the API call,
// calls PMD Check API, and writes: <originalNumber>,<mappedCode|ERROR>,<name|errorMessage> to output CSV.

const fs = require('fs');
const path = require('path');
const https = require('https');
const axios = require('axios');
const xlsx = require('xlsx');

async function main() {
  // ── Hard-coded config ──
  const inputExcel = 'sample_numbers2.xlsx'; // <- your XLSX file
  const sheetName  = null;        // null = first sheet, or e.g. "Sheet1"
  const columnIndex = 0;          // 0-based column index to read numbers from (e.g., 0 = first column)
  const hasHeader   = true;       // true = skip first row

  const outputFile = 'sample_numbers_pmd2.csv';

  // Replace with your real Bearer token:
  const bearerToken = 'ebb4cec3f22f32fc21b0709e8fbfb432';
  // ────────────────────────

  // PmdEnum mapping (as in your code)
  const codeMapping = {
    "01": "1",
    "03": "2",
    "04": "3",
    "06": "4",
    "07": "1"
  };

  const inputPath  = path.resolve(inputExcel);
  const outputPath = path.resolve(outputFile);

  // 1) Read Excel (as array-of-arrays using header:1)
  let rows;
  try {
    const wb = xlsx.readFile(inputPath);
    const wsName = sheetName || wb.SheetNames[0];
    const ws = wb.Sheets[wsName];
    if (!ws) throw new Error(`Sheet "${wsName}" not found.`);
    // rows: array of arrays (each row is an array of cell values)
    rows = xlsx.utils.sheet_to_json(ws, { header: 1, defval: '' });
  } catch (err) {
    console.error(`❌ Could not read "${inputPath}": ${err.message}`);
    process.exit(1);
  }

  if (!Array.isArray(rows) || rows.length === 0) {
    console.error('❌ Excel appears empty.');
    process.exit(1);
  }

  // 2) Build list of numbers from chosen column
  const startRow = hasHeader ? 1 : 0;
  const numbers = [];
  for (let r = startRow; r < rows.length; r++) {
    const row = rows[r];
    if (!Array.isArray(row)) continue;
    const cell = (row[columnIndex] ?? '').toString().trim();
    if (cell) numbers.push(cell);
  }

  console.log(`Loaded ${numbers.length} numbers from Excel (sheet: ${sheetName || 'first'}, colIndex: ${columnIndex}).`);

  // Prepare a write stream for the output CSV (no header, same as your script)
  const writer = fs.createWriteStream(outputPath, { encoding: 'utf-8' });

  for (let i = 0; i < numbers.length; i++) {
    const originalNumber = numbers[i];

    // Strip leading '92' ONLY for API call
    let msisdn = originalNumber;
    if (msisdn.startsWith('92')) {
      msisdn = msisdn.substring(2);
    }

    // Random 9-digit tID
    const tID = Math.floor(100000000 + Math.random() * 900000000);

    const payload = { msisdn, tID };

    try {
      const resp = await axios.post(
        'https://mni.pmd.org.pk/api/v1/rest/CheckMobileNumberNetwork',
        payload,
        {
          headers: {
            'Content-Type': 'application/json',
            // Note: Your existing script uses "Authentication", keeping it the same.
            'Authentication': `Bearer ${bearerToken}`
          },
          httpsAgent: new https.Agent({ rejectUnauthorized: false }),
          timeout: 3000
        }
      );

      const originalCode = (resp.data && resp.data.code) ? String(resp.data.code) : "";
      const name = (resp.data && resp.data.name) ? String(resp.data.name) : "";

      const mappedCode = codeMapping[originalCode] || originalCode;

      writer.write(`${originalNumber},${mappedCode},${name}\n`);
      console.log(`✔ [${i + 1}/${numbers.length}] ${originalNumber} → ${name} (mapped code: ${mappedCode})`);
    } catch (err) {
      let errorMessage;
      if (err.response) {
        errorMessage = `HTTP ${err.response.status} ${JSON.stringify(err.response.data)}`;
      } else {
        errorMessage = err.message;
      }
      writer.write(`${originalNumber},ERROR,${errorMessage}\n`);
      console.error(`✖ [${i + 1}/${numbers.length}] ${originalNumber} → Error: ${errorMessage}`);
    }
  }

  writer.end(() => {
    console.log(`\n✅ Done. Output written to "${outputPath}".`);
  });
}

main();
