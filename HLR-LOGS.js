const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Configuration
const INPUT_FILE = 'tyk-logs.csv';
const OUTPUT_FILE = 'filtered_date25_with_operator_msisdn.csv';

// Operator mapping function
function mapOperator(operator) {
    if (operator === "1") return "Mobilink";
    else if (operator === "2") return "Ufone";
    else if (operator === "3") return "Zong";
    else if (operator === "4") return "Telenor";
    else if (operator === "5") return "Mobilink";
    else return "Other";
}

// Function to extract date from createdAt string
function extractDate(createdAt) {
    const match = createdAt.match(/\w+\s+\w+\s+(\d+)\s+\d{4}/);
    return match ? match[1] : null;
}

// Main processing function
async function processCSV() {
    const results = [];
    let totalRows = 0;
    let filteredRows = 0;
    
    console.log('Starting CSV processing...');
    console.log(`Reading from: ${INPUT_FILE}`);
    
    // Read and process the CSV file
    return new Promise((resolve, reject) => {
        fs.createReadStream(INPUT_FILE)
            .pipe(csv())
            .on('data', (row) => {
                totalRows++;
                
                // Check if this row is from date 25
                const date = extractDate(row.createdAt);
                if (date === '25') {
                    filteredRows++;
                    
                    // Parse the response column to extract operator and msisdn
                    let operator = '';
                    let msisdn = '';
                    let operatorName = '';
                    
                    try {
                        const responseObj = JSON.parse(row.response);
                        if (responseObj.responseData) {
                            operator = responseObj.responseData.operator || '';
                            msisdn = responseObj.responseData.msisdn || '';
                            operatorName = mapOperator(operator);
                        }
                    } catch (error) {
                        console.error(`Error parsing response for row ${row._id}: ${error.message}`);
                    }
                    
                    // Add the new columns to the row
                    const processedRow = {
                        ...row,
                        operator: operatorName,
                        msisdn: msisdn
                    };
                    
                    results.push(processedRow);
                }
            })
            .on('end', async () => {
                console.log(`\nProcessing complete!`);
                console.log(`Total rows read: ${totalRows}`);
                console.log(`Rows filtered for date 25: ${filteredRows}`);
                
                if (results.length === 0) {
                    console.log('No rows found for date 25!');
                    resolve();
                    return;
                }
                
                // Get all column headers including the new ones
                const headers = Object.keys(results[0]);
                const csvHeaders = headers.map(header => ({
                    id: header,
                    title: header
                }));
                
                // Create CSV writer
                const csvWriter = createCsvWriter({
                    path: OUTPUT_FILE,
                    header: csvHeaders
                });
                
                // Write the processed data to new CSV file
                try {
                    await csvWriter.writeRecords(results);
                    console.log(`\nSuccess! Processed data written to: ${OUTPUT_FILE}`);
                    console.log(`New file contains ${results.length} rows with ${headers.length} columns`);
                    
                    // Show sample of processed data
                    console.log('\nSample of processed data (first 3 rows):');
                    results.slice(0, 3).forEach((row, index) => {
                        console.log(`\nRow ${index + 1}:`);
                        console.log(`  MSISDN: ${row.msisdn}`);
                        console.log(`  Operator: ${row.operator}`);
                        console.log(`  Created: ${row.createdAt}`);
                    });
                    
                    resolve();
                } catch (error) {
                    console.error('Error writing CSV file:', error);
                    reject(error);
                }
            })
            .on('error', (error) => {
                console.error('Error reading CSV file:', error);
                reject(error);
            });
    });
}

// Run the script
processCSV().catch(console.error);

// Instructions for running the script
console.log(`
====================================
CSV Processing Script
====================================
This script will:
1. Read ${INPUT_FILE}
2. Filter rows for date 25
3. Extract operator and msisdn from response column
4. Map operator codes to names
5. Save to ${OUTPUT_FILE}

Make sure to install required dependencies:
npm install csv-parser csv-writer

To run: node process_csv.js
====================================
`);