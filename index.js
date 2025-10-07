const express = require('express');
const multer = require('multer');
const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');

const app = express();
const port = 3008;

// Configure multer to save uploaded files to the "uploads" folder
const upload = multer({ dest: 'uploads/' });

// POST endpoint to receive the uploaded Excel file
app.post('/upload', upload.single('file'), (req, res) => {
  try {
    // Get the path of the uploaded file
    const filePath = req.file.path;

    // Read the workbook using the XLSX module
    const workbook = XLSX.readFile(filePath);

    // Assuming the file has one sheet; otherwise, you may need to specify the correct sheet.
    const firstSheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[firstSheetName];

    // Convert the sheet data into a JSON array of objects
    const jsonData = XLSX.utils.sheet_to_json(worksheet);

    // Initialize a map to count the frequency of each mobile number
    const frequencyMap = {};
    jsonData.forEach(record => {
      // Assumes the Excel file has a header "mobileNumber"
      const mobile = record.mobileNumber;
      if (mobile) {
        frequencyMap[mobile] = (frequencyMap[mobile] || 0) + 1;
      }
    });

    // Calculate unique numbers and duplicates
    let duplicateCount = 0;
    const uniqueCount = Object.keys(frequencyMap).length;
    Object.values(frequencyMap).forEach(count => {
      if (count > 1) {
        duplicateCount += count - 1;
      }
    });

    // Optionally, remove the file after processing
    fs.unlink(filePath, (err) => {
      if (err) console.error('Error deleting file:', err);
    });

    // Send the results back to the client
    res.json({
      uniqueNumbers: uniqueCount,
      duplicateNumbers: duplicateCount
    });
  } catch (error) {
    console.error('Error processing file:', error);
    res.status(500).send('Error processing file');
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});
