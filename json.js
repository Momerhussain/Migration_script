const jwt = require('jsonwebtoken');

const secretKey = 'vOVH6sdmpNWjRRIqCc7rdxs01lwHzfr3'; // Your secret key
const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3ZTNhNWY2NDUyM2I0OTI2ZTZlYTI1MCIsImVtYWlsQWRkcmVzcyI6Im51YmFzaGlyYS5raGFuQHlvcG1haWwuY29tIiwiY3VzdG9tZXJJZCI6IjZiMTA2OGE0LWEwODItNDQyOS1hMjUwLTU2Mjg4ZWE3OGI0ZiIsImF1ZCI6IjZiMTA2OGE0LWEwODItNDQyOS1hMjUwLTU2Mjg4ZWE3OGI0ZiIsImZpcnN0TmFtZSI6Ik11YmFzaGlyYSIsImxhc3ROYW1lIjoiICIsInJvbGVJRCI6WyI2N2UzYTVmNjQ1MjNiNDkyNmU2ZWEyNGYiXSwic2hvcnRjb2RlcyI6WyI4ODEwIl0sImlhdCI6MTc0NjQ0OTQyNCwiZXhwIjoxNzQ2NTM0MDI0fQ.1XJwAsB_qv6hcklL46tvk9DdyD08euOBBT-QbVIyUk8'; // Your JWT token

const verifyToken = (token) => {
  try {
    const decoded = jwt.verify(token, secretKey);
    console.log('Token is valid:', decoded);
    return decoded; // This will return the decoded token if valid
  } catch (error) {
    console.error('Token verification failed:', error.message);
    return null; // Return null if the token is invalid
  }
};

// Verify the token
const decodedToken = verifyToken(token);
