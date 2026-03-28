#!/usr/bin/env node

import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = 3001;

// Serve static files
app.use(express.static(path.join(__dirname, 'dist/public')));

// SPA routing - serve index.html for all routes
app.get('*', (req, res) => {
  console.log(`📄 SPA route: ${req.path}`);
  res.sendFile(path.join(__dirname, 'dist/public/index.html'));
});

app.listen(PORT, () => {
  console.log(`✅ Test SPA server running on http://localhost:${PORT}`);
  console.log(`🔗 Test routes:`);
  console.log(`   http://localhost:${PORT}/`);
  console.log(`   http://localhost:${PORT}/app/majors`);
  console.log(`   http://localhost:${PORT}/app/ethereum`);
});