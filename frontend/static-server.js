#!/usr/bin/env node

import express from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 8080;

console.log('🚀 Starting static server for deployment...');

// Check if static-build directory exists
const staticBuildPath = path.join(__dirname, 'static-build');

if (!fs.existsSync(staticBuildPath)) {
  console.log('❌ static-build directory not found. Running build process...');
  console.log('🔧 Run "node static-deployment-fix.js" first to create the static build');
  process.exit(1);
}

// Serve static files from static-build directory
app.use(express.static(staticBuildPath));

// Handle SPA routing - serve index.html for all routes
app.get('*', (req, res) => {
  res.sendFile(path.join(staticBuildPath, 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ Static server running on port ${PORT}`);
  console.log(`🌐 Serving files from: ${staticBuildPath}`);
  console.log(`📁 Available files:`, fs.readdirSync(staticBuildPath));
});