#!/usr/bin/env node

// Minimal static server for deployment
// Assumes build files already exist
// Fastest startup time for deployment

import express from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 8080;

console.log('🌐 Starting static server...');

// Find static files
let staticPath;
const possiblePaths = [
  path.join(__dirname, 'static-build'),
  path.join(__dirname, 'dist', 'public'),
  path.join(__dirname, 'dist'),
  path.join(__dirname, 'build')
];

for (const checkPath of possiblePaths) {
  if (fs.existsSync(checkPath) && fs.existsSync(path.join(checkPath, 'index.html'))) {
    staticPath = checkPath;
    break;
  }
}

if (!staticPath) {
  console.error('❌ No static files found. Searched paths:');
  possiblePaths.forEach(p => console.error(`   - ${p}`));
  process.exit(1);
}

console.log(`📂 Static files found: ${path.relative(__dirname, staticPath)}`);

// Basic security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  next();
});

// Health check for deployment monitoring
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy',
    staticPath: path.relative(__dirname, staticPath),
    timestamp: new Date().toISOString()
  });
});

// Serve static files
app.use(express.static(staticPath));

// SPA fallback
app.get('*', (req, res) => {
  res.sendFile(path.join(staticPath, 'index.html'));
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ Static server running on port ${PORT}`);
  console.log(`📋 Serving files from: ${staticPath}`);
  
  const files = fs.readdirSync(staticPath);
  console.log(`📁 Available: ${files.join(', ')}`);
});