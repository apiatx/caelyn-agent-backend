#!/usr/bin/env node

// Production server for Cloud Run deployment
// This is a long-running server process suitable for Cloud Run
// Can be used as run command: node serve-production.js

import express from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 8080;

console.log('🌐 Starting production server...');

// Check for existing static build, if not found, use dist/public
let staticPath = path.join(__dirname, 'static-build');

if (!fs.existsSync(staticPath)) {
  staticPath = path.join(__dirname, 'dist', 'public');
  console.log('📁 Using dist/public as static path');
  
  if (!fs.existsSync(staticPath)) {
    console.error('❌ No build files found. Run "npm run build" first.');
    process.exit(1);
  }
} else {
  console.log('📁 Using static-build as static path');
}

// Security headers for production
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  next();
});

// Serve static files
app.use(express.static(staticPath, {
  maxAge: '1d', // Cache static assets for 1 day
  etag: true,
  lastModified: true
}));

// Health check endpoint for deployment monitoring
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    timestamp: new Date().toISOString(),
    staticPath: staticPath.replace(__dirname, '.'),
    files: fs.existsSync(staticPath) ? fs.readdirSync(staticPath).length : 0
  });
});

// Handle SPA routing - serve index.html for all other routes
app.get('*', (req, res) => {
  const indexPath = path.join(staticPath, 'index.html');
  if (fs.existsSync(indexPath)) {
    res.sendFile(indexPath);
  } else {
    res.status(404).send('Application not found. Please build the application first.');
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ Production server running on port ${PORT}`);
  console.log(`📂 Serving files from: ${staticPath.replace(__dirname, '.')}`);
  console.log(`🔗 Health check: http://localhost:${PORT}/health`);
  
  // List available files
  if (fs.existsSync(staticPath)) {
    const files = fs.readdirSync(staticPath);
    console.log(`📋 Available files: ${files.join(', ')}`);
  }
});