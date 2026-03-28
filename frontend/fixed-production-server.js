#!/usr/bin/env node

// Production server for CryptoHippo with proper SPA routing
import express from 'express';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 8080;

console.log('🚀 CryptoHippo production server starting...');
console.log(`📍 Port: ${PORT}`);

let buildReady = false;
let buildError = null;

// Build the application
async function buildApp() {
  try {
    console.log('🔧 Building CryptoHippo...');
    await execAsync('npm run build', { 
      cwd: __dirname,
      timeout: 300000 
    });
    console.log('✅ Build completed successfully');
    buildReady = true;
  } catch (error) {
    console.error('❌ Build failed:', error.message);
    buildError = error;
    buildReady = false;
  }
}

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: buildReady ? 'ready' : 'building',
    ready: buildReady,
    error: buildError ? buildError.message : null,
    timestamp: new Date().toISOString()
  });
});

// Wait for build before serving
app.use((req, res, next) => {
  if (!buildReady && !req.path.startsWith('/health')) {
    return res.status(503).json({
      error: 'Service building',
      message: 'CryptoHippo is building, please wait...'
    });
  }
  next();
});

// Security headers
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  
  // Cache static assets
  if (req.path.includes('/assets/')) {
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
  } else if (req.path.endsWith('.html')) {
    res.setHeader('Cache-Control', 'no-cache');
  } else {
    res.setHeader('Cache-Control', 'public, max-age=3600');
  }
  
  next();
});

// Serve static files from dist/public
const staticPath = path.join(__dirname, 'dist', 'public');
app.use(express.static(staticPath, {
  index: false, // Don't auto-serve index.html
  maxAge: '1d'
}));

// SPA routing - serve index.html for all non-static routes
app.get('*', (req, res) => {
  const indexPath = path.join(staticPath, 'index.html');
  
  if (fs.existsSync(indexPath)) {
    console.log(`📄 SPA route: ${req.path} -> index.html`);
    res.sendFile(indexPath);
  } else {
    console.error(`❌ Missing index.html at: ${indexPath}`);
    res.status(404).json({
      error: 'CryptoHippo not found',
      message: 'Application build files missing'
    });
  }
});

// Error handling
app.use((error, req, res, next) => {
  console.error('Server error:', error);
  res.status(500).json({
    error: 'Internal server error',
    message: 'Something went wrong with CryptoHippo'
  });
});

// Start server
app.listen(PORT, '0.0.0.0', async () => {
  console.log(`✅ CryptoHippo server running on port ${PORT}`);
  
  // Build after server starts
  await buildApp();
  
  if (buildReady) {
    const files = fs.readdirSync(staticPath);
    console.log(`📂 Serving ${files.length} files from: ${path.relative(__dirname, staticPath)}`);
    console.log('🎉 CryptoHippo ready for production traffic!');
    console.log(`🌐 Access at: http://0.0.0.0:${PORT}`);
  } else {
    console.log('❌ CryptoHippo build failed - check logs above');
  }
});