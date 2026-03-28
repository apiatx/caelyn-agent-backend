#!/usr/bin/env node

// Production server for CryptoHippo with full API support
// This ensures API endpoints work in production deployment
import express from 'express';
import cors from 'cors';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 5000;

console.log('🚀 CryptoHippo Production Server Starting...');
console.log(`📍 Port: ${PORT}`);
console.log(`🌍 Environment: ${process.env.NODE_ENV || 'production'}`);

// Set NODE_ENV to production if not set
if (!process.env.NODE_ENV) {
  process.env.NODE_ENV = 'production';
}

// CORS configuration for production
const corsConfig = cors({
  origin: function(origin, callback) {
    // Allow requests with no origin (mobile apps, curl, etc.)
    if (!origin) return callback(null, true);
    
    // Allow localhost for development
    if (origin.includes('localhost') || origin.includes('127.0.0.1')) {
      return callback(null, true);
    }
    
    // Allow Replit domains
    if (origin.includes('.replit.dev') || origin.includes('.replit.app') || origin.includes('.replit.co')) {
      return callback(null, true);
    }
    
    // Allow custom domain for CryptoHippo
    if (origin.includes('cryptohippo.locker')) {
      return callback(null, true);
    }
    
    // Block all other origins in production
    callback(new Error('Not allowed by CORS'));
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept'],
  maxAge: 86400 // 24 hours
});

// Apply CORS early
app.use(corsConfig);

// Body parsing
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: false, limit: '10mb' }));

// Import and register API routes
let registerRoutes;
try {
  // Import the server routes module
  const serverModule = await import('./server/index.js');
  console.log('✅ Server module loaded successfully');
  
  // Since server/index.js is a full server, we need to import the routes separately
  const routesModule = await import('./server/routes.js');
  registerRoutes = routesModule.registerRoutes;
  console.log('✅ Routes module loaded successfully');
} catch (error) {
  console.error('❌ Failed to load server modules:', error.message);
  console.log('🔄 Attempting to build server modules...');
  
  // Build the server modules if they don't exist
  const { exec } = await import('child_process');
  const { promisify } = await import('util');
  const execAsync = promisify(exec);
  
  try {
    await execAsync('npm run build');
    console.log('✅ Build completed');
    
    // Try importing again after build
    const routesModule = await import('./dist/routes.js');
    registerRoutes = routesModule.registerRoutes;
    console.log('✅ Routes loaded from build');
  } catch (buildError) {
    console.error('❌ Build failed:', buildError.message);
    console.log('🔄 Using development mode...');
    
    // Fall back to running directly from TypeScript
    const { spawn } = await import('child_process');
    const tsx = spawn('npx', ['tsx', 'server/index.ts'], {
      env: { ...process.env, NODE_ENV: 'production' },
      stdio: 'inherit'
    });
    
    tsx.on('close', (code) => {
      console.log(`Server process exited with code ${code}`);
      process.exit(code);
    });
    
    return; // Exit this script as we're delegating to the main server
  }
}

// Health check endpoints
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    service: 'cryptohippo-production',
    environment: process.env.NODE_ENV,
    apis: 'active'
  });
});

app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    service: 'cryptohippo-api',
    version: '1.0.0'
  });
});

// Register API routes if available
if (registerRoutes) {
  console.log('📡 Registering API routes...');
  try {
    await registerRoutes(app);
    console.log('✅ API routes registered successfully');
  } catch (error) {
    console.error('❌ Failed to register routes:', error.message);
  }
}

// Serve static files
const staticPath = path.join(__dirname, 'dist', 'public');
if (fs.existsSync(staticPath)) {
  app.use(express.static(staticPath, {
    maxAge: '1d',
    etag: true,
    lastModified: true
  }));
  console.log(`📂 Serving static files from: ${staticPath}`);
} else {
  console.warn(`⚠️  Static files not found at: ${staticPath}`);
}

// SPA routing fallback
app.get('*', (req, res) => {
  const indexPath = path.join(staticPath, 'index.html');
  
  if (fs.existsSync(indexPath)) {
    res.sendFile(indexPath);
  } else {
    res.status(404).json({
      error: 'Application not found',
      message: 'Please build the application first'
    });
  }
});

// Error handling
app.use((error, req, res, next) => {
  console.error('Production server error:', error);
  res.status(500).json({
    error: 'Internal server error',
    message: 'Something went wrong'
  });
});

// Start server
app.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ CryptoHippo Production Server running on port ${PORT}`);
  console.log(`🌐 API endpoints available at: http://0.0.0.0:${PORT}/api/`);
  console.log(`📱 Frontend available at: http://0.0.0.0:${PORT}/`);
  console.log(`❤️  Health check: http://0.0.0.0:${PORT}/health`);
  
  if (fs.existsSync(staticPath)) {
    const files = fs.readdirSync(staticPath);
    console.log(`📋 Serving ${files.length} static files`);
  }
  
  console.log('🎉 CryptoHippo ready for production!');
});