#!/usr/bin/env node

// Alternative production starter for CryptoHippo
// Sets NODE_ENV=production and starts the server

process.env.NODE_ENV = 'production';
process.env.PORT = process.env.PORT || '8080';

console.log('🚀 CryptoHippo Production Server Starting...');
console.log(`📍 Port: ${process.env.PORT}`);
console.log(`🌍 Environment: ${process.env.NODE_ENV}`);

// Import and start the server
import('./dist/index.js').catch((error) => {
  console.error('❌ Failed to start server:', error);
  process.exit(1);
});