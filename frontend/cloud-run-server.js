#!/usr/bin/env node

// CryptoHippo Production Server - Direct Express Server
// This file directly imports and runs the built Express server
// Ensures proper deployment functionality for cryptohippo.locker

// Set production environment variables
process.env.NODE_ENV = 'production';
process.env.PORT = process.env.PORT || '8080';

console.log('🚀 CryptoHippo Production Server');
console.log(`📍 Port: ${process.env.PORT}`);
console.log(`🌍 Environment: ${process.env.NODE_ENV}`);

// Import and start the complete Express server
import('./dist/index.js').catch((error) => {
  console.error('❌ Failed to start CryptoHippo server:', error);
  process.exit(1);
});

