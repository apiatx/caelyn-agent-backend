#!/bin/bash

# Production Deployment Script for CryptoHippo
# This script ensures proper deployment with API routes

echo "🚀 CryptoHippo Production Deployment"
echo "======================================"

# Set production environment
export NODE_ENV=production

# Check if build exists
if [ ! -f "dist/index.js" ]; then
    echo "📦 Building application..."
    npm run build
    
    if [ $? -ne 0 ]; then
        echo "❌ Build failed"
        exit 1
    fi
    echo "✅ Build completed"
else
    echo "✅ Build files found"
fi

# Verify dist structure
if [ ! -d "dist/public" ]; then
    echo "❌ Frontend build not found in dist/public"
    exit 1
fi

echo "📊 Starting Express server with API routes..."
echo "🌐 Server will include:"
echo "   - Static file serving"
echo "   - API endpoints (/api/*)"
echo "   - CORS for cryptohippo.locker"
echo "   - Real-time market data"

# Start the full Express server
exec node dist/index.js