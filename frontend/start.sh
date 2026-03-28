#!/bin/bash

# Production startup script for CryptoHippo
# This ensures the correct server configuration is used in deployment

echo "🚀 Starting CryptoHippo Production Server..."

# Set environment to production
export NODE_ENV=production

# Check if built files exist
if [ ! -f "dist/index.js" ]; then
    echo "📦 Building application..."
    npm run build
fi

echo "✅ Starting Express server with API routes..."
node dist/index.js