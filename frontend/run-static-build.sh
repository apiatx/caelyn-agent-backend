#!/bin/bash

# Alternative deployment script that can be called from system PATH
# This addresses the "Command 'static-build' not found in the system PATH" error

echo "🚀 Starting deployment build process..."

# Change to project directory
cd "$(dirname "$0")"

# Run the build process
echo "📦 Running npm build..."
npm run build

# Run the static deployment fix
echo "🔧 Preparing static deployment..."
node static-deployment-fix.js

# Start the static server
echo "🌐 Starting deployment server..."
exec node static-server.js