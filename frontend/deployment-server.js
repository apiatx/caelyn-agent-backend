#!/usr/bin/env node

// Direct Node.js deployment server - alternative to shell script approach
// This can be called directly as: node deployment-server.js

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

console.log('🚀 Starting deployment build and serve process...');

async function buildAndServe() {
  try {
    // Step 1: Build the application
    console.log('📦 Running build process...');
    const buildResult = await execAsync('npm run build');
    console.log('✅ Build completed successfully');
    
    // Step 2: Create static deployment
    console.log('🔧 Creating static deployment...');
    const { exec: execSync } = await import('child_process');
    await new Promise((resolve, reject) => {
      execSync('node static-deployment-fix.js', (error, stdout, stderr) => {
        if (error) {
          console.error('❌ Static deployment failed:', error);
          reject(error);
        } else {
          console.log(stdout);
          resolve();
        }
      });
    });

    // Step 3: Verify static build exists
    const staticBuildPath = path.join(__dirname, 'static-build');
    if (!fs.existsSync(staticBuildPath)) {
      throw new Error('Static build directory not created');
    }

    // Step 4: Configure Express to serve static files
    app.use(express.static(staticBuildPath));
    
    // Handle SPA routing
    app.get('*', (req, res) => {
      res.sendFile(path.join(staticBuildPath, 'index.html'));
    });

    // Step 5: Start server
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`✅ Deployment server running on port ${PORT}`);
      console.log(`🌐 Serving from: ${staticBuildPath}`);
      
      // List deployed files for verification
      const files = fs.readdirSync(staticBuildPath);
      console.log(`📁 Deployed files: ${files.join(', ')}`);
    });

  } catch (error) {
    console.error('❌ Deployment failed:', error);
    process.exit(1);
  }
}

// Start the build and serve process
buildAndServe();