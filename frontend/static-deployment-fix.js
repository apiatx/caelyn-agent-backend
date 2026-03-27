#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

console.log('🔧 Creating bulletproof static deployment...');

// Step 1: Ensure we have a build first
const publicDir = path.join(process.cwd(), 'dist', 'public');
const staticDir = path.join(process.cwd(), 'static-build');

// Check if build exists
if (!fs.existsSync(publicDir)) {
  console.log('❌ dist/public/ not found. Build directory missing.');
  console.log('💡 Run "npm run build" first to create the build');
  process.exit(1);
}

// Remove any existing static build
if (fs.existsSync(staticDir)) {
  fs.rmSync(staticDir, { recursive: true, force: true });
  console.log('🧹 Cleaned existing static build');
}

// Create new static build directory
fs.mkdirSync(staticDir, { recursive: true });

// Copy index.html and fix paths
const indexPath = path.join(publicDir, 'index.html');
if (fs.existsSync(indexPath)) {
  let html = fs.readFileSync(indexPath, 'utf-8');
  
  // Fix absolute paths to relative paths for static hosting
  html = html.replace(/src="\/assets\//g, 'src="./assets/');
  html = html.replace(/href="\/assets\//g, 'href="./assets/');
  
  // Remove Replit dev banner for production
  html = html.replace(/<script type="text\/javascript" src="https:\/\/replit\.com\/public\/js\/replit-dev-banner\.js"><\/script>/g, '');
  
  // Add deployment metadata
  const deploymentComment = `\n<!-- Static deployment created: ${new Date().toISOString()} -->\n`;
  html = html.replace('</head>', `${deploymentComment}</head>`);
  
  fs.writeFileSync(path.join(staticDir, 'index.html'), html);
  console.log('✅ Fixed index.html with relative paths and deployment metadata');
} else {
  console.log('❌ index.html not found in build directory');
  process.exit(1);
}

// Copy assets folder
const assetsSource = path.join(publicDir, 'assets');
const assetsDest = path.join(staticDir, 'assets');

if (fs.existsSync(assetsSource)) {
  fs.cpSync(assetsSource, assetsDest, { recursive: true });
  console.log('✅ Copied assets folder');
} else {
  console.log('⚠️  No assets folder found, creating empty one');
  fs.mkdirSync(assetsDest, { recursive: true });
}

// Copy any other files that might be needed
const filesToCopy = ['favicon.ico', 'robots.txt', 'manifest.json'];
filesToCopy.forEach(file => {
  const sourcePath = path.join(publicDir, file);
  const destPath = path.join(staticDir, file);
  if (fs.existsSync(sourcePath)) {
    fs.copyFileSync(sourcePath, destPath);
    console.log(`✅ Copied ${file}`);
  }
});

// Verify deployment structure
const staticIndexPath = path.join(staticDir, 'index.html');
const staticAssetsPath = path.join(staticDir, 'assets');

if (fs.existsSync(staticIndexPath)) {
  const indexSize = fs.statSync(staticIndexPath).size;
  const assetsFiles = fs.existsSync(staticAssetsPath) ? fs.readdirSync(staticAssetsPath) : [];
  
  console.log('✅ Static build created successfully:');
  console.log(`   📄 index.html: ${Math.round(indexSize / 1024)}KB`);
  console.log(`   📁 assets/: ${assetsFiles.length} files`);
  
  // List key asset files for verification
  if (assetsFiles.length > 0) {
    const jsFiles = assetsFiles.filter(f => f.endsWith('.js')).length;
    const cssFiles = assetsFiles.filter(f => f.endsWith('.css')).length;
    console.log(`   🔧 Assets: ${jsFiles} JS, ${cssFiles} CSS files`);
  }
  
  console.log('🚀 Static deployment ready in static-build/ directory');
  console.log('🌐 Ready to serve with Node.js server or static hosting');
  
} else {
  console.log('❌ Static build verification failed - index.html missing');
  process.exit(1);
}