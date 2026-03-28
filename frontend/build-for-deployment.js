#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

console.log('🔧 Building for deployment...');

// Step 1: Fix absolute paths to relative paths
const indexPath = path.join(process.cwd(), 'dist', 'public', 'index.html');

if (fs.existsSync(indexPath)) {
  let html = fs.readFileSync(indexPath, 'utf-8');
  
  // Replace absolute asset paths with relative paths
  html = html.replace(/src="\/assets\//g, 'src="./assets/');
  html = html.replace(/href="\/assets\//g, 'href="./assets/');
  
  fs.writeFileSync(indexPath, html);
  console.log('✅ Fixed asset paths for static deployment');
} else {
  console.log('❌ index.html not found at', indexPath);
  process.exit(1);
}

// Step 2: Copy contents to deployment root (dist/ instead of dist/public/)
const publicDir = path.join(process.cwd(), 'dist', 'public');
const distDir = path.join(process.cwd(), 'dist');

if (fs.existsSync(publicDir)) {
  // Copy index.html to dist root
  fs.copyFileSync(
    path.join(publicDir, 'index.html'), 
    path.join(distDir, 'index.html')
  );
  
  // Copy assets folder to dist root
  const assetsSource = path.join(publicDir, 'assets');
  const assetsDest = path.join(distDir, 'assets');
  
  if (fs.existsSync(assetsSource)) {
    // Remove existing assets folder if it exists
    if (fs.existsSync(assetsDest)) {
      fs.rmSync(assetsDest, { recursive: true, force: true });
    }
    
    // Copy assets folder
    fs.cpSync(assetsSource, assetsDest, { recursive: true });
    console.log('✅ Copied assets to deployment root');
  }
  
  console.log('✅ Prepared deployment structure in dist/');
} else {
  console.log('❌ dist/public/ not found');
  process.exit(1);
}

console.log('🚀 Deployment build complete!');