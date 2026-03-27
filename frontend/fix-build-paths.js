#!/usr/bin/env node
import fs from 'fs';
import path from 'path';

// Fix absolute paths in built HTML for static deployment
const indexPath = path.join(process.cwd(), 'dist', 'public', 'index.html');

if (fs.existsSync(indexPath)) {
  let html = fs.readFileSync(indexPath, 'utf-8');
  
  // Replace absolute asset paths with relative paths
  html = html.replace(/src="\/assets\//g, 'src="./assets/');
  html = html.replace(/href="\/assets\//g, 'href="./assets/');
  
  fs.writeFileSync(indexPath, html);
  console.log('✅ Fixed asset paths in index.html for static deployment');
} else {
  console.log('❌ index.html not found at', indexPath);
}