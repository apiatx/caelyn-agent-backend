# Deployment Guide

This guide provides multiple solutions for the deployment errors:
- "Command 'static-build' not found in the system PATH"
- "Using Cloud Run deployment type for a static website build"
- "Build process completes successfully but deployment fails"

## Deployment Configuration Solutions

### ⭐ Method 1: Cloud Run Server (Recommended for Cloud Run)
**Best for: Replit Autoscale/Cloud Run deployment**

```bash
node cloud-run-server.js
```

**Replit Deployment Configuration:**
- Build command: `npm run build`
- Run command: `node cloud-run-server.js`
- Deployment type: **Cloud Run/Autoscale**

This server:
- Builds the application automatically on startup
- Provides proper health checks for Cloud Run
- Handles graceful shutdown
- Long-running process suitable for Cloud Run
- Includes build error handling and status reporting

### Method 2: Simple Static Server (Fast Startup)
**Best for: Pre-built applications**

```bash
node simple-static-server.js
```

**Replit Deployment Configuration:**
- Build command: `npm run build`
- Run command: `node simple-static-server.js`
- Deployment type: **Cloud Run/Autoscale**

This server:
- Fastest startup time
- Assumes build files already exist
- Minimal resource usage
- Perfect for pre-built static applications

### Method 3: Production Server (Flexible)
**Best for: Development or custom setups**

```bash
node serve-production.js
```

**Replit Deployment Configuration:**
- Build command: `npm run build`
- Run command: `node serve-production.js`
- Deployment type: **Cloud Run/Autoscale**

### Method 4: Static Deployment (Alternative)
**Best for: Pure static hosting**

If you want to use static deployment instead of Cloud Run:

**Replit Deployment Configuration:**
- Build command: `npm run build && node static-deployment-fix.js`
- Run command: Not needed (static files only)
- Deployment type: **Static**
- Serve from: `static-build/` directory

## Quick Fix Summary

### Current Error Resolution
The error "Command 'static-build' not found in the system PATH" occurs because:
1. The deployment is looking for a `static-build` executable in PATH
2. Cloud Run expects a long-running server process
3. Your app is a React SPA that needs proper server setup

### Immediate Solution
**Change your Replit deployment configuration to:**
- **Build command:** `npm run build`
- **Run command:** `node cloud-run-server.js`
- **Deployment type:** Cloud Run/Autoscale

This will:
✅ Build your React app automatically
✅ Create proper static file structure  
✅ Serve files with a long-running Node.js server
✅ Provide health checks for Cloud Run
✅ Handle graceful shutdown

## Deployment Type Comparison

| Method | Build Time | Startup | Best For | Resource Usage |
|--------|------------|---------|----------|----------------|
| `cloud-run-server.js` | Auto | Medium | Cloud Run | Medium |
| `simple-static-server.js` | Manual | Fast | Pre-built | Low |
| `serve-production.js` | Manual | Fast | Development | Low |
| Static deployment | Manual | N/A | CDN hosting | None |

## Health Check Endpoints

All servers provide health check endpoints for monitoring:
- `/health` - Detailed status information
- `/readiness` - Simple ready/not ready status

## Troubleshooting

### "Command not found" errors
✅ **Solution:** Use Node.js server scripts instead of shell commands

### "Using Cloud Run for static website" warnings  
✅ **Solution:** Cloud Run servers provide long-running processes as required

### Build timeouts
✅ **Solution:** `cloud-run-server.js` has 5-minute build timeout

### Port conflicts
✅ **Solution:** All servers use `process.env.PORT` with fallback to 8080

## Troubleshooting

### Error: "Command 'static-build' not found"
**Solution:** Use one of the methods above instead of the `static-build` command.

### Error: "Build files not found"
**Solution:** Run `npm run build` first to create the build files.

### Error: "Port already in use"
**Solution:** Change the PORT environment variable or kill existing processes.

## File Structure After Deployment

```
project/
├── dist/public/          # Vite build output
├── static-build/         # Deployment-ready files
│   ├── index.html       # Fixed paths for static hosting
│   └── assets/          # CSS, JS, images
├── serve-production.js   # Production server (recommended)
├── deployment-server.js  # All-in-one deployment
├── run-static-build.sh  # Build + serve script
└── static-server.js     # Static file server
```

## Recommended Configuration

For Replit Autoscale deployment, use:

**Build Command:** `npm run build`
**Run Command:** `node serve-production.js`

This provides the most reliable deployment with automatic build detection and production optimizations.