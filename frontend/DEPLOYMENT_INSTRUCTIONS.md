# How to Deploy CryptoHippo with Express Server

## Step-by-Step Deployment Instructions

### 1. Before Deploying
Verify your code is ready:
```bash
# Build the application
npm run build

# Test production server locally (optional)
NODE_ENV=production node dist/index.js
```

### 2. Deploy Using Replit Deploy Button
1. Click the "Deploy" button in your Replit workspace
2. In the deployment settings, ensure:
   - **Build Command**: `npm run build`
   - **Start Command**: `npm run start` (which runs `node dist/index.js`)
   - **Environment Variables**: Set `NODE_ENV=production`

### 3. Verify Deployment is Working
After deployment completes, test these URLs:

**API Health Check** (should return JSON):
```
https://cryptohippo.locker/api/health
```
Expected response:
```json
{"status":"healthy","timestamp":"...","service":"crypto-intelligence-platform"}
```

**Market Data API** (should return JSON):
```
https://cryptohippo.locker/api/coinmarketcap/market-overview
```
Expected response: Large JSON object with market data

**Frontend** (should load the app):
```
https://cryptohippo.locker/
```

### 4. If APIs Return HTML Instead of JSON
This means the deployment is using static file serving. To fix:

1. Check deployment logs for errors
2. Verify the start command is `npm run start` or `node dist/index.js`
3. Ensure `NODE_ENV=production` is set
4. Redeploy with correct Express server configuration

### 5. Success Indicators
✅ API endpoints return JSON data  
✅ Market Overview page loads CMC data  
✅ Base page shows DexScreener integrations  
✅ Portfolio page shows portfolio tools  

### 6. Common Issues
- **Static files only**: Start command is wrong, should be Express server
- **API 404 errors**: Server not running, only static files being served
- **CORS errors**: Environment variables not set correctly

The key is ensuring the deployment runs the Express server (`node dist/index.js`) which serves both API routes AND static files, not just static file hosting.