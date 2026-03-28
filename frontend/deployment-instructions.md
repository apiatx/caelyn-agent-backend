# CryptoHippo Production Deployment Instructions

## The Problem
The deployed website at cryptohippo.locker is serving static HTML files instead of running the Express server with API endpoints. This is why the Market Overview, Base page, and Portfolio page are not working.

## The Solution
The deployment needs to run the full Express server that includes both API routes and static file serving.

## Correct Deployment Commands

### Option 1: Use the built production server (RECOMMENDED)
```bash
npm run build
NODE_ENV=production node dist/index.js
```

### Option 2: Use TypeScript directly in production
```bash
NODE_ENV=production tsx server/index.ts
```

## Files That Must NOT Be Used for Deployment
- `deployment-server.js` - Only serves static files, no API
- `fixed-production-server.js` - Only serves static files, no API
- `serve-production.js` - Only serves static files, no API
- `simple-static-server.js` - Only serves static files, no API

## Environment Variables Required for Production
- `DATABASE_URL` - PostgreSQL connection string
- `NODE_ENV=production` - Sets production mode
- `PORT` - Server port (defaults to 5000)
- `ANTHROPIC_API_KEY` - For AI features
- `ETHERSCAN_API_KEY` - For blockchain data
- `BASESCAN_API_KEY` - For Base network data
- `TAOSTATS_API_KEY` - For Bittensor data

## Verification Steps
After deployment, these endpoints should return JSON data:
- `https://cryptohippo.locker/api/health` - Server health check
- `https://cryptohippo.locker/api/coinmarketcap/market-overview` - Market data
- `https://cryptohippo.locker/api/dashboard` - Dashboard data
- `https://cryptohippo.locker/api/portfolio/1` - Portfolio data

## Current Status
- ✅ CORS configuration fixed for cryptohippo.locker domain
- ✅ TypeScript errors resolved
- ✅ Local Express server works perfectly
- ✅ Production build completed successfully
- ❌ Deployment using wrong server configuration

## Next Steps for Deployment
1. Ensure Replit deployment uses `npm run start` or `NODE_ENV=production node dist/index.js`
2. Verify all environment variables are set in production
3. Test API endpoints return JSON data instead of HTML