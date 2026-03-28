import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import cors from 'cors';
import { body, validationResult } from 'express-validator';
import type { Request, Response, NextFunction } from 'express';
import xss from 'xss';
import DOMPurify from 'dompurify';
import { JSDOM } from 'jsdom';

// Initialize DOMPurify with JSDOM for server-side use
const window = new JSDOM('').window;
const purify = DOMPurify(window);

/**
 * Content Security Policy configuration
 */
export const cspConfig = helmet.contentSecurityPolicy({
  directives: {
    defaultSrc: ["'self'", "https://investing.com", "https://*.investing.com", "https://www.investing.com"],
    scriptSrc: [
      "'self'",
      "'unsafe-inline'",
      "'unsafe-eval'",
      "https://thenew.money",
      "https://*.thenew.money",
      "https://www.thenew.money",
      "https://tradingview.com",
      "https://*.tradingview.com",
      "https://dexscreener.com",
      "https://*.dexscreener.com",
      "https://taostats.io",
      "https://*.hyperliquid.xyz",
      "https://cdn.jsdelivr.net",
      "https://unpkg.com",
      "https://mobyscreener.com",
      "https://*.mobyscreener.com",
      "https://app.mobyscreener.com",
      "https://artemis.xyz",
      "https://*.artemis.xyz",
      "https://app.artemis.xyz",
      "https://artemisanalytics.com",
      "https://*.artemisanalytics.com",
      "https://app.artemisanalytics.com",
      "https://zoracle.xyz",
      "https://*.zoracle.xyz",
      "https://www.zoracle.xyz",
      "https://stocktwits.com",
      "https://*.stocktwits.com",
      "https://www.stocktwits.com",
      "https://investing.com",
      "https://*.investing.com",
      "https://www.investing.com",
      "https://macroedge.ai",
      "https://*.macroedge.ai",
      "https://blockcreeper.com",
      "https://*.blockcreeper.com",
      "https://www.blockcreeper.com",
      "https://taofi.com",
      "https://*.taofi.com",
      "https://www.taofi.com"
    ],
    styleSrc: [
      "'self'",
      "'unsafe-inline'",
      "https://thenew.money",
      "https://*.thenew.money",
      "https://www.thenew.money",
      "https://fonts.googleapis.com",
      "https://cdn.jsdelivr.net",
      "https://mobyscreener.com",
      "https://*.mobyscreener.com",
      "https://artemis.xyz",
      "https://*.artemis.xyz",
      "https://artemisanalytics.com",
      "https://*.artemisanalytics.com",
      "https://zoracle.xyz",
      "https://*.zoracle.xyz",
      "https://www.zoracle.xyz",
      "https://stocktwits.com",
      "https://*.stocktwits.com",
      "https://www.stocktwits.com",
      "https://app.intellectia.ai",
      "https://intellectia.ai",
      "https://*.intellectia.ai",
      "https://investing.com",
      "https://*.investing.com",
      "https://www.investing.com",
      "https://blockcreeper.com",
      "https://*.blockcreeper.com",
      "https://www.blockcreeper.com",
      "https://taofi.com",
      "https://*.taofi.com",
      "https://www.taofi.com",
      // DeFi Platforms
      "https://tharwa.xyz",
      "https://*.tharwa.xyz",
      "https://harvest.finance",
      "https://*.harvest.finance",
      "https://stacks.co",
      "https://*.stacks.co",
      "https://stacks.org",
      "https://*.stacks.org",
      "https://satlayer.xyz",
      "https://*.satlayer.xyz",
      "https://satlayer.io",
      "https://*.satlayer.io"
    ],
    imgSrc: [
      "'self'",
      "data:",
      "blob:",
      "https:",
      "http:"
    ],
    fontSrc: [
      "'self'",
      "https://fonts.gstatic.com",
      "https://cdn.jsdelivr.net"
    ],
    connectSrc: [
      "'self'",
      "https://thenew.money",
      "https://*.thenew.money",
      "https://www.thenew.money",
      "https:",
      "wss:",
      "ws:",
      "https://mobyscreener.com",
      "https://*.mobyscreener.com",
      "https://api.mobyscreener.com",
      "https://fonts.googleapis.com",
      "https://api.coingecko.com",
      "https://api.dexscreener.com",
      "https://artemis.xyz",
      "https://*.artemis.xyz",
      "https://api.artemis.xyz",
      "https://artemisanalytics.com",
      "https://*.artemisanalytics.com",
      "https://api.artemisanalytics.com",
      "https://zoracle.xyz",
      "https://*.zoracle.xyz",
      "https://www.zoracle.xyz",
      "https://api.zoracle.xyz",
      "https://stocktwits.com",
      "https://*.stocktwits.com",
      "https://www.stocktwits.com",
      "https://app.intellectia.ai",
      "https://intellectia.ai",
      "https://*.intellectia.ai",
      "https://investing.com",
      "https://*.investing.com",
      "https://www.investing.com",
      "https://taofi.com",
      "https://*.taofi.com",
      "https://www.taofi.com",
      // DeFi Platforms
      "https://tharwa.xyz",
      "https://*.tharwa.xyz",
      "https://harvest.finance",
      "https://*.harvest.finance",
      "https://stacks.co",
      "https://*.stacks.co",
      "https://stacks.org",
      "https://*.stacks.org",
      "https://satlayer.xyz",
      "https://*.satlayer.xyz",
      "https://satlayer.io",
      "https://*.satlayer.io"
    ],
    frameSrc: [
      "'self'",
      "https://tradingview.com",
      "https://*.tradingview.com",
      "https://dexscreener.com",
      "https://*.dexscreener.com",
      "https://taostats.io",
      "https://*.taostats.io",
      "https://hyperliquid.xyz",
      "https://*.hyperliquid.xyz",
      "https://backprop.finance",
      "https://taomarketcap.com",
      "https://swordscan.com",
      "https://taohub.info",
      "https://*.taohub.info",
      "https://peapods.finance",
      "https://artemis.xyz",
      "https://*.artemis.xyz",
      "https://app.artemis.xyz",
      "https://artemis.xyz/embed",
      "https://artemisanalytics.com",
      "https://*.artemisanalytics.com",
      "https://app.artemisanalytics.com",
      "https://cointelegraph.com",
      "https://newhedge.io",
      "https://bitcointreasuries.net",
      "https://strategicethreserve.xyz",
      "https://taotreasuries.app",
      "https://app.hyperliquid.xyz",
      "https://hyperdash.info",
      "https://app.hyperswap.exchange",
      "https://velo.xyz",
      "https://stockanalysis.com",
      "https://screener.in",
      "https://ainvest.com",
      "https://polymarket.com",
      "https://*.polymarket.com",
      "https://gamma.polymarket.com",
      "https://hyperfolio.xyz",
      "https://*.hyperfolio.xyz",
      "https://www.hyperfolio.xyz",
      "https://terminal.co",
      "https://*.terminal.co",
      "https://zoracle.xyz",
      "https://*.zoracle.xyz",
      "https://www.zoracle.xyz",
      "https://checkr.social",
      "https://*.checkr.social",
      "https://blockcreeper.com",
      "https://*.blockcreeper.com",
      "https://www.blockcreeper.com",
      "https://virtuals.io",
      "https://*.virtuals.io",
      "https://app.virtuals.io",
      "https://strategicethreserve.xyz",
      "https://*.strategicethreserve.xyz",
      "https://www.strategicethreserve.xyz",
      "https://mobyscreener.com",
      "https://*.mobyscreener.com",
      "https://www.mobyscreener.com",
      "https://app.mobyscreener.com",
      "https://api.mobyscreener.com",
      "https://jup.ag",
      "https://*.jup.ag",
      "https://app.jup.ag",
      "https://portal.abs.xyz",
      "https://*.abs.xyz",
      "https://abs.xyz",
      "https://pudgyinvest.com",
      "https://*.pudgyinvest.com",
      "https://thenew.money",
      "https://*.thenew.money",
      "https://www.thenew.money",
      "https://app.intellectia.ai",
      "https://intellectia.ai",
      "https://*.intellectia.ai",
      "https://stocktwits.com",
      "https://*.stocktwits.com",
      "https://www.stocktwits.com",
      "https://accounts.google.com",
      "https://*.googleapis.com",
      "https://apis.google.com",
      "https://app.intellectia.ai",
      "https://intellectia.ai",
      "https://*.intellectia.ai",
      "https://investing.com",
      "https://*.investing.com",
      "https://www.investing.com",
      "https://macroedge.ai",
      "https://*.macroedge.ai",
      "https://taofi.com",
      "https://*.taofi.com",
      "https://www.taofi.com",
      "https://l2beat.com",
      "https://*.l2beat.com",
      "https://www.l2beat.com",
      "https://etherscan.io",
      "https://*.etherscan.io",
      "https://www.etherscan.io",
      "https://defillama.com",
      "https://*.defillama.com",
      "https://www.defillama.com",
      "https://uniswap.org",
      "https://*.uniswap.org",
      "https://app.uniswap.org",
      "https://aave.com",
      "https://*.aave.com",
      "https://app.aave.com",
      "https://opensea.io",
      "https://*.opensea.io",
      "https://www.opensea.io",
      "https://compound.finance",
      "https://*.compound.finance",
      "https://app.compound.finance",
      "https://coinglass.com",
      "https://*.coinglass.com",
      "https://www.coinglass.com",
      "https://www.theblock.co",
      "https://theblock.co",
      "https://*.theblock.co",
      "https://growthepie.com",
      "https://*.growthepie.com",
      "https://www.growthepie.com",
      "https://buymeacoffee.com",
      "https://*.buymeacoffee.com",
      "https://cloudbet.com",
      "https://*.cloudbet.com",
      "https://www.cloudbet.com",
      "https://cryptoquant.com",
      "https://*.cryptoquant.com",
      "https://www.cryptoquant.com",
      "https://bitcoinmagazinepro.com",
      "https://*.bitcoinmagazinepro.com",
      "https://www.bitcoinmagazinepro.com",
      "https://charts.bitbo.io",
      "https://bitbo.io",
      "https://*.bitbo.io",
      "https://tao.bot",
      "https://*.tao.bot",
      "https://www.tao.bot",
      // DeFi Platforms
      "https://tharwa.xyz",
      "https://*.tharwa.xyz",
      "https://harvest.finance",
      "https://*.harvest.finance",
      "https://stacks.co",
      "https://*.stacks.co",
      "https://stacks.org",
      "https://*.stacks.org",
      "https://satlayer.xyz",
      "https://*.satlayer.xyz",
      "https://satlayer.io",
      "https://*.satlayer.io",
      "https://matcha.xyz",
      "https://*.matcha.xyz",
      "https://app.dodoex.io",
      "https://dodoex.io",
      "https://*.dodoex.io",
      "https://app.xy.finance",
      "https://xy.finance",
      "https://*.xy.finance",
      "https://app.tabtrader.com",
      "https://tabtrader.com",
      "https://*.tabtrader.com",
      "https://app.rubic.exchange",
      "https://rubic.exchange",
      "https://*.rubic.exchange"
    ],
    objectSrc: ["'none'"],
    mediaSrc: ["'self'", "https:", "data:"],
    workerSrc: [
      "'self'", 
      "blob:",
      "https://mobyscreener.com",
      "https://*.mobyscreener.com",
      "https://artemis.xyz",
      "https://*.artemis.xyz",
      "https://zoracle.xyz",
      "https://*.zoracle.xyz",
      "https://www.zoracle.xyz"
    ],
    childSrc: [
      "'self'", 
      "blob:",
      "https://mobyscreener.com",
      "https://*.mobyscreener.com",
      "https://artemis.xyz",
      "https://*.artemis.xyz",
      "https://zoracle.xyz",
      "https://*.zoracle.xyz",
      "https://www.zoracle.xyz"
    ],
    formAction: ["'self'"],
    upgradeInsecureRequests: []
  }
});

/**
 * Helmet configuration with comprehensive security headers
 */
export const helmetConfig = helmet({
  frameguard: false, // Disable X-Frame-Options to allow Replit Preview embedding
  crossOriginEmbedderPolicy: false, // Allow iframe embedding
  crossOriginResourcePolicy: false, // Allow cross-origin resources
  contentSecurityPolicy: {
    useDefaults: true,
    directives: {
      ...helmet.contentSecurityPolicy.getDefaultDirectives(),
      // Allow embedding by Replit preview (permissive for development)
      'frame-ancestors': ["'self'", "*"],
      // Keep other necessary directives for functionality
      'script-src': ["'self'", "'unsafe-inline'", "'unsafe-eval'", "https:"],
      'style-src': ["'self'", "'unsafe-inline'", "https:"],
      'img-src': ["'self'", "data:", "blob:", "https:", "http:"],
      'connect-src': ["'self'", "https:", "wss:", "ws:"],
      'frame-src': ["'self'", "https:"],
      'object-src': ["'none'"],
      'media-src': ["'self'", "https:", "data:"],
      'font-src': ["'self'", "https:", "data:"]
    }
  },
  referrerPolicy: {
    policy: ['strict-origin-when-cross-origin']
  }
});

/**
 * Rate limiting configuration
 */
export const apiRateLimit = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per windowMs
  message: {
    error: 'Too many requests from this IP, please try again later.',
    retryAfter: '15 minutes'
  },
  standardHeaders: true,
  legacyHeaders: false,
  handler: (req: Request, res: Response) => {
    res.status(429).json({
      error: 'Rate limit exceeded',
      message: 'Too many requests from this IP, please try again later.',
      retryAfter: '15 minutes'
    });
  }
});

/**
 * Strict rate limiting for sensitive endpoints
 */
export const strictRateLimit = rateLimit({
  windowMs: 5 * 60 * 1000, // 5 minutes
  max: 10, // Limit each IP to 10 requests per 5 minutes
  message: {
    error: 'Too many requests to sensitive endpoint',
    retryAfter: '5 minutes'
  }
});

/**
 * CORS configuration for static assets (more permissive)
 */
export const staticAssetCorsConfig = cors({
  origin: '*', // Allow all origins for static assets
  methods: ['GET', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Accept', 'Origin', 'Cache-Control', 'If-Modified-Since', 'If-None-Match'],
  exposedHeaders: ['Content-Length', 'Content-Type', 'Last-Modified', 'ETag'],
  maxAge: 86400, // 24 hours
  preflightContinue: false,
  optionsSuccessStatus: 200
});

/**
 * CORS configuration for API routes (more restrictive)
 */
export const corsConfig = cors({
  origin: function(origin, callback) {
    // Always allow requests with no origin (mobile apps, curl, etc.)
    if (!origin) return callback(null, true);
    
    // In development, be more permissive
    if (process.env.NODE_ENV === 'development') {
      return callback(null, true);
    }
    
    // Allow localhost for development
    if (origin.includes('localhost') || origin.includes('127.0.0.1')) {
      return callback(null, true);
    }
    
    // Allow Replit domains
    if (origin.includes('.replit.dev') || origin.includes('.replit.app') || origin.includes('.replit.co')) {
      return callback(null, true);
    }
    
    // Allow custom domain for CryptoHippo
    if (origin.includes('cryptohippo.locker')) {
      return callback(null, true);
    }
    
    // Allow other common development origins
    if (origin.includes('vite') || origin === 'null') {
      return callback(null, true);
    }
    
    // Block all other origins in production only
    if (process.env.NODE_ENV === 'production') {
      callback(new Error('Not allowed by CORS'));
    } else {
      callback(null, true);
    }
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept', 'Origin', 'Cache-Control'],
  exposedHeaders: ['Content-Length', 'Content-Type'],
  maxAge: 86400, // 24 hours
  preflightContinue: false,
  optionsSuccessStatus: 200
});

/**
 * Input sanitization middleware
 */
export const sanitizeInput = (req: Request, res: Response, next: NextFunction) => {
  // Sanitize body parameters
  if (req.body && typeof req.body === 'object') {
    for (const key in req.body) {
      if (typeof req.body[key] === 'string') {
        // Use DOMPurify to sanitize HTML content
        req.body[key] = purify.sanitize(req.body[key], {
          ALLOWED_TAGS: [],
          ALLOWED_ATTR: []
        });
        
        // Additional XSS protection
        req.body[key] = xss(req.body[key], {
          whiteList: {},
          stripIgnoreTag: true,
          stripIgnoreTagBody: ['script']
        });
      }
    }
  }
  
  // Sanitize query parameters
  if (req.query && typeof req.query === 'object') {
    for (const key in req.query) {
      if (typeof req.query[key] === 'string') {
        req.query[key] = purify.sanitize(req.query[key] as string, {
          ALLOWED_TAGS: [],
          ALLOWED_ATTR: []
        });
        
        req.query[key] = xss(req.query[key] as string, {
          whiteList: {},
          stripIgnoreTag: true,
          stripIgnoreTagBody: ['script']
        });
      }
    }
  }
  
  // Sanitize URL parameters
  if (req.params && typeof req.params === 'object') {
    for (const key in req.params) {
      if (typeof req.params[key] === 'string') {
        req.params[key] = purify.sanitize(req.params[key], {
          ALLOWED_TAGS: [],
          ALLOWED_ATTR: []
        });
        
        req.params[key] = xss(req.params[key], {
          whiteList: {},
          stripIgnoreTag: true,
          stripIgnoreTagBody: ['script']
        });
      }
    }
  }
  
  next();
};

/**
 * Validation error handler
 */
export const handleValidationErrors = (req: Request, res: Response, next: NextFunction) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({
      error: 'Validation failed',
      details: errors.array().map(error => ({
        field: error.type === 'field' ? (error as any).path : 'unknown',
        message: error.msg,
        value: error.type === 'field' ? (error as any).value : undefined
      }))
    });
  }
  next();
};

/**
 * Common validation schemas
 */
export const validateWalletAddress = body('walletAddress')
  .isLength({ min: 40, max: 42 })
  .matches(/^0x[a-fA-F0-9]{40}$/)
  .withMessage('Invalid Ethereum wallet address format');

export const validateUserId = body('userId')
  .isInt({ min: 1 })
  .withMessage('User ID must be a positive integer');

export const validatePortfolioId = body('portfolioId')
  .isInt({ min: 1 })
  .withMessage('Portfolio ID must be a positive integer');

/**
 * Security headers middleware
 */
export const securityHeaders = (req: Request, res: Response, next: NextFunction) => {
  // Additional security headers
  res.setHeader('X-Content-Type-Options', 'nosniff');
  // Remove X-Frame-Options to allow legitimate iframes
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains; preload');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');
  
  // Remove server information
  res.removeHeader('X-Powered-By');
  
  next();
};

/**
 * Error handling middleware
 */
export const errorHandler = (error: any, req: Request, res: Response, next: NextFunction) => {
  // Log error for monitoring (but don't expose sensitive details)
  console.error('Security Error:', {
    message: error.message,
    url: req.url,
    method: req.method,
    ip: req.ip,
    userAgent: req.get('User-Agent'),
    timestamp: new Date().toISOString()
  });
  
  // Generic error response (don't expose internal details)
  res.status(error.status || 500).json({
    error: 'An error occurred',
    message: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
  });
};