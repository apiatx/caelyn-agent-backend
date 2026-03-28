import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import { startBackgroundServices, stopBackgroundServices } from "./background-services";
import { realTimePriceService } from "./real-time-price-service";

// Security imports
import { 
  helmetConfig, 
  cspConfig, 
  apiRateLimit, 
  corsConfig,
  staticAssetCorsConfig, 
  sanitizeInput, 
  securityHeaders,
  errorHandler 
} from "./security/middleware";
import { env, logSecurityConfig, isProduction } from "./security/environment";

const app = express();

// Configure trust proxy for rate limiting accuracy
app.set('trust proxy', true);

// CRITICAL: Deployment health check endpoint - MUST respond with 200 immediately
// Also serves as user redirect to the frontend
app.get("/", (req, res, next) => {
  const userAgent = req.get('User-Agent') || '';
  const acceptHeader = req.get('Accept') || '';
  const host = req.get('Host') || '';
  
  // Check if this is a browser request (has text/html in Accept header)
  if (acceptHeader.includes('text/html') && userAgent.includes('Mozilla')) {
    // For custom domains like cryptohippo.locker, always serve the frontend directly
    // This ensures domain functionality works correctly
    if (host.includes('cryptohippo.locker') || host.includes('.replit.app') || host.includes('.replit.dev')) {
      // Let this fall through to the catch-all route that serves the frontend
      return next();
    } else {
      // Redirect browsers to the frontend application for other domains
      return res.redirect(302, '/app');
    }
  }
  
  // For deployment health checks and monitoring tools
  res.status(200).json({ status: "ok", service: "crypto-intelligence-platform" });
});

app.get("/deployment-health", (req, res) => {
  res.status(200).send("OK");
});

app.get("/health", (req, res) => {
  res.status(200).json({ 
    status: "healthy", 
    timestamp: new Date().toISOString(),
    service: "crypto-intelligence-platform",
    uptime: process.uptime()
  });
});

app.get("/ready", (req, res) => {
  res.status(200).send("OK");
});

// Log security configuration
logSecurityConfig();

// Apply static asset CORS for JS, CSS, images, fonts, etc.
app.use((req, res, next) => {
  const staticAssetExtensions = ['.js', '.css', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.woff', '.woff2', '.ttf', '.eot', '.map'];
  const isStaticAsset = staticAssetExtensions.some(ext => req.path.toLowerCase().endsWith(ext)) || 
                       req.path.startsWith('/assets/') || 
                       req.path.startsWith('/static/') ||
                       req.path.startsWith('/@vite/') ||
                       req.path.includes('.vite');
  
  if (isStaticAsset) {
    return staticAssetCorsConfig(req, res, next);
  } else {
    return corsConfig(req, res, next);
  }
});

// Apply security middleware FIRST (CSP disabled for investing.com)
app.use(securityHeaders);
app.use(helmetConfig);
// app.use(cspConfig); // Disabled to allow investing.com iframe

// Force HTTPS in production
if (isProduction && env.FORCE_HTTPS) {
  app.use((req: Request, res: Response, next: NextFunction) => {
    if (req.header('x-forwarded-proto') !== 'https') {
      res.redirect(301, `https://${req.header('host')}${req.url}`);
    } else {
      next();
    }
  });
}

// Rate limiting for API routes
app.use('/api', apiRateLimit);

// Body parsing with size limits
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: false, limit: '10mb' }));

// Input sanitization
app.use(sanitizeInput);

app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;
  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }

      if (logLine.length > 80) {
        logLine = logLine.slice(0, 79) + "…";
      }

      log(logLine);
    }
  });

  next();
});

(async () => {
  const server = await registerRoutes(app);

  // Use security error handler
  app.use(errorHandler);

  // importantly only setup vite in development and after
  // setting up all the other routes so the catch-all route
  // doesn't interfere with the other routes
  if (app.get("env") === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }

  // ALWAYS serve the app on the port specified in the environment variable PORT
  // Other ports are firewalled. Default to 5000 if not specified.
  // this serves both the API and the client.
  // It is the only port that is not firewalled.
  const port = parseInt(process.env.PORT || '5000', 10);
  
  server.listen({
    port,
    host: "0.0.0.0",
    reusePort: true,
  }, (error?: Error) => {
    if (error) {
      console.error('Failed to start server:', error);
      process.exit(1);
    }
    
    log(`serving on port ${port}`);
    log(`Server is ready and healthy`);
    
    // Show the user the actual working URL
    const replicDevDomain = process.env.REPLIT_DEV_DOMAIN;
    const replSlug = process.env.REPL_SLUG;
    const replOwner = process.env.REPL_OWNER;
    
    if (replicDevDomain) {
      log(`🌍 ACCESS YOUR APP HERE: https://${replicDevDomain}`);
    } else if (replSlug && replOwner) {
      log(`🌍 ACCESS YOUR APP HERE: https://${replSlug}.${replOwner}.repl.co`);
    }
    
    log(`Health check endpoints available at:`);
    log(`  - http://0.0.0.0:${port}/deployment-health (priority)`);
    log(`  - http://0.0.0.0:${port}/`);
    log(`  - http://0.0.0.0:${port}/health`);
    log(`  - http://0.0.0.0:${port}/ready`);
    log(`  - http://0.0.0.0:${port}/api/health`);
    log(`  - http://0.0.0.0:${port}/api/ready`);
    
    // Start background services after a delay to ensure server is ready
    setTimeout(() => {
      log(`Starting background services...`);
      try {
        startBackgroundServices();
        log(`Background services started successfully`);
      } catch (error) {
        console.error('Error starting background services:', error);
        // Don't exit - server can still function without background services
      }
    }, 1000);
  }).on('error', (err) => {
    console.error('Server failed to start:', err);
    process.exit(1);
  });

  // Add error handler for server
  server.on('error', (error: Error) => {
    console.error('Server error:', error);
    // In production, attempt to gracefully recover or restart
    if (isProduction) {
      console.error('Production server error detected, attempting graceful recovery...');
    }
  });

  // Set server timeout to prevent hanging requests during deployment health checks
  server.setTimeout(30000); // 30 second timeout
  
  // Keep alive timeout for load balancers
  server.keepAliveTimeout = 65000; // 65 seconds
  server.headersTimeout = 66000; // 66 seconds

  // Graceful shutdown
  process.on('SIGTERM', () => {
    stopBackgroundServices();
    process.exit(0);
  });

  process.on('SIGINT', () => {
    stopBackgroundServices();
    process.exit(0);
  });
})();
