import { z } from 'zod';

/**
 * Environment variable validation schema
 */
const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.string().transform(Number).pipe(z.number().min(1).max(65535)).default('3000'),
  
  // Database
  DATABASE_URL: z.string().url('DATABASE_URL must be a valid URL'),
  
  // Security
  JWT_SECRET: z.string().min(32, 'JWT_SECRET must be at least 32 characters'),
  SESSION_SECRET: z.string().min(32, 'SESSION_SECRET must be at least 32 characters'),
  
  // API Keys
  ANTHROPIC_API_KEY: z.string().optional(),
  BASESCAN_API_KEY: z.string().optional(),
  ETHERSCAN_API_KEY: z.string().optional(),
  TAOSTATS_API_KEY: z.string().optional(),
  
  // Optional settings
  JWT_EXPIRES_IN: z.string().default('24h'),
  REFRESH_TOKEN_EXPIRES_IN: z.string().default('7d'),
  RATE_LIMIT_WINDOW_MS: z.string().transform(Number).pipe(z.number().positive()).default('900000'), // 15 minutes
  RATE_LIMIT_MAX_REQUESTS: z.string().transform(Number).pipe(z.number().positive()).default('100'),
  
  // HTTPS settings
  FORCE_HTTPS: z.string().transform(val => val === 'true').default('false'),
  HSTS_MAX_AGE: z.string().transform(Number).pipe(z.number().positive()).default('31536000'), // 1 year
  
  // CORS settings
  ALLOWED_ORIGINS: z.string().optional().transform(val => val ? val.split(',') : []),
  
  // Security headers
  CSP_REPORT_URI: z.string().url().optional(),
  CSP_REPORT_ONLY: z.string().transform(val => val === 'true').default('false'),
});

/**
 * Validated environment variables
 */
export type Environment = z.infer<typeof envSchema>;

/**
 * Validate and parse environment variables
 */
export function validateEnvironment(): Environment {
  try {
    return envSchema.parse(process.env);
  } catch (error) {
    if (error instanceof z.ZodError) {
      const missingVars = error.errors.map(err => {
        const path = err.path.join('.');
        return `${path}: ${err.message}`;
      });
      
      console.error('❌ Environment validation failed:');
      missingVars.forEach(err => console.error(`  - ${err}`));
      
      if (process.env.NODE_ENV === 'production') {
        console.error('\n🔴 Critical: Missing required environment variables in production!');
        console.error('Please set the following environment variables:');
        missingVars.forEach(err => console.error(`  ${err}`));
        process.exit(1);
      } else {
        console.warn('\n⚠️  Warning: Some environment variables are missing or invalid.');
        console.warn('Using default values for development. Set these for production:');
        missingVars.forEach(err => console.warn(`  ${err}`));
      }
    }
    
    // Return partial environment for development
    return {
      NODE_ENV: 'development',
      PORT: 3000,
      DATABASE_URL: process.env.DATABASE_URL || '',
      JWT_SECRET: process.env.JWT_SECRET || 'dev-jwt-secret-change-in-production-min-32-chars',
      SESSION_SECRET: process.env.SESSION_SECRET || 'dev-session-secret-change-in-production-min-32-chars',
      JWT_EXPIRES_IN: '24h',
      REFRESH_TOKEN_EXPIRES_IN: '7d',
      RATE_LIMIT_WINDOW_MS: 900000,
      RATE_LIMIT_MAX_REQUESTS: 100,
      FORCE_HTTPS: false,
      HSTS_MAX_AGE: 31536000,
      ALLOWED_ORIGINS: [],
      CSP_REPORT_ONLY: false,
      ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY,
      BASESCAN_API_KEY: process.env.BASESCAN_API_KEY,
      ETHERSCAN_API_KEY: process.env.ETHERSCAN_API_KEY,
      TAOSTATS_API_KEY: process.env.TAOSTATS_API_KEY,
      CSP_REPORT_URI: process.env.CSP_REPORT_URI
    } as Environment;
  }
}

/**
 * Get environment configuration
 */
export const env = validateEnvironment();

/**
 * Check if running in production
 */
export const isProduction = env.NODE_ENV === 'production';

/**
 * Check if running in development
 */
export const isDevelopment = env.NODE_ENV === 'development';

/**
 * Security configuration based on environment
 */
export const securityConfig = {
  // Force HTTPS in production
  forceHttps: isProduction || env.FORCE_HTTPS,
  
  // HSTS settings
  hstsMaxAge: env.HSTS_MAX_AGE,
  
  // Rate limiting
  rateLimitWindowMs: env.RATE_LIMIT_WINDOW_MS,
  rateLimitMaxRequests: env.RATE_LIMIT_MAX_REQUESTS,
  
  // CORS
  corsOrigins: env.ALLOWED_ORIGINS.length > 0 ? env.ALLOWED_ORIGINS : (
    isDevelopment ? ['http://localhost:3000', 'http://127.0.0.1:3000'] : []
  ),
  
  // CSP
  cspReportUri: env.CSP_REPORT_URI,
  cspReportOnly: env.CSP_REPORT_ONLY,
  
  // JWT
  jwtSecret: env.JWT_SECRET,
  jwtExpiresIn: env.JWT_EXPIRES_IN,
  refreshTokenExpiresIn: env.REFRESH_TOKEN_EXPIRES_IN,
  
  // Session
  sessionSecret: env.SESSION_SECRET,
  
  // API Keys (don't log these)
  hasAnthropicKey: !!env.ANTHROPIC_API_KEY,
  hasBasescanKey: !!env.BASESCAN_API_KEY,
  hasEtherscanKey: !!env.ETHERSCAN_API_KEY,
  hasTaoStatsKey: !!env.TAOSTATS_API_KEY
};

/**
 * Log security configuration (without secrets)
 */
export function logSecurityConfig() {
  console.log('🔒 Security Configuration:');
  console.log(`  Environment: ${env.NODE_ENV}`);
  console.log(`  Force HTTPS: ${securityConfig.forceHttps}`);
  console.log(`  HSTS Max Age: ${securityConfig.hstsMaxAge}s`);
  console.log(`  Rate Limit: ${securityConfig.rateLimitMaxRequests} requests per ${securityConfig.rateLimitWindowMs}ms`);
  console.log(`  CORS Origins: ${securityConfig.corsOrigins.length} configured`);
  console.log(`  CSP Report Only: ${securityConfig.cspReportOnly}`);
  console.log(`  JWT Expires: ${securityConfig.jwtExpiresIn}`);
  console.log(`  API Keys Available:`);
  console.log(`    - Anthropic: ${securityConfig.hasAnthropicKey ? '✅' : '❌'}`);
  console.log(`    - Basescan: ${securityConfig.hasBasescanKey ? '✅' : '❌'}`);
  console.log(`    - Etherscan: ${securityConfig.hasEtherscanKey ? '✅' : '❌'}`);
  console.log(`    - TaoStats: ${securityConfig.hasTaoStatsKey ? '✅' : '❌'}`);
}