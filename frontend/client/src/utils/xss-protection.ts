/**
 * Client-side XSS protection utilities
 */

/**
 * Escape HTML characters to prevent XSS
 */
export function escapeHtml(text: string): string {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

/**
 * Strip HTML tags from user input
 */
export function stripHtml(html: string): string {
  const div = document.createElement('div');
  div.innerHTML = html;
  return div.textContent || div.innerText || '';
}

/**
 * Sanitize user input for display
 */
export function sanitizeUserInput(input: string): string {
  if (!input || typeof input !== 'string') {
    return '';
  }
  
  // Remove script tags and event handlers
  let sanitized = input
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
    .replace(/on\w+\s*=\s*["'][^"']*["']/gi, '')
    .replace(/on\w+\s*=\s*[^"'\s>]*/gi, '')
    .replace(/javascript:/gi, '')
    .replace(/vbscript:/gi, '')
    .replace(/data:/gi, '');
  
  return sanitized;
}

/**
 * Validate URL to prevent malicious redirects
 */
export function isValidUrl(url: string): boolean {
  try {
    const urlObj = new URL(url);
    
    // Only allow http and https protocols
    if (!['http:', 'https:'].includes(urlObj.protocol)) {
      return false;
    }
    
    // Block dangerous hosts
    const dangerousHosts = [
      'localhost',
      '127.0.0.1',
      '0.0.0.0',
      '[::]',
      '::1'
    ];
    
    if (dangerousHosts.includes(urlObj.hostname.toLowerCase())) {
      return false;
    }
    
    return true;
  } catch {
    return false;
  }
}

/**
 * Sanitize form data before submission
 */
export function sanitizeFormData(formData: Record<string, any>): Record<string, any> {
  const sanitized: Record<string, any> = {};
  
  for (const [key, value] of Object.entries(formData)) {
    if (typeof value === 'string') {
      sanitized[key] = sanitizeUserInput(value);
    } else if (Array.isArray(value)) {
      sanitized[key] = value.map(item => 
        typeof item === 'string' ? sanitizeUserInput(item) : item
      );
    } else {
      sanitized[key] = value;
    }
  }
  
  return sanitized;
}

/**
 * Content Security Policy nonce generator
 */
export function generateNonce(): string {
  const array = new Uint8Array(16);
  crypto.getRandomValues(array);
  return btoa(String.fromCharCode.apply(null, Array.from(array)));
}

/**
 * Safe JSON parse that handles potential XSS in JSON strings
 */
export function safeJsonParse<T>(jsonString: string): T | null {
  try {
    // First sanitize the JSON string
    const sanitized = sanitizeUserInput(jsonString);
    return JSON.parse(sanitized);
  } catch {
    return null;
  }
}