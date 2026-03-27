// Mobile-optimized external link utilities for iOS app deep linking

/**
 * Detects if the user is on a mobile device
 */
export const isMobile = (): boolean => {
  return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
};

/**
 * Detects if the user is on iOS
 */
export const isIOS = (): boolean => {
  return /iPad|iPhone|iPod/.test(navigator.userAgent);
};

/**
 * Opens CoinMarketCap links - uses app deep link on mobile when available
 */
export const openCoinMarketCapLink = (path: string): void => {
  if (isIOS()) {
    // Try to open in CoinMarketCap app first, fallback to web
    const appUrl = `coinmarketcap://${path}`;
    const webUrl = `https://coinmarketcap.com/${path}`;
    
    // Attempt to open app, fallback to web after timeout
    window.location.href = appUrl;
    setTimeout(() => {
      window.open(webUrl, '_blank');
    }, 1000);
  } else {
    // Open in web browser for non-iOS devices
    window.open(`https://coinmarketcap.com/${path}`, '_blank');
  }
};

/**
 * Opens DexScreener links - optimized for mobile viewing
 */
export const openDexScreenerLink = (path: string): void => {
  const url = `https://dexscreener.com/${path}`;
  
  if (isMobile()) {
    // Open in same tab on mobile for better UX (DexScreener doesn't have a native app)
    window.open(url, '_blank');
  } else {
    window.open(url, '_blank');
  }
};

/**
 * Opens CoinGecko links - uses app deep link on mobile when available
 */
export const openCoinGeckoLink = (path: string): void => {
  if (isIOS()) {
    // Try to open in CoinGecko app first, fallback to web
    const appUrl = `coingecko://${path}`;
    const webUrl = `https://coingecko.com/${path}`;
    
    // Attempt to open app, fallback to web after timeout
    window.location.href = appUrl;
    setTimeout(() => {
      window.open(webUrl, '_blank');
    }, 1000);
  } else {
    window.open(`https://coingecko.com/${path}`, '_blank');
  }
};

/**
 * Opens TradingView links - uses app deep link on mobile when available
 */
export const openTradingViewLink = (path: string): void => {
  if (isIOS()) {
    // Try to open in TradingView app first, fallback to web
    const appUrl = `tradingview://${path}`;
    const webUrl = `https://tradingview.com/${path}`;
    
    // Attempt to open app, fallback to web after timeout
    window.location.href = appUrl;
    setTimeout(() => {
      window.open(webUrl, '_blank');
    }, 1000);
  } else {
    window.open(`https://tradingview.com/${path}`, '_blank');
  }
};

/**
 * Generic external link opener with mobile optimization
 */
export const openExternalLink = (url: string): void => {
  // Parse the URL to determine the service
  const urlObj = new URL(url);
  const hostname = urlObj.hostname.toLowerCase();
  
  if (hostname.includes('coinmarketcap.com')) {
    const path = urlObj.pathname + urlObj.search;
    openCoinMarketCapLink(path.startsWith('/') ? path.slice(1) : path);
  } else if (hostname.includes('dexscreener.com')) {
    const path = urlObj.pathname + urlObj.search;
    openDexScreenerLink(path.startsWith('/') ? path.slice(1) : path);
  } else if (hostname.includes('coingecko.com')) {
    const path = urlObj.pathname + urlObj.search;
    openCoinGeckoLink(path.startsWith('/') ? path.slice(1) : path);
  } else if (hostname.includes('tradingview.com')) {
    const path = urlObj.pathname + urlObj.search;
    openTradingViewLink(path.startsWith('/') ? path.slice(1) : path);
  } else {
    // For other external links, open normally
    window.open(url, '_blank');
  }
};