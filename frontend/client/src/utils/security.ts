/**
 * Security utilities for XSS protection and safe iframe handling
 */

// Comprehensive XSS-safe iframe sandbox attributes
export const SECURE_IFRAME_SANDBOX = "allow-same-origin allow-scripts allow-forms allow-popups allow-top-navigation-by-user-activation allow-presentation";

// Stricter sandbox for untrusted content
export const STRICT_IFRAME_SANDBOX = "allow-scripts allow-forms allow-popups";

// Content Security Policy for iframes
export const IFRAME_CSP = "default-src 'self'; script-src 'self' 'unsafe-inline' https:; style-src 'self' 'unsafe-inline' https:; img-src 'self' data: https:; connect-src 'self' https:; font-src 'self' https:; object-src 'none'; media-src 'self' https:; frame-ancestors 'none';";

// Trusted domains for our application
const TRUSTED_DOMAINS = [
  'dexscreener.com',
  'taostats.io',
  'bittensor.ai',
  'www.bittensor.ai',
  'swordscan.com',
  'taohub.info',
  'hyperliquid.xyz',
  'hyperfolio.xyz',
  'backprop.finance',
  'taomarketcap.com',
  'tradingview.com',
  'opensea.io',
  'checkr.social',
  'terminal.co',
  'www.terminal.co',
  'zoracle.xyz',
  'www.zoracle.xyz',
  'zora.co',
  'www.zora.co',
  'blockcreeper.com',
  'www.blockcreeper.com',
  'virtuals.io',
  'app.virtuals.io',
  'creator.bid',
  'bankr.bot',
  'peapods.finance',
  'app.hyperliquid.xyz',
  'hyperdash.info',
  'app.hyperswap.exchange',
  'liquidlaunch.app',
  'artemis.xyz',
  'app.artemisanalytics.com',
  'artemisanalytics.com',
  'nansen.ai',
  'app.nansen.ai',
  'chainlyze.ai',
  'app.chainlyze.ai',
  'dune.com',
  'tokenterminal.com',
  'chainspect.app',
  'x.com',
  'twitter.com',
  't.me',
  'telegram.me',
  'ethereum-ecosystem.com',
  'swissborg.com',
  'base.org',
  'gravity.xyz',
  'dappradar.com',
  'zapper.xyz',
  'wach.ai',
  'chatgpt.com',
  'chat.openai.com',
  'substack.com',
  'bubblemaps.io',
  'kolytics.com',
  'app.kolytics.pro',
  'www.alphabot.app',
  'alphabot.app',
  'kaito.ai',
  'cookie.fun',
  'mindshare.elfi.ai',
  'mindshare.elfi.io',
  'dexcheck.ai',
  'birdeye.so',
  'velvet.capital',
  'dapp.velvet.capital',
  'cointelegraph.com',
  'coinmarketcap.com',
  'coingecko.com',
  'www.coingecko.com',
  'jup.ag',
  'mobyscreener.com',
  'web3.okx.com',
  'app.relay.link',
  'aerodrome.finance',
  'app.uniswap.org',
  'polymarket.com',
  'pmx.trade',
  'kalshi.com',
  'www.kalshi.com',
  'app.elfa.ai',
  'elfa.ai',
  'ayaoracle.xyz',
  'indexy.xyz',
  'debank.com',
  'www.relay.link',
  'relay.link',
  'universalx.app',
  'defi.instadapp.io',
  'ave.ai',
  'o1.exchange',
  'app.swing.xyz',
  'macroedge.ai',
  'app.aave.com',
  'senpi.ai',
  'www.aixvc.io',
  'aixvc.io',
  'app.arma.xyz',
  'arma.xyz',
  'zyf.ai',
  'lido.fi',
  'app.coinmarketman.com',
  'coinmarketman.com',
  'velo.xyz',
  'simplywall.st',
  'client.schwab.com',
  'unusualwhales.com',
  'newhedge.io',
  'portal.abs.xyz',
  'abs.xyz',
  'pudgyinvest.com',
  'thenew.money',
  'stocktwits.com',
  'www.stocktwits.com',
  'app.intellectia.ai',
  'intellectia.ai',
  'investing.com',
  'www.investing.com',
  'quanto.trade',
  'perps.saros.xyz',
  'saros.xyz',
  'degenai.dev',
  'messari.io',
  // DeFi Platforms
  'tharwa.xyz',
  'app.tharwa.xyz',
  'www.tharwa.xyz',
  'harvest.finance',
  'app.harvest.finance',
  'www.harvest.finance',
  'stacks.co',
  'app.stacks.co',
  'www.stacks.co',
  'stacks.org',
  'app.stacks.org',
  'www.stacks.org',
  'satlayer.xyz',
  'app.satlayer.xyz',
  'www.satlayer.xyz',
  'satlayer.io',
  'app.satlayer.io',
  'www.satlayer.io',
  'perps.raydium.io',
  'raydium.io',
  'pro.edgex.exchange',
  'edgex.exchange',
  'trade.perpsdao.xyz',
  'perpsdao.xyz',
  'deri.io',
  'www.hegic.co',
  'hegic.co',
  'indexy.xyz',
  'www.tarot.to',
  'tarot.to',
  'arcadia.finance',
  'mamo.bot',
  'quant.fun',
  'www.ourbit.com',
  'ourbit.com',
  'limitless.exchange',
  'morpho.org',
  'www.ether.fi',
  'ether.fi',
  'wallet.coinbase.com',
  'app.teller.org',
  'app.seamlessprotocol.com',
  'app.lotusfinance.io',
  'app.spectra.finance',
  'www.theblock.co',
  'theblock.co',
  'l2beat.com',
  'www.l2beat.com',
  'growthepie.com',
  'www.growthepie.com',
  'coinglass.com',
  'www.coinglass.com',
  'buymeacoffee.com',
  'cloudbet.com',
  'www.cloudbet.com',
  'cryptoquant.com',
  'bitcoinmagazinepro.com',
  'www.bitcoinmagazinepro.com',
  'charts.bitbo.io',
  'bitbo.io',
  'taofi.com',
  'www.taofi.com',
  'tao.bot',
  'www.tao.bot',
  'bscscan.com',
  'pancakeswap.finance',
  'cetus.zone',
  'www.cetus.zone',
  'suivision.xyz',
  'hashscan.io',
  'hedera.com',
  'app.sonic.game',
  'bridge.sonic.game',
  'wallet.sonic.game',
  'games.sonic.game',
  'nft.sonic.game',
  'analytics.sonic.game',
  'binance.com',
  'www.binance.com',
  'novaex.com',
  'www.novaex.com',
  'app.extended.exchange',
  'extended.exchange',
  'app.defi.app',
  'defi.app',
  'jumper.exchange',
  'app.debridge.finance',
  'debridge.finance',
  'app.definitive.fi',
  'definitive.fi',
  'holdstation.com',
  'www.holdstation.com',
  'developerreport.com',
  'www.developerreport.com',
  'avantisfi.com',
  'www.avantisfi.com',
  'drift.trade',
  'app.drift.trade',
  'superp.xyz',
  'noliquidation.superp.xyz',
  'meme.superp.xyz',
  'coinmarketcal.com',
  'asterdex.com',
  'www.asterdex.com',
  'paradex.trade',
  'www.paradex.trade',
  'myx.finance',
  'app.myx.finance',
  'ranger.finance',
  'app.ranger.finance',
  'www.app.ranger.finance',
  'bouncebit.io',
  'portal.bouncebit.io',
  'blofin.com',
  'www.blofin.com',
  'dydx.trade',
  'www.dydx.trade',
  'app.gmx.io',
  'gmx.io',
  'matcha.xyz',
  'www.matcha.xyz',
  'app.dodoex.io',
  'dodoex.io',
  'www.dodoex.io',
  'app.xy.finance',
  'xy.finance',
  'www.xy.finance',
  'app.tabtrader.com',
  'tabtrader.com',
  'www.tabtrader.com',
  'app.rubic.exchange',
  'rubic.exchange',
  'www.rubic.exchange',
  'app.reservoir.xyz',
  'reservoir.xyz',
  'www.reservoir.xyz',
  'app.spark.fi',
  'spark.fi',
  'www.spark.fi',
  'kerneldao.com',
  'www.kerneldao.com',
  'fluid.instadapp.io',
  'defi.instadapp.io',
  'app.euler.finance',
  'euler.finance',
  'www.euler.finance',
  'app.tharwa.finance',
  'tharwa.finance',
  'www.tharwa.finance',
  'usda.avalonfinance.xyz',
  'avalonfinance.xyz',
  'www.avalonfinance.xyz',
  'lombard.finance',
  'www.lombard.finance',
  'app.lombard.finance',
  'apex.exchange',
  'app.apex.exchange',
  'www.apex.exchange',
  'platform.alphanomics.io',
  'alphanomics.io',
  'www.alphanomics.io',
  'coinbubbles.app',
  'www.coinbubbles.app',
  'stargate.finance',
  'www.stargate.finance',
  // DeFi Analytics
  'defillama.com',
  'www.defillama.com',
  'swap.defillama.com',
  'feed.defillama.com',
  'kinetiq.xyz',
  // News Sources
  'dlnews.com',
  'www.dlnews.com',
  'notifai.news',
  'www.notifai.news',
  // Social Platforms
  'x.com',
  // Cross-chain Swap Platforms
  'swap.mayan.finance',
  'mayan.finance',
  'www.mayan.finance',
  'oku.trade',
  'app.plasma.to',
  'plasma.to',
  'alchemy.com',
  'www.alchemy.com',
  'yield.fi',
  'www.yield.fi',
  'zkp2p.xyz',
  'www.zkp2p.xyz',
  'thena.fi',
  'www.thena.fi',
  'perps.thena.fi',
  'kamino.com',
  'www.kamino.com',
  'engine.aarna.ai',
  'app.gammaswap.com',
  'app.gearbox.fi',
  'app.pendle.finance',
  'boros.pendle.finance',
  'app.maple.finance',
  'betbase.xyz',
  'app.resolv.xyz',
  'app.initia.xyz',
  'mux.network',
  'app.mux.network',
  'web3.okx.com',
  'app.ethena.fi',
  'ethena.fi',
  'app.aevo.xyz',
  'aevo.xyz',
  'app.aark.digital',
  'aark.digital',
  'app.rwa.xyz',
  'rwa.xyz',
  'app.allo.xyz',
  'allo.xyz',
  'app.ondo.finance',
  'ondo.finance',
  'xstocks.com',
  'yield.goat.network',
  'goat.network',
  'speedmarkets.xyz',
  'overtimemarkets.xyz',
  'www.overtimemarkets.xyz',
  'trade.bluefin.io',
  'bluefin.io',
  'app.gearbox.fi',
  'gearbox.fi',
  'balancer.fi',
  'pinlink.ai',
  'impermax.finance',
  'app.impermax.finance',
  'www.impermax.finance',
  'escher.finance',
  'app.escher.finance',
  'www.escher.finance',
  'synthetix.io',
  '420.synthetix.io',
  'www.synthetix.io',
  'orderly.network',
  'app.orderly.network',
  'www.orderly.network',
  'huma.finance',
  'app.huma.finance',
  'www.huma.finance',
  'river.inc',
  'app.river.inc',
  'www.river.inc',
  'yala.org',
  'app.yala.org',
  'www.yala.org',
  'sky.money',
  'app.sky.money',
  'www.sky.money',
  'venus.io',
  'app.venus.io',
  'www.venus.io',
  'convexfinance.com',
  'www.convexfinance.com',
  'app.convexfinance.com',
  'solstice.finance',
  'app.solstice.finance',
  'www.solstice.finance',
  'stellaxyz.io',
  'app.stellaxyz.io',
  'www.stellaxyz.io',
  'arkm.com',
  'intel.arkm.com',
  'www.arkm.com',
  'lighter.xyz',
  'app.lighter.xyz',
  'www.lighter.xyz',
  'arbitrum.io',
  'portal.arbitrum.io',
  'www.arbitrum.io',
  'based.one',
  'app.based.one',
  'www.based.one',
  'vooi.io',
  'app.vooi.io',
  'www.vooi.io',
  'variational.io',
  'omni.variational.io',
  'www.variational.io',
  'ostium.com',
  'app.ostium.com',
  'www.ostium.com',
  'pacifica.fi',
  'app.pacifica.fi',
  'www.pacifica.fi',
  'taobridge.xyz',
  'www.taobridge.xyz',
  'tao.app',
  'www.tao.app',
  'taoyield.com',
  'www.taoyield.com'
];

/**
 * Validates if a URL is from a trusted domain
 */
export function isTrustedDomain(url: string): boolean {
  try {
    const urlObj = new URL(url);
    const hostname = urlObj.hostname.toLowerCase();
    
    return TRUSTED_DOMAINS.some(domain => 
      hostname === domain || hostname.endsWith('.' + domain)
    );
  } catch {
    return false;
  }
}

/**
 * Sanitizes a URL to prevent XSS attacks
 */
export function sanitizeUrl(url: string): string | null {
  try {
    const urlObj = new URL(url);
    
    // Only allow https and http protocols
    if (!['https:', 'http:'].includes(urlObj.protocol)) {
      console.warn(`Blocked non-HTTP(S) protocol: ${urlObj.protocol}`);
      return null;
    }
    
    // Check if domain is trusted
    if (!isTrustedDomain(url)) {
      console.warn(`Blocked untrusted domain: ${urlObj.hostname}`);
      return null;
    }
    
    return urlObj.toString();
  } catch (error) {
    console.error('Invalid URL format:', url, error);
    return null;
  }
}

/**
 * Secure wrapper for opening links in new tabs
 */
export function openSecureLink(url: string): void {
  const sanitizedUrl = sanitizeUrl(url);
  
  if (!sanitizedUrl) {
    console.error('Blocked potentially unsafe URL:', url);
    alert('This link has been blocked for security reasons.');
    return;
  }
  
  // Open with security attributes in new tab (not window)
  const newWindow = window.open(
    sanitizedUrl,
    '_blank'
  );
  
  // Additional security: ensure the new window can't access opener
  if (newWindow) {
    newWindow.opener = null;
  }
}

/**
 * Gets appropriate sandbox attributes based on domain trust level
 */
export function getIframeSandbox(url: string): string {
  const sanitizedUrl = sanitizeUrl(url);
  
  if (!sanitizedUrl) {
    return STRICT_IFRAME_SANDBOX;
  }
  
  // Use stricter sandbox for less trusted domains
  try {
    const urlObj = new URL(sanitizedUrl);
    const hostname = urlObj.hostname.toLowerCase();
    
    // More permissive for our core trusted platforms
    const coreTrustedDomains = [
      'dexscreener.com',
      'tradingview.com',
      'hyperliquid.xyz',
      'taostats.io'
    ];
    
    if (coreTrustedDomains.some(domain => 
      hostname === domain || hostname.endsWith('.' + domain)
    )) {
      return SECURE_IFRAME_SANDBOX;
    }
  } catch {
    // Invalid URL, use strict sandbox
  }
  
  return STRICT_IFRAME_SANDBOX;
}

/**
 * Common iframe security props
 */
export function getSecureIframeProps(src: string, title: string) {
  const sanitizedSrc = sanitizeUrl(src);
  
  if (!sanitizedSrc) {
    throw new Error(`Unsafe iframe source blocked: ${src}`);
  }
  
  return {
    src: sanitizedSrc,
    title,
    frameBorder: "0",
    loading: "lazy" as const,
    sandbox: getIframeSandbox(sanitizedSrc),
    referrerPolicy: "strict-origin-when-cross-origin" as const,
    allow: "clipboard-read; clipboard-write; encrypted-media; fullscreen; picture-in-picture"
  };
}

/**
 * Validates and sanitizes external link props
 */
export function getSecureLinkProps(href: string) {
  const sanitizedHref = sanitizeUrl(href);
  
  if (!sanitizedHref) {
    throw new Error(`Unsafe link blocked: ${href}`);
  }
  
  return {
    href: sanitizedHref,
    target: "_blank" as const,
    rel: "noopener noreferrer nofollow",
    onClick: (e: React.MouseEvent) => {
      e.preventDefault();
      openSecureLink(sanitizedHref);
    }
  };
}