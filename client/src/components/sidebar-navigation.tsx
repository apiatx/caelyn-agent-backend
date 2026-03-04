import { useState } from "react";
import { Activity, BarChart3, TrendingUp, ChartLine, Brain, Zap, DollarSign, Building2, Layers, Coins, ChevronRight, ChevronDown, ChevronLeft, Wallet, Users, MessageSquare, Rocket, Globe, ArrowLeftRight, Search, Menu, X, Gamepad2, Gem, CalendarDays } from "lucide-react";
import { useLocation } from "wouter";
import caelynLogo from "@assets/ChatGPT_Image_Feb_20,_2026,_01_10_21_AM_1771571543846.png";

interface SidebarNavigationProps {
  className?: string;
  isCollapsed: boolean;
  isMobile?: boolean;
  isMobileMenuOpen?: boolean;
  onToggle: () => void;
  onCloseMobile?: () => void;
}

interface NavItem {
  id: string;
  label: string;
  icon: React.ReactNode;
  path?: string;
  children?: NavItem[];
}

export function SidebarNavigation({ className = "", isCollapsed, isMobile = false, isMobileMenuOpen = false, onToggle, onCloseMobile }: SidebarNavigationProps) {
  const [location, setLocation] = useLocation();
  const [expandedItems, setExpandedItems] = useState<string[]>(['crypto-stocks']);

  const navigateTo = (url: string) => {
    setLocation(url);
    // Close mobile menu when navigating
    if (isMobile && onCloseMobile) {
      onCloseMobile();
    }
  };

  const isActive = (path: string) => {
    const currentPath = location.replace(/^\/+/, '').replace(/\/+$/, '');
    if (path === '/app' || path === '/') {
      return currentPath === '' || currentPath === 'app';
    }
    return currentPath === path.replace(/^\/+/, '').replace(/\/+$/, '');
  };

  const toggleExpanded = (itemId: string) => {
    setExpandedItems(prev => 
      prev.includes(itemId) 
        ? prev.filter(id => id !== itemId)
        : [...prev, itemId]
    );
  };

  const navItems: NavItem[] = [
    {
      id: 'caelyn-ai',
      label: 'CaelynAI',
      icon: <Brain className="w-4 h-4" />,
      path: '/app/caelyn-ai'
    },
    {
      id: 'macro-dashboard',
      label: 'Macro Dashboard',
      icon: <Activity className="w-4 h-4" />,
      path: '/app/stocks/dashboard'
    },
    {
      id: 'stocks-portfolio',
      label: 'Portfolio Dashboard',
      icon: <Wallet className="w-4 h-4" />,
      path: '/app/stocks/portfolio'
    },
    {
      id: 'crypto-stocks',
      label: 'Stocks',
      icon: <TrendingUp className="w-4 h-4" />,
      children: [
        {
          id: 'stocks-screening',
          label: 'Screening',
          icon: <Search className="w-4 h-4" />,
          path: '/app/stocks/screening'
        },
        {
          id: 'stocks-earnings-calendar',
          label: 'Earnings',
          icon: <CalendarDays className="w-4 h-4" />,
          path: '/app/stocks/earnings-calendar'
        },
        {
          id: 'stocks-sectors',
          label: 'Sectors + ETFs',
          icon: <Layers className="w-4 h-4" />,
          path: '/app/stocks/sectors'
        },
        {
          id: 'stocks-fundamentals',
          label: 'Fundamentals',
          icon: <TrendingUp className="w-4 h-4" />,
          path: '/app/stocks/fundamentals'
        }
      ]
    },
    {
      id: 'crypto',
      label: 'Crypto',
      icon: <Coins className="w-4 h-4" />,
      children: [
        {
          id: 'dashboard',
          label: 'Market Overview',
          icon: <Activity className="w-4 h-4" />,
          path: '/app/market-overview'
        },
        {
          id: 'onchain-analytics',
          label: 'Screening',
          icon: <TrendingUp className="w-4 h-4" />,
          path: '/app/onchain/analytics'
        },
        {
          id: 'onchain-analyze',
          label: 'Analyze',
          icon: <Brain className="w-4 h-4" />,
          path: '/app/onchain/analyze'
        },
        {
          id: 'charts',
          label: 'Charts',
          icon: <BarChart3 className="w-4 h-4" />,
          children: [
            {
              id: 'majors',
              label: 'Majors',
              icon: <BarChart3 className="w-4 h-4" />,
              path: '/app/charts/majors'
            },
            {
              id: 'altcoins',
              label: 'Altcoins',
              icon: <Coins className="w-4 h-4" />,
              path: '/app/charts/altcoins'
            }
          ]
        },
        {
          id: 'trade',
          label: 'Trade',
          icon: <TrendingUp className="w-4 h-4" />,
          children: [
            {
              id: 'trade-swidge',
              label: 'Swidge',
              icon: <ArrowLeftRight className="w-4 h-4" />,
              path: '/app/trade'
            },
            {
              id: 'trade-perps',
              label: 'Perps',
              icon: <TrendingUp className="w-4 h-4" />,
              path: '/app/trade/perps'
            },
            {
              id: 'trade-spot-terminals',
              label: 'Spot Terminals',
              icon: <BarChart3 className="w-4 h-4" />,
              path: '/app/trade/spot-terminals'
            },
            {
              id: 'trade-options',
              label: 'Options',
              icon: <Zap className="w-4 h-4" />,
              path: '/app/trade/options'
            },
            {
              id: 'trade-onramp',
              label: 'On Ramp',
              icon: <Wallet className="w-4 h-4" />,
              path: '/app/trade/onramp'
            }
          ]
        },
        {
          id: 'defi',
          label: 'Earn',
          icon: <DollarSign className="w-4 h-4" />,
          children: [
            {
              id: 'defi-overview',
              label: 'DeFi',
              icon: <DollarSign className="w-4 h-4" />,
              path: '/app/defi'
            },
            {
              id: 'btc-defi',
              label: 'BTC Fi',
              icon: <Coins className="w-4 h-4" />,
              path: '/app/defi/btc-defi'
            },
            {
              id: 'defai',
              label: 'DeFAI',
              icon: <Brain className="w-4 h-4" />,
              path: '/app/defi/defai'
            },
            {
              id: 'depin',
              label: 'DePIN',
              icon: <Zap className="w-4 h-4" />,
              path: '/app/defi/depin'
            },
            {
              id: 'p2e',
              label: 'P2E',
              icon: <Gamepad2 className="w-4 h-4" />,
              path: '/app/p2e'
            }
          ]
        },
        {
          id: 'ecosystems',
          label: 'Ecosystems',
          icon: <Layers className="w-4 h-4" />,
          children: [
            {
              id: 'ethereum',
              label: 'ETH',
              icon: <Coins className="w-4 h-4" />,
              path: '/app/ethereum'
            },
            {
              id: 'base',
              label: 'Base',
              icon: <ChartLine className="w-4 h-4" />,
              path: '/app/base'
            },
            {
              id: 'solana',
              label: 'Solana',
              icon: <Zap className="w-4 h-4" />,
              path: '/app/solana'
            },
            {
              id: 'bittensor',
              label: 'Bittensor',
              icon: <Brain className="w-4 h-4" />,
              path: '/app/bittensor'
            },
            {
              id: 'bnb',
              label: 'BNB',
              icon: <Coins className="w-4 h-4" />,
              path: '/app/bnb'
            },
            {
              id: 'sui',
              label: 'SUI',
              icon: <Zap className="w-4 h-4" />,
              path: '/app/sui'
            },
            {
              id: 'arbitrum',
              label: 'Arbitrum',
              icon: <Layers className="w-4 h-4" />,
              path: '/app/arbitrum'
            },
          ]
        },
        {
          id: 'onchain',
          label: 'Onchain',
          icon: <BarChart3 className="w-4 h-4" />,
          children: [
            {
              id: 'onchain-smart-wallets',
              label: 'Smart Wallets',
              icon: <Wallet className="w-4 h-4" />,
              path: '/app/onchain/smart-wallets'
            },
            {
              id: 'onchain-launchpad',
              label: 'Launchpad',
              icon: <Rocket className="w-4 h-4" />,
              path: '/app/onchain/launchpad'
            },
            {
              id: 'onchain-airdrop',
              label: 'Airdrop',
              icon: <Zap className="w-4 h-4" />,
              path: '/app/onchain/airdrop'
            },
            {
              id: 'onchain-memes',
              label: 'Memes',
              icon: <Coins className="w-4 h-4" />,
              path: '/app/onchain/memes'
            },
            {
              id: 'onchain-discover',
              label: 'Discover Web3',
              icon: <Globe className="w-4 h-4" />,
              path: '/app/onchain/discover'
            }
          ]
        },
        {
          id: 'crypto-stonks',
          label: 'Treasuries',
          icon: <Building2 className="w-4 h-4" />,
          path: '/app/crypto-stonks'
        },
        {
          id: 'portfolio',
          label: 'Portfolio',
          icon: <Activity className="w-4 h-4" />,
          path: '/app/portfolio'
        },
      ]
    },
    {
      id: 'commodities',
      label: 'Commodities',
      icon: <Gem className="w-4 h-4" />,
      path: '/app/commodities'
    },
    {
      id: 'rwa',
      label: 'Tokenization',
      icon: <Building2 className="w-4 h-4" />,
      path: '/app/rwa'
    },
    {
      id: 'predict',
      label: 'Predict',
      icon: <TrendingUp className="w-4 h-4" />,
      path: '/app/predict'
    },
    {
      id: 'onchain-social',
      label: 'Social',
      icon: <MessageSquare className="w-4 h-4" />,
      path: '/app/onchain/social'
    },
    {
      id: 'about',
      label: 'About',
      icon: <Activity className="w-4 h-4" />,
      path: '/app/about'
    }
  ];

  const hasActiveDescendant = (item: NavItem): boolean => {
    if (!item.children) return false;
    return item.children.some(child => 
      (child.path && isActive(child.path)) || hasActiveDescendant(child)
    );
  };

  const renderNavItem = (item: NavItem, level: number = 0) => {
    const hasChildren = item.children && item.children.length > 0;
    const isExpanded = !isCollapsed && expandedItems.includes(item.id);
    const itemIsActive = item.path ? isActive(item.path) : false;
    const hasActiveChild = hasActiveDescendant(item);
    const indent = !isCollapsed && level > 0 ? level * 12 : 0;

    return (
      <div key={item.id} className="w-full">
        {hasChildren ? (
          <button
            onClick={() => !isCollapsed && toggleExpanded(item.id)}
            title={isCollapsed ? item.label : undefined}
            style={indent > 0 ? { marginLeft: indent } : undefined}
            className={`w-full flex items-center ${isCollapsed ? 'justify-center' : 'justify-between'} px-2 ${isMobile ? 'py-3' : 'py-1.5'} text-left text-xs font-medium transition-all duration-200 rounded-lg group ${
              hasActiveChild
                ? "bg-gradient-to-r from-crypto-warning/20 to-yellow-400/10 border-l-2 border-crypto-warning text-white"
                : "text-gray-300 hover:bg-white/5 hover:text-white"
            }`}
            data-testid={`nav-${item.id}`}
          >
            <div className={`flex items-center ${isCollapsed ? 'justify-center' : 'gap-3'}`}>
              {item.icon}
              {!isCollapsed && <span>{item.label}</span>}
            </div>
            {!isCollapsed && (
              <span className="shrink-0 ml-1">
                {isExpanded ? (
                  <ChevronDown className="w-3.5 h-3.5 transition-transform duration-200" />
                ) : (
                  <ChevronRight className="w-3.5 h-3.5 transition-transform duration-200" />
                )}
              </span>
            )}
          </button>
        ) : (
          <button
            onClick={() => item.path && navigateTo(item.path)}
            title={isCollapsed ? item.label : undefined}
            style={indent > 0 ? { marginLeft: indent } : undefined}
            className={`w-full flex items-center ${isCollapsed ? 'justify-center' : 'gap-2'} px-2 ${isMobile ? 'py-3' : 'py-1.5'} text-left text-xs font-medium transition-all duration-200 rounded-lg group ${
              itemIsActive
                ? "bg-gradient-to-r from-crypto-warning/20 to-yellow-400/10 border-l-2 border-crypto-warning text-white shadow-md"
                : "text-gray-300 hover:bg-white/5 hover:text-white"
            }`}
            data-testid={`nav-${item.id}`}
          >
            {item.icon}
            {!isCollapsed && <span>{item.label}</span>}
          </button>
        )}

        {hasChildren && isExpanded && !isCollapsed && (
          <div className="mt-0.5 space-y-0.5">
            {item.children!.map(child => renderNavItem(child, level + 1))}
          </div>
        )}
      </div>
    );
  };

  return (
    <>
      {isMobile && (
        <div className="fixed top-0 left-0 right-0 z-50 lg:hidden flex items-center justify-between px-4 h-14 bg-black/95 backdrop-blur-lg border-b border-crypto-silver/20">
          <div className="flex items-center gap-3">
            <img src={caelynLogo} alt="CaelynAI" className="w-10 h-10 rounded-full" />
            <span style={{ fontSize:'0.9rem', fontWeight:800, letterSpacing:'0.1em', textTransform:'uppercase', background:'linear-gradient(135deg, #e8eaef 0%, #a78bfa 40%, #3b82f6 70%, #22c55e 100%)', WebkitBackgroundClip:'text', WebkitTextFillColor:'transparent', fontFamily:"'JetBrains Mono', 'SF Mono', 'Fira Code', monospace" }}>TradeBlade</span>
          </div>
          <button
            onClick={onToggle}
            className="bg-white/5 border border-crypto-silver/20 rounded-lg p-2.5 text-gray-300 hover:text-white transition-all"
            aria-label={isMobileMenuOpen ? "Close menu" : "Open menu"}
          >
            {isMobileMenuOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
          </button>
        </div>
      )}
      
      <div 
        id="mobile-navigation-menu"
        className={`fixed top-0 h-full ${
          isMobile 
            ? `right-0 w-full transform transition-transform duration-300 ease-in-out ${
                isMobileMenuOpen ? 'translate-x-0' : 'translate-x-full'
              }` 
            : `left-0 ${isCollapsed ? 'w-16' : 'w-48'}`
        } bg-black/95 backdrop-blur-lg border-r border-crypto-silver/20 ${
          isMobile ? 'z-50' : 'z-40'
        } ${!isMobile ? 'transition-all duration-300 ease-in-out' : ''} flex flex-col ${className}`}
        aria-hidden={isMobile && !isMobileMenuOpen}
        inert={isMobile && !isMobileMenuOpen ? true : undefined}
      >
        
        {/* Desktop Toggle Button */}

      {!isMobile && (
      <div className="flex-shrink-0 border-b border-crypto-silver/20 flex items-center justify-center relative" style={{ width:'100%', padding: isCollapsed ? '6px' : '4px' }}>
        <div className="overflow-hidden" style={{ transition: 'all 0.3s ease-in-out', width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <img 
            src={caelynLogo}
            alt="CaelynAI"
            style={{ width: '100%', height: 'auto', objectFit: 'contain', display: 'block' }}
            data-testid="logo-cryptohippo"
          />
        </div>
        <div className="absolute -right-3 -bottom-3 z-50">
          <button
            onClick={onToggle}
            className="bg-black/90 backdrop-blur-lg border border-crypto-silver/20 rounded-full p-1.5 text-gray-300 hover:text-white hover:bg-white/5 transition-all duration-200 shadow-lg"
            data-testid="toggle-sidebar"
          >
            {isCollapsed ? (
              <ChevronRight className="w-4 h-4" />
            ) : (
              <ChevronLeft className="w-4 h-4" />
            )}
          </button>
        </div>
      </div>
      )}

      {/* Navigation Items - Scrollable Area */}
      <div className={`flex-1 min-h-0 overflow-y-auto pl-2 pr-3 py-4 space-y-1 scrollbar-thin scrollbar-thumb-crypto-silver/20 scrollbar-track-transparent ${isMobile ? 'pt-16' : ''}`}>
        {navItems.map(item => renderNavItem(item))}
      </div>

      {/* Footer */}
      {(!isCollapsed || isMobile) && (
        <div className="flex-shrink-0 p-4 border-t border-crypto-silver/20">
          <div className="text-xs text-center" style={{ color: '#cce8ff' }}>
            © 2026 CaelynAI
          </div>
        </div>
      )}
    </div>
    </>
  );
}