import { useState } from "react";
import { Activity, BarChart3, TrendingUp, ChartLine, Brain, Zap, DollarSign, Building2, Layers, Coins, ChevronRight, ChevronDown, ChevronLeft, Wallet, Users, MessageSquare, Rocket, Globe, ArrowLeftRight, Search, Menu, X, Gamepad2, Gem, CalendarDays, Settings, Info, Newspaper, ScrollText } from "lucide-react";
import { useLocation } from "wouter";
import caelynLogo from "@assets/ChatGPT_Image_Feb_20,_2026,_01_10_21_AM_1771571543846.png";
import { SettingsModal } from "@/pages/settings";
import { HistoryPanel } from "@/components/HistoryPanel";

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
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);

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
      label: 'Terminal',
      icon: <Brain className="w-4 h-4" />,
      path: '/app/caelyn-ai'
    },
    {
      id: 'notifai',
      label: 'NotifAI',
      icon: <Newspaper className="w-4 h-4" />,
      path: '/app/notifai'
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
          id: 'options-flow',
          label: 'Options Flow',
          icon: <Zap className="w-4 h-4" />,
          path: '/app/options'
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
                ? "bg-white/[0.06] border-l-2 border-[hsl(200,90%,58%)] text-white"
                : "text-white/45 hover:bg-white/[0.04] hover:text-white/80"
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
                ? "bg-white/[0.06] border-l-2 border-[hsl(200,90%,58%)] text-white shadow-md"
                : "text-white/45 hover:bg-white/[0.04] hover:text-white/80"
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
        <div className="fixed top-0 left-0 right-0 z-50 lg:hidden flex items-center justify-between px-4 h-14 border-b border-white/[0.06]" style={{ background: '#060709' }}>
          <div className="flex items-center gap-3">
            <img src={caelynLogo} alt="CaelynAI" className="w-10 h-10 rounded-full" />
            <span style={{ fontSize:'0.85rem', fontWeight:700, letterSpacing:'0.12em', textTransform:'uppercase', background:'linear-gradient(135deg, #e0f0ff 0%, #5cc8f0 40%, #3b9ee6 70%, #2080c8 100%)', WebkitBackgroundClip:'text', WebkitTextFillColor:'transparent', fontFamily:"'Inter', -apple-system, sans-serif" }}>CaelynAI</span>
          </div>
          <button
            onClick={onToggle}
            className="bg-white/[0.04] border border-white/[0.08] rounded-lg p-2.5 text-white/45 hover:text-white transition-all"
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
        } border-r border-white/[0.06] ${
          isMobile ? 'z-50' : 'z-40'
        } ${!isMobile ? 'transition-all duration-300 ease-in-out' : ''} flex flex-col ${className}`}
        style={{ background: '#060709' }}
        aria-hidden={isMobile && !isMobileMenuOpen}
        inert={isMobile && !isMobileMenuOpen ? true : undefined}
      >
        
        {/* Desktop Toggle Button — fixed y position, never moves */}
      {!isMobile && (
        <button
          onClick={onToggle}
          className="border border-white/[0.08] rounded-full p-1.5 text-white/40 hover:text-white hover:border-white/15 transition-all duration-200 shadow-lg"
          style={{ background: '#0a0b0f', position: 'fixed', top: 138, left: isCollapsed ? 52 : 180, zIndex: 50, transition: 'left 0.3s ease-in-out, border-color 0.2s, color 0.2s' }}
          data-testid="toggle-sidebar"
        >
          {isCollapsed ? (
            <ChevronRight className="w-4 h-4" />
          ) : (
            <ChevronLeft className="w-4 h-4" />
          )}
        </button>
      )}

      {/* Logo — collapses when sidebar is collapsed */}
      {!isMobile && (
      <div className="flex-shrink-0 flex flex-col items-center justify-center" style={{ width:'100%', borderBottom: isCollapsed ? 'none' : '1px solid rgba(255,255,255,0.06)' }}>
        <div className="overflow-hidden" style={{ transition: 'max-height 0.3s ease-in-out, opacity 0.25s ease-in-out, padding 0.3s ease-in-out', maxHeight: isCollapsed ? 0 : 200, opacity: isCollapsed ? 0 : 1, padding: isCollapsed ? '0' : '4px', width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <img
            src={caelynLogo}
            alt="CaelynAI"
            style={{ width: '100%', height: 'auto', objectFit: 'contain', display: 'block' }}
            data-testid="logo-cryptohippo"
          />
        </div>
      </div>
      )}

      {/* Navigation Items - Scrollable Area */}
      <div className={`flex-1 min-h-0 overflow-y-auto pl-2 pr-3 py-4 space-y-1 scrollbar-thin scrollbar-thumb-crypto-silver/20 scrollbar-track-transparent ${isMobile ? 'pt-16' : ''}`}>
        {navItems.map(item => renderNavItem(item))}
      </div>

      {/* Pinned Bottom: Settings + About */}
      <div className="flex-shrink-0 border-t border-white/[0.06] px-2 py-2 space-y-0.5">
        <button
          onClick={() => setSettingsOpen(true)}
          title={isCollapsed ? "Settings" : undefined}
          className={`w-full flex items-center ${isCollapsed ? 'justify-center' : 'gap-2'} px-2 ${isMobile ? 'py-3' : 'py-1.5'} text-left text-xs font-medium transition-all duration-200 rounded-lg text-white/45 hover:bg-white/[0.04] hover:text-white/80`}
          data-testid="nav-settings"
        >
          <Settings className="w-4 h-4" />
          {!isCollapsed && <span>Settings</span>}
        </button>
        <button
          onClick={() => setHistoryOpen(true)}
          title={isCollapsed ? "History" : undefined}
          className={`w-full flex items-center ${isCollapsed ? 'justify-center' : 'gap-2'} px-2 ${isMobile ? 'py-3' : 'py-1.5'} text-left text-xs font-medium transition-all duration-200 rounded-lg text-white/45 hover:bg-white/[0.04] hover:text-white/80`}
          data-testid="nav-history"
        >
          <ScrollText className="w-4 h-4" />
          {!isCollapsed && <span>History</span>}
        </button>
        <button
          onClick={() => navigateTo('/app/about')}
          title={isCollapsed ? "About" : undefined}
          className={`w-full flex items-center ${isCollapsed ? 'justify-center' : 'gap-2'} px-2 ${isMobile ? 'py-3' : 'py-1.5'} text-left text-xs font-medium transition-all duration-200 rounded-lg ${
            isActive('/app/about')
              ? "bg-white/[0.06] border-l-2 border-[hsl(200,90%,58%)] text-white shadow-md"
              : "text-white/45 hover:bg-white/[0.04] hover:text-white/80"
          }`}
          data-testid="nav-about"
        >
          <Info className="w-4 h-4" />
          {!isCollapsed && <span>About</span>}
        </button>
      </div>

      {/* Footer */}
      {(!isCollapsed || isMobile) && (
        <div className="flex-shrink-0 px-4 py-2 border-t border-white/[0.06]">
          <div className="text-[10px] text-center text-white/20">
            © 2026 CaelynAI
          </div>
        </div>
      )}
    </div>

    {/* Settings Modal */}
    <SettingsModal isOpen={settingsOpen} onClose={() => setSettingsOpen(false)} />
    {/* History Panel */}
    <HistoryPanel isOpen={historyOpen} onClose={() => setHistoryOpen(false)} />
    </>
  );
}