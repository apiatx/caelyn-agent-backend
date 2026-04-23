"""
Sector stock scan — curated sector→stocks mapping with live price enrichment.

Three role categories per sector:
  momentum_leaders   — trend leaders / strongest movers in the sector right now
  bottleneck_enablers — picks-and-shovels, infrastructure, enabling names
  anchor_giants      — large-cap market-defining names (Serenity-consistent)

Prices are enriched via the existing Tradier batch quote infrastructure.
If Tradier is unavailable, stocks are returned with price=None (no fake data).
"""
from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from services.sector_rotation.schemas import SectorStock, SectorStockGroup

# ── Curated sector stock universe ─────────────────────────────────────────────
# Format: { etf: { "sector_name": str, role: [(ticker, company_name, reason)] } }
# Anchor giants deliberately overlap with Serenity giant_map.py's known anchors.

_RAW: Dict[str, Dict] = {
    "XLK": {
        "sector_name": "Technology",
        "momentum_leaders": [
            ("NVDA",  "NVIDIA",            "AI GPU cycle — dominant data center revenue inflection"),
            ("AVGO",  "Broadcom",          "Custom XPU TAM + networking silicon; hyperscaler spend proxy"),
            ("ORCL",  "Oracle",            "Cloud revenue accelerating; AI/GPU cluster deals"),
            ("ANET",  "Arista Networks",   "AI networking infrastructure; data center switching"),
            ("AMD",   "Advanced Micro Devices", "GPU/CPU gaining share; MI300X ramp"),
        ],
        "bottleneck_enablers": [
            ("AMAT",  "Applied Materials", "Wafer fab equipment; gating factor for advanced node capacity"),
            ("LRCX",  "Lam Research",      "Etch/deposition; critical for HBM and CoWoS packaging"),
            ("KLAC",  "KLA Corp",          "Process control metrology; yield-gate for leading nodes"),
            ("MRVL",  "Marvell Technology","Custom silicon for AI inference; optical DSP for DCI"),
            ("CDNS",  "Cadence Design",    "EDA software; every chip design flows through Cadence"),
        ],
        "anchor_giants": [
            ("AAPL",  "Apple",             "Largest cap; iPhone ecosystem; Apple Silicon in-house"),
            ("MSFT",  "Microsoft",         "Azure AI cloud + OpenAI + Copilot; $80B capex 2025"),
            ("NVDA",  "NVIDIA",            "AI GPU platform; H100/B200 cycle anchors data center capex"),
            ("GOOGL", "Alphabet",          "AI-native cloud; TPU custom silicon; DeepMind"),
            ("META",  "Meta Platforms",    "Llama open-source AI + custom MTIA silicon; massive capex"),
        ],
    },
    "XLF": {
        "sector_name": "Financials",
        "momentum_leaders": [
            ("GS",    "Goldman Sachs",     "Dealmaking revival; equity underwriting + advisory"),
            ("JPM",   "JPMorgan Chase",    "Net interest margin strength; credit card revenue"),
            ("MS",    "Morgan Stanley",    "Wealth management + investment banking recovery"),
            ("BX",    "Blackstone",        "Private credit growth; real assets AUM expansion"),
            ("KKR",   "KKR",              "Private credit + infrastructure momentum"),
        ],
        "bottleneck_enablers": [
            ("V",     "Visa",             "Payment rails — every consumer/business transaction flows through"),
            ("MA",    "Mastercard",       "Cross-border payment network; resilient fee income"),
            ("ICE",   "Intercontinental Exchange", "Market infrastructure; NYSE + mortgage tech"),
            ("MSCI",  "MSCI",            "Index/analytics backbone; every ETF and fund pays MSCI"),
            ("SPGI",  "S&P Global",      "Credit ratings + data; chokepoint for bond issuance"),
        ],
        "anchor_giants": [
            ("JPM",   "JPMorgan Chase",   "Largest US bank; rate sensitivity + trading revenue"),
            ("BAC",   "Bank of America",  "Consumer + commercial banking; rate-sensitive NIM"),
            ("V",     "Visa",            "Payment network duopoly; durable fee stream"),
            ("MA",    "Mastercard",      "Global payment network; cross-border travel recovery"),
            ("BRK-B", "Berkshire Hathaway","Value anchor; insurance float + operating businesses"),
        ],
    },
    "XLE": {
        "sector_name": "Energy",
        "momentum_leaders": [
            ("XOM",   "ExxonMobil",        "Upstream + refining; Pioneer acquisition integration"),
            ("CVX",   "Chevron",           "Permian + LNG; strong free cash flow"),
            ("OKE",   "ONEOK",            "Midstream gas gathering + processing; Magellan merger synergies"),
            ("VLO",   "Valero Energy",     "Refining margins; crack spread leverage"),
            ("COP",   "ConocoPhillips",    "Low-cost Permian Basin; Marathon acquisition"),
        ],
        "bottleneck_enablers": [
            ("SLB",   "SLB (Schlumberger)","Oilfield services; technology + digital; global drilling capex"),
            ("HAL",   "Halliburton",       "Completion services; North America fracking activity"),
            ("BKR",   "Baker Hughes",      "LNG equipment + subsea; industrial energy tech"),
            ("CIVI",  "Civitas Resources", "DJ Basin + Permian; capital-efficient E&P"),
            ("FANG",  "Diamondback Energy","Low-cost Permian operator; Double Eagle acquisition"),
        ],
        "anchor_giants": [
            ("XOM",   "ExxonMobil",       "Largest US energy company; vertically integrated"),
            ("CVX",   "Chevron",          "Diversified global energy; reliable dividend"),
            ("EOG",   "EOG Resources",    "Premium return E&P; Permian + Eagle Ford"),
            ("COP",   "ConocoPhillips",   "Low-cost global E&P; scale after Marathon deal"),
            ("PSX",   "Phillips 66",      "Refining + chemicals + midstream; integrated model"),
        ],
    },
    "XLI": {
        "sector_name": "Industrials",
        "momentum_leaders": [
            ("GEV",   "GE Vernova",        "Power generation; grid infrastructure; energy transition play"),
            ("RTX",   "RTX Corp",          "Defense + aerospace; GTF engine recovery + PATRIOT"),
            ("GE",    "GE Aerospace",      "Jet engine aftermarket; LEAP/GEnx service revenue"),
            ("CAT",   "Caterpillar",       "Construction + mining equipment; data center build-out demand"),
            ("EMR",   "Emerson Electric",  "Automation + process control; industrial software"),
        ],
        "bottleneck_enablers": [
            ("ITW",   "Illinois Tool Works","Diversified manufacturer; segment margin expansion model"),
            ("PH",    "Parker Hannifin",   "Motion and control; aerospace + industrial filtration"),
            ("ROK",   "Rockwell Automation","Factory automation + digital twin; reshoring beneficiary"),
            ("IR",    "Ingersoll Rand",    "Compressed air + fluid management; industrial lifecycle"),
            ("TT",    "Trane Technologies","HVAC + datacenter cooling; electrification demand"),
        ],
        "anchor_giants": [
            ("GE",    "GE Aerospace",     "Largest aero engine platform; aftermarket engine"),
            ("HON",   "Honeywell",        "Diversified industrial + aerospace; building automation"),
            ("RTX",   "RTX Corp",         "Defense + commercial aerospace prime"),
            ("CAT",   "Caterpillar",      "Construction machinery; global infrastructure proxy"),
            ("UPS",   "United Parcel Service", "Global logistics; e-commerce + B2B freight"),
        ],
    },
    "XLV": {
        "sector_name": "Health Care",
        "momentum_leaders": [
            ("LLY",   "Eli Lilly",        "GLP-1 (Mounjaro/Zepbound) volume ramp; obesity TAM"),
            ("NVO",   "Novo Nordisk",      "Ozempic/Wegovy global demand; cardiovascular data"),
            ("ABBV",  "AbbVie",           "Skyrizi/Rinvoq replacing Humira; derm + immunology"),
            ("ELV",   "Elevance Health",  "Managed care; MA + Medicaid enrollment mix"),
            ("ISRG",  "Intuitive Surgical","Robotic surgery; da Vinci 5 launch; procedure volume"),
        ],
        "bottleneck_enablers": [
            ("DXCM",  "Dexcom",           "CGM — real-time glucose; GLP-1 patient companion device"),
            ("IDXX",  "IDEXX Laboratories","Veterinary diagnostics; recurring reagent revenue"),
            ("VEEV",  "Veeva Systems",     "Life sciences CRM/data; FDA-regulated cloud infrastructure"),
            ("RMD",   "ResMed",           "Sleep apnea devices + software; GLP-1 displaced patients return"),
            ("HOLX",  "Hologic",          "Women's health diagnostics; Surgical + GYN"),
        ],
        "anchor_giants": [
            ("LLY",   "Eli Lilly",        "GLP-1 blockbuster; fastest-growing mega-cap drug company"),
            ("UNH",   "UnitedHealth Group","Largest managed care + Optum services; scale moat"),
            ("JNJ",   "Johnson & Johnson", "MedTech + pharma diversification; defensive quality"),
            ("ABBV",  "AbbVie",           "Immunology pipeline; replacing Humira loss of exclusivity"),
            ("MRK",   "Merck",            "Keytruda oncology; Gardasil vaccine; animal health"),
        ],
    },
    "XLC": {
        "sector_name": "Communication Services",
        "momentum_leaders": [
            ("META",  "Meta Platforms",   "AI-driven ad targeting; Reels + WhatsApp monetization"),
            ("GOOGL", "Alphabet",         "Search + YouTube + cloud; AI Overviews monetization"),
            ("NFLX",  "Netflix",          "Ad-tier subscriber growth; live events strategy"),
            ("SPOT",  "Spotify",          "Podcast + audiobook + pricing power monetization"),
            ("TTD",   "The Trade Desk",   "Programmatic ad buy-side; CTV spending shift"),
        ],
        "bottleneck_enablers": [
            ("DIS",   "Walt Disney",      "Streaming + parks; ESPN direct-to-consumer transition"),
            ("PINS",  "Pinterest",        "Visual discovery + AI shopping; ARPU expansion"),
            ("ROKU",  "Roku",            "CTV OS platform; streaming advertising aggregator"),
            ("ZG",    "Zillow",          "Real estate marketplace; AI-driven listing + rental"),
            ("MTCH",  "Match Group",      "Dating apps portfolio; pricing + payer ARPU recovery"),
        ],
        "anchor_giants": [
            ("META",  "Meta Platforms",   "Social media ad duopoly; 3.3B+ daily users"),
            ("GOOGL", "Alphabet",         "Search monopoly + YouTube; AI defensive moat"),
            ("NFLX",  "Netflix",          "Streaming market share leader; ad tier scaling"),
            ("DIS",   "Walt Disney",      "Content IP + parks + streaming; brand moat"),
            ("T",     "AT&T",            "Wireless + fiber; dividend yield + deleveraging"),
        ],
    },
    "XLY": {
        "sector_name": "Consumer Discretionary",
        "momentum_leaders": [
            ("AMZN",  "Amazon",           "AWS + retail + advertising; fastest EBIT margin expansion"),
            ("TSLA",  "Tesla",            "EV volume recovery + FSD/robotaxi + energy storage"),
            ("RCL",   "Royal Caribbean",  "Cruise demand; record bookings + pricing"),
            ("BKNG",  "Booking Holdings", "Travel demand; European hotel + flight growth"),
            ("NKE",   "Nike",            "Brand reset; DTC recovery + China rebound thesis"),
        ],
        "bottleneck_enablers": [
            ("HD",    "Home Depot",       "Housing remodel; professional contractor demand recovery"),
            ("LOW",   "Lowe's",           "Home improvement retail; spring selling season"),
            ("POOL",  "Pool Corp",        "Outdoor living + pool maintenance; seasonal recovery"),
            ("ULTA",  "Ulta Beauty",      "Beauty retail + loyalty program; prestige trade-down"),
            ("TPR",   "Tapestry",         "Affordable luxury + Capri merger thesis"),
        ],
        "anchor_giants": [
            ("AMZN",  "Amazon",           "E-commerce + AWS; consumer + cloud + ad flywheel"),
            ("TSLA",  "Tesla",            "EV + energy + AI robotaxi; disruption anchor"),
            ("HD",    "Home Depot",       "Home improvement market leader; contractor backbone"),
            ("MCD",   "McDonald's",       "QSR dominant; value meal traffic + digital loyalty"),
            ("NKE",   "Nike",            "Global sports brand; DTC + licensing revenue"),
        ],
    },
    "XLP": {
        "sector_name": "Consumer Staples",
        "momentum_leaders": [
            ("WMT",   "Walmart",          "Advertising + membership growth; grocery market share gain"),
            ("COST",  "Costco",           "Membership renewal + volume; gold bar momentum"),
            ("PM",    "Philip Morris",    "IQOS heated tobacco international growth; ZYN nicotine"),
            ("MO",    "Altria",           "Domestic tobacco + on! nicotine pouch volume"),
            ("SFM",   "Sprouts Farmers",  "Natural/organic food momentum; health-conscious consumer"),
        ],
        "bottleneck_enablers": [
            ("PG",    "Procter & Gamble", "Consumer brands portfolio; pricing power + distribution moat"),
            ("KO",    "Coca-Cola",        "Beverage brand + global distribution; pricing architecture"),
            ("PEP",   "PepsiCo",          "Snacks + beverages; Frito-Lay volume recovery"),
            ("CL",    "Colgate-Palmolive","Oral care + personal care; emerging market mix"),
            ("CHD",   "Church & Dwight",  "ARM & Hammer + niche brands; organic growth"),
        ],
        "anchor_giants": [
            ("WMT",   "Walmart",          "Largest US retailer; grocery + omnichannel + ad platform"),
            ("PG",    "Procter & Gamble", "Global household brands; defensive free cash flow"),
            ("KO",    "Coca-Cola",        "Iconic global beverage; long-cycle dividend compounder"),
            ("PEP",   "PepsiCo",          "Snacks + beverages diversification; volume inflection"),
            ("COST",  "Costco",           "Membership retail; pricing loyalty + traffic moat"),
        ],
    },
    "XLB": {
        "sector_name": "Materials",
        "momentum_leaders": [
            ("FCX",   "Freeport-McMoRan", "Copper production; AI/EV/grid electrification demand"),
            ("NEM",   "Newmont",          "Gold mining; record gold price leverage"),
            ("CF",    "CF Industries",    "Nitrogen fertilizer; natural gas input leverage"),
            ("ALB",   "Albemarle",        "Lithium production; EV battery demand recovery"),
            ("MP",    "MP Materials",     "Rare earth mining + processing; US supply chain independence"),
        ],
        "bottleneck_enablers": [
            ("LIN",   "Linde",            "Industrial gases; semiconductor fab + green hydrogen"),
            ("APD",   "Air Products",     "Industrial gases + hydrogen energy transition"),
            ("EMN",   "Eastman Chemical", "Advanced materials; specialty polymer for auto/EV"),
            ("FMC",   "FMC Corp",        "Crop protection chemicals; agricultural input"),
            ("LTHM",  "Livent",          "Lithium chemicals for EV battery cathode"),
        ],
        "anchor_giants": [
            ("LIN",   "Linde",           "Largest industrial gas company; semiconductor fab critical"),
            ("FCX",   "Freeport-McMoRan","Largest US copper producer; EV + grid infrastructure anchor"),
            ("NUE",   "Nucor",           "US steel leader; EV + construction + reshoring"),
            ("APD",   "Air Products",    "Hydrogen + industrial gases; energy transition infrastructure"),
            ("DOW",   "Dow",            "Specialty chemicals + polyethylene; packaging + materials"),
        ],
    },
    "XLRE": {
        "sector_name": "Real Estate",
        "momentum_leaders": [
            ("EQIX",  "Equinix",          "Data center REIT; AI compute colocation demand surge"),
            ("DLR",   "Digital Realty",   "Data center + cloud connectivity; hyperscaler leasing"),
            ("WELL",  "Welltower",        "Senior housing; aging demographics + occupancy recovery"),
            ("AMT",   "American Tower",   "Cell tower REIT; 5G densification lease escalators"),
            ("PLD",   "Prologis",         "Industrial logistics REIT; e-commerce + nearshoring"),
        ],
        "bottleneck_enablers": [
            ("IRM",   "Iron Mountain",    "Physical + digital records + data center colocation"),
            ("COLD",  "Americold Realty", "Cold storage REIT; food supply chain infrastructure"),
            ("SUI",   "Sun Communities", "Manufactured housing + RV parks; affordable housing"),
            ("CUBE",  "CubeSmart",       "Self-storage; urban density + life-event demand"),
            ("REXR",  "Rexford Industrial","Southern California industrial; near-port logistics"),
        ],
        "anchor_giants": [
            ("PLD",   "Prologis",         "Global logistics real estate; e-commerce anchor"),
            ("AMT",   "American Tower",   "Largest US cell tower REIT; global wireless infrastructure"),
            ("EQIX",  "Equinix",          "Data center colocation global leader; AI demand"),
            ("DLR",   "Digital Realty",   "Hyperscaler data center partner; cloud + AI leasing"),
            ("WELL",  "Welltower",        "Senior housing; healthcare real estate scale"),
        ],
    },
    "XLU": {
        "sector_name": "Utilities",
        "momentum_leaders": [
            ("CEG",   "Constellation Energy","Nuclear power; AI data center clean energy PPAs"),
            ("VST",   "Vistra Energy",    "Power generation + retail; AI datacenter demand + ERCOT"),
            ("GEV",   "GE Vernova",       "Power generation equipment; gas turbine backlog"),
            ("SO",    "Southern Company", "Nuclear (Vogtle) + gas; rate base growth in the South"),
            ("AEE",   "Ameren",          "Midwest electric + gas; data center load growth"),
        ],
        "bottleneck_enablers": [
            ("NEE",   "NextEra Energy",   "Renewable energy developer; wind + solar pipeline"),
            ("AES",   "AES Corp",        "Renewable energy + battery storage; data center PPAs"),
            ("PCG",   "PG&E",            "California electric utility; grid hardening + wildfire capex"),
            ("EXC",   "Exelon",          "Nuclear + regulated distribution; mid-Atlantic / midwest"),
            ("ETR",   "Entergy",         "Nuclear + gas; Gulf Coast industrial load growth"),
        ],
        "anchor_giants": [
            ("NEE",   "NextEra Energy",   "Largest US utility; wind + solar + nuclear + FPL"),
            ("SO",    "Southern Company", "Integrated utility; Vogtle nuclear online"),
            ("DUK",   "Duke Energy",      "Largest regulated utility; 8-state franchise territory"),
            ("D",     "Dominion Energy",  "Virginia + Carolinas utility; offshore wind + rate base"),
            ("CEG",   "Constellation Energy","Nuclear fleet owner; AI data center clean-power leader"),
        ],
    },
}


def _tv_symbol(ticker: str, sector_etf: str) -> str:
    """Best-effort TradingView symbol. Most US stocks are NASDAQ or NYSE."""
    _NYSE = {"JPM", "GS", "MS", "BAC", "XOM", "CVX", "COP", "EOG", "PSX", "VLO",
             "OKE", "SLB", "HAL", "BKR", "LLY", "UNH", "JNJ", "ABBV", "MRK",
             "GE", "GEV", "RTX", "HON", "CAT", "UPS", "ITW", "PH", "TT",
             "DIS", "T", "MO", "KO", "PG", "PEP", "CL", "FCX", "NEM", "LIN",
             "APD", "NUE", "DOW", "AMT", "PLD", "EQIX", "DLR", "WELL",
             "IRM", "NEE", "SO", "DUK", "D", "EXC", "PCG", "WMT", "HD", "LOW",
             "MCD", "V", "MA", "BX", "KKR", "SPGI", "ICE", "BRK-B"}
    exchange = "NYSE" if ticker in _NYSE else "NASDAQ"
    return f"{exchange}:{ticker.replace('-', '.')}"


def _market_cap_label(ticker: str) -> str:
    """Rough market-cap bucket. Mega-cap roster is stable; everything else is Large by default."""
    _MEGA = {"AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "BRK-B", "LLY",
             "XOM", "JPM", "V", "UNH", "WMT", "JNJ", "AVGO", "MA", "PG", "HD", "CVX",
             "MRK", "ABBV", "COST", "KO", "PEP", "BAC", "NVO"}
    _MID  = {"DXCM", "IDXX", "VEEV", "RMD", "HOLX", "PINS", "ROKU", "MTCH", "ZG",
             "POOL", "TPR", "SFM", "CHD", "ALB", "MP", "LTHM", "FMC", "COLD",
             "CUBE", "REXR", "SUI", "AEE", "ETR", "CIVI", "FANG"}
    if ticker in _MEGA:
        return "Mega"
    if ticker in _MID:
        return "Mid"
    return "Large"


async def _enrich_with_quotes(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch real-time quotes for a batch of stock tickers using the existing
    Tradier batch-quote infrastructure (same provider used for sector ETFs).
    Returns {ticker: {price, change_1d_pct}} — empty dict on failure.
    """
    try:
        from services.sector_rotation.providers import _tradier_quotes_batch
        return await _tradier_quotes_batch(tickers)
    except Exception as e:
        print(f"[SR_STOCKS] Quote enrichment error (non-fatal): {e}")
        return {}


def _build_group(etf: str, quotes: dict[str, dict]) -> SectorStockGroup:
    """Build a SectorStockGroup from the curated map + live quote data."""
    raw = _RAW.get(etf, {})
    sector_name = raw.get("sector_name", etf)

    def make_stocks(role: str) -> list[SectorStock]:
        entries = raw.get(role, [])
        out = []
        for ticker, company_name, reason in entries:
            q = quotes.get(ticker, {})
            out.append(SectorStock(
                ticker=ticker,
                company_name=company_name,
                sector_etf=etf,
                sector_name=sector_name,
                role=role.replace("_", " ").split(" ")[0],  # "momentum" | "bottleneck" | "anchor"
                reason_for_inclusion=reason,
                price=q.get("price"),
                change_1d_pct=q.get("change_1d_pct"),
                market_cap_label=_market_cap_label(ticker),
                tv_symbol=_tv_symbol(ticker, etf),
            ))
        return out

    return SectorStockGroup(
        etf=etf,
        sector_name=sector_name,
        momentum_leaders=make_stocks("momentum_leaders"),
        bottleneck_enablers=make_stocks("bottleneck_enablers"),
        anchor_giants=make_stocks("anchor_giants"),
    )


async def get_sector_stocks(etfs: list[str]) -> list[SectorStockGroup]:
    """
    Return stock groups for the requested sector ETFs, enriched with live prices.

    Args:
        etfs: e.g. ["XLK", "XLF"] — the winning sectors to scan

    Returns:
        One SectorStockGroup per ETF in the requested order.
    """
    # Collect all tickers that need enrichment (unique across all requested sectors)
    all_tickers: list[str] = []
    seen: set[str] = set()
    for etf in etfs:
        for role in ("momentum_leaders", "bottleneck_enablers", "anchor_giants"):
            for ticker, _, _ in _RAW.get(etf, {}).get(role, []):
                if ticker not in seen:
                    all_tickers.append(ticker)
                    seen.add(ticker)

    quotes = await _enrich_with_quotes(all_tickers) if all_tickers else {}

    return [_build_group(etf, quotes) for etf in etfs if etf in _RAW]


def build_top_stocks_list(
    groups: list[SectorStockGroup],
    limit: int = 10,
) -> list[dict]:
    """
    Flatten SectorStockGroup list into a prioritised list of SectorStock dicts
    suitable for injection into AIAnalysis.top_stocks_to_watch.

    Priority order within each group:
      1. momentum_leaders   (most immediately actionable)
      2. bottleneck_enablers
      3. anchor_giants

    Across groups the top-ranked winning sector comes first.
    Deduplicates by ticker; returns up to `limit` entries.
    """
    seen: set[str] = set()
    result: list[dict] = []

    for group in groups:
        for stock in (group.momentum_leaders + group.bottleneck_enablers + group.anchor_giants):
            if stock.ticker in seen:
                continue
            seen.add(stock.ticker)
            result.append(stock.model_dump())
            if len(result) >= limit:
                return result

    return result


def build_ranked_top_stocks(
    groups: list[SectorStockGroup],
    limit: int = 15,
) -> list["SectorStock"]:
    """
    Build a flat, live-signal-ranked list of SectorStock objects from one or
    more sector groups.  This is the canonical stock list for the Sectors page
    and is computed independently of agent analysis.

    Ranking rules:
      momentum_leaders   — sorted by change_1d_pct descending (live momentum);
                           stocks with no price data fall to the end of the group.
      bottleneck_enablers — kept in curated structural order (supply-chain
                           criticality is not a daily-price signal).
      anchor_giants       — kept in curated order (market-cap proxy already baked in).

    Groups from higher-ranked winning sectors appear before lower-ranked ones.
    Deduplicates by ticker across all groups.
    """
    seen: set[str] = set()
    result: list[SectorStock] = []

    def _add(stock: SectorStock) -> None:
        if stock.ticker in seen or len(result) >= limit:
            return
        seen.add(stock.ticker)
        result.append(stock)

    for group in groups:
        # momentum: rank by 1-day change descending; None prices go last
        ranked_momentum = sorted(
            group.momentum_leaders,
            key=lambda s: (s.change_1d_pct is not None, s.change_1d_pct or 0.0),
            reverse=True,
        )
        for s in ranked_momentum:
            _add(s)

        # bottleneck: curated structural order preserved
        for s in group.bottleneck_enablers:
            _add(s)

        # anchor: curated order preserved (market-cap-weighted curation)
        for s in group.anchor_giants:
            _add(s)

    return result
