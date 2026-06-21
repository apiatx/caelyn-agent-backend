# OpenAI Critical Supply-Chain Map — Public Companies Only

This is not an official OpenAI vendor ledger. OpenAI does not publish a full bill-of-materials. This is the public-market map of companies that either have disclosed OpenAI/Stargate exposure or sit in the scarce upstream bottlenecks that must feed OpenAI's compute, networking, power, cooling, and data-center stack.

The framework here is exactly Macro → Sector → Bottleneck: work backward from the end customer, identify the physical constraint, then score public companies by scarcity, directness, substitutability, and capacity leverage. That is the right lens for this problem, because the edge is not "AI is big." Everyone knows that. The edge is who owns the choke point the buildout cannot route around.

## Scoring key

95–100 = hard bottleneck
OpenAI cannot scale meaningfully without this layer.

85–94 = Tier 1 critical supplier/enabler
Direct or near-direct beneficiary with scarce capacity.

75–84 = Tier 2 major enabler
Important, but more substitutable or less direct.

60–74 = derivative / smaller / lower-confidence exposure
Still relevant, but not the main valve controlling flow.

Directness flags: D = disclosed OpenAI/Stargate link, E = direct ecosystem supplier to disclosed OpenAI platforms, I = inferred upstream bottleneck.

## The critical path

OpenAI's current infrastructure strategy is basically: Microsoft Azure + Oracle/Stargate + CoreWeave + AWS + NVIDIA + AMD + Broadcom custom silicon, then all the upstream bottlenecks required to make that real: TSMC / ASML / HBM / CoWoS / optical networking / switchgear / turbines / liquid cooling / ODM racks.

The direct disclosed anchors are huge. Microsoft remains OpenAI's primary cloud partner; OpenAI and AWS expanded their existing $38B agreement by another $100B over eight years including about 2GW of Trainium capacity; NVIDIA announced an OpenAI LOI for at least 10GW of NVIDIA systems and up to $100B of investment; Oracle/OpenAI Stargate has over 5GW under development via the Oracle deal and nearly 7GW planned when including Abilene, five new sites, and CoreWeave; CoreWeave's OpenAI contract value is now about $22.4B.

The silicon diversification is also now explicit: OpenAI/AMD announced a 6GW Instinct GPU agreement beginning with MI450 in 2H 2026, while OpenAI/Broadcom announced 10GW of OpenAI-designed custom AI accelerators developed and deployed with Broadcom.

## Tier 0: highest bottleneck scores

Score   Company Ticker  Layer   Why it matters
100     NVIDIA  NVDA    GPUs, networking, rack-scale AI systems The current king valve. OpenAI has disclosed a 10GW NVIDIA systems partnership/LOI.
98      TSMC    TSM / 2330.TW   Foundry + advanced packaging    Manufactures the leading AI silicon; OpenAI's Broadcom chip path relies on TSMC capacity per reporting.
97      Broadcom        AVGO    Custom accelerators + Ethernet  OpenAI 10GW custom accelerator partner. Also key Ethernet silicon.
96      ASML    ASML    EUV lithography No EUV, no frontier chips. Monopoly-like tool bottleneck.
95      Oracle  ORCL    Stargate / OCI AI data centers  OpenAI's largest Stargate cloud/data-center partner after Microsoft/NVIDIA.
95      SK Hynix        000660.KS       HBM memory      Primary HBM supplier to NVIDIA; HBM is essential for AI chipsets.
94      Microsoft       MSFT    Azure / OpenAI cloud channel    Primary OpenAI cloud partner.
92      AMD     AMD     GPUs    6GW OpenAI Instinct agreement; biggest disclosed non-NVIDIA GPU path.
92      Vertiv  VRT     AI power/cooling        One of the purest public ways to play high-density AI data-center power/cooling.
91      Schneider Electric      SU.PA / SBGSY   UPS, switchgear, power/cooling  Supplies critical data-center infrastructure: UPS, switchgear, PDUs, precision cooling, software.
91      Amazon  AMZN    AWS + Trainium  $100B expansion, 2GW Trainium commitment.
90      CoreWeave       CRWV    Neocloud GPU capacity   Direct OpenAI contract value about $22.4B.
90      Applied Materials       AMAT    Semi equipment  Core wafer/process equipment across foundry/memory.
89      GE Vernova      GEV     Gas turbines / grid power       Explicitly cited in Oracle's Abilene Stargate fact sheet as turbine provider.
88      Micron  MU      HBM / DRAM      U.S. HBM alternative; memory oligopoly beneficiary.
88      Lam Research    LRCX    Semi equipment  Etch/deposition bottleneck for leading nodes and memory.
88      KLA     KLAC    Metrology / inspection  Yield-control choke point; leading-edge chips need defect control.
88      Ajinomoto       2802.T / AJINY  ABF substrate material  ABF material is critical for advanced GPU/AI packages.
87      Quanta Services PWR     Grid construction       AI factories need interconnection, transmission, substations, construction muscle.
86      Arista Networks ANET    AI Ethernet switching   One of the cleanest Ethernet AI networking plays.
86      Eaton   ETN     Electrical gear Switchgear, UPS, power distribution for hyperscale data centers.

## Layer 1 — Cloud, neoclouds, Stargate operators

These are the entities OpenAI actually uses or could use to turn chips into usable training/inference capacity.

Score   Company Ticker  Directness      Bottleneck role
95      Oracle  ORCL    D       Stargate/OCI buildout; OpenAI/Oracle 4.5GW additional Stargate capacity.
94      Microsoft       MSFT    D       Primary OpenAI cloud partner; Azure remains the default OpenAI cloud path.
91      Amazon  AMZN    D       AWS partnership expansion; 2GW Trainium capacity.
90      CoreWeave       CRWV    D       OpenAI GPU cloud capacity; $22.4B contract value.
83      SoftBank Group  9984.T / SFTBY  D       Stargate financial lead; capital allocator and project sponsor.
76      Alphabet        GOOGL   D/I     Google Cloud exposure exists, but OpenAI reportedly does not currently plan to use Google TPUs as a primary path. Lower-confidence.
68      DigitalBridge   DBRG    I       Data-center investment/owner exposure; derivative, not OpenAI-specific.
67      Applied Digital APLD    I       AI data-center capacity optionality; not confirmed OpenAI supplier.
66      Nebius  NBIS    I       Neocloud capacity; relevant to overall compute scarcity, not confirmed OpenAI.
64      Iris Energy     IREN    I       AI/HPC data-center conversion capacity; not confirmed OpenAI.
63      Equinix EQIX    I       Interconnection/data-center real estate; more enterprise colo than direct AI factory bottleneck.
63      Digital Realty  DLR     I       Data-center landlord; useful but less scarce than power/chips.
62      TeraWulf        WULF    I       Power-backed HPC conversion optionality; not confirmed OpenAI.
61      Cipher Mining   CIFR    I       Power/data-center optionality; derivative.
60      Hut 8   HUT     I       Data-center/energy optionality; derivative.
60      Bitdeer BTDR    I       Compute/data-center manufacturing + hosting optionality; derivative.

Takeaway: direct OpenAI exposure here is ORCL / MSFT / AMZN / CRWV, with SoftBank as the Stargate capital wrapper. The smaller neocloud/miner-conversion names are real AI infrastructure optionality, but calling them "OpenAI suppliers" would be fake precision.

## Layer 2 — AI accelerators, custom silicon, CPU IP

Score   Company Ticker  Directness      Bottleneck role
100     NVIDIA  NVDA    D       GPUs, NVLink, networking, CUDA/software, rack-scale systems.
97      Broadcom        AVGO    D       OpenAI custom accelerators + Ethernet.
92      AMD     AMD     D       6GW OpenAI GPU agreement, MI450 ramp beginning 2H 2026.
85      Arm     ARM     D/E     Stargate technology partner; CPU/IP layer in AI servers.
84      Marvell MRVL    I       Custom silicon, DSPs, optical/electrical connectivity; strong AI ASIC exposure, no disclosed OpenAI deal.
76      Intel   INTC    I       Foundry/packaging/CPU optionality; not a core OpenAI path today.
72      MediaTek        2454.TW I       Custom ASIC ecosystem exposure; derivative.
70      Monolithic Power        MPWR    I       Power management for high-density AI boards; important but more substitutable.
68      Rambus  RMBS    I       Memory interface IP; derivative to HBM/advanced memory.
66      Synopsys        SNPS    I       EDA/IP for chip design; essential to chip design but less OpenAI-specific.
65      Cadence CDNS    I       EDA/design tools; same logic as Synopsys.

Takeaway: the direct OpenAI silicon winners are NVDA, AVGO, AMD. The upstream "must exist for everyone" layer is TSMC / ASML / EDA / HBM / substrates.

## Layer 3 — Foundry, lithography, advanced packaging

Score   Company Ticker  Directness      Bottleneck role
98      TSMC    TSM / 2330.TW   D/I     Leading-edge foundry + CoWoS advanced packaging. CoWoS is widely used in AI chips including NVIDIA designs.
96      ASML    ASML    I       EUV monopoly-like supplier for frontier chip manufacturing.
90      Applied Materials       AMAT    I       Deposition, etch, process control across foundry/memory.
88      Lam Research    LRCX    I       Etch/deposition for advanced nodes and memory.
88      KLA     KLAC    I       Inspection/metrology/yield bottleneck.
84      Tokyo Electron  8035.T / TOELY  I       Leading semiconductor process equipment.
82      ASM International       ASM.AS / ASMIY  I       ALD deposition; advanced logic/memory scaling.
82      Advantest       6857.T / ATEYY  I       AI chip test equipment.
80      Teradyne        TER     I       Semiconductor test.
79      BE Semiconductor        BESI.AS I       Hybrid bonding/advanced packaging tools.
79      Entegris        ENTG    I       Specialty materials, filters, contamination control.
78      Disco   6146.T  I       Dicing/grinding for advanced packages.
77      Lasertec        6920.T  I       EUV mask inspection; very scarce but indirect.
76      Amkor   AMKR    I       OSAT / advanced packaging alternative capacity.
76      ASE Technology  ASX / 3711.TW   I       Largest OSAT / packaging and testing.
76      Camtek  CAMT    I       Inspection/metrology for advanced packaging. Smaller but real.
75      Nova    NVMI    I       Process control/metrology.
75      FormFactor      FORM    I       Probe cards/testing.
74      Onto Innovation ONTO    I       Advanced packaging/process metrology.
72      Kulicke & Soffa KLIC    I       Bonding/assembly tools.
72      Ultra Clean     UCTT    I       Subsystems for semi equipment.
71      VAT Group       VACN.SW I       Vacuum valves for semiconductor equipment. Tiny but genuinely chokepoint-ish.
70      Ichor   ICHR    I       Fluid delivery subsystems for wafer fab equipment.
69      Cohu    COHU    I       Semiconductor test handling.
68      Aehr Test Systems       AEHR    I       Burn-in/test; smaller and more niche.

Takeaway: if you force me to name the deepest non-obvious bottleneck layer, it is TSMC CoWoS + HBM + ASML EUV + advanced packaging inspection/test. That is where "demand" becomes physical reality instead of press-release confetti.

## Layer 4 — HBM, DRAM, NAND, storage

Score   Company Ticker  Directness      Bottleneck role
95      SK Hynix        000660.KS       I/E     Primary NVIDIA HBM supplier; HBM essential for AI chipsets.
88      Micron  MU      I       HBM/DRAM alternative; U.S. supply-chain importance.
86      Samsung Electronics     005930.KS / SSNLF       I       HBM, DRAM, NAND, foundry optionality.
74      Western Digital / SanDisk       WDC / SNDK      I       AI data storage, training data, inference storage. Less scarce than HBM.
72      Seagate STX     I       HDD nearline storage for AI data lakes.
70      Kioxia  285A.T  I       NAND/storage; derivative exposure.
69      Phison  8299.TWO        I       SSD controllers/storage; smaller.
68      Silicon Motion  SIMO    I       SSD controllers; smaller derivative.
67      NetApp  NTAP    I       Enterprise storage; useful but less pure.
66      Pure Storage    PSTG    I       AI storage systems; not a hard OpenAI bottleneck.

Takeaway: HBM is the bottleneck; storage is the plumbing. The cleanest bottleneck is SK Hynix, then Micron/Samsung.

## Layer 5 — ABF substrates, PCBs, interposers, package materials

This is a less sexy layer, which is usually exactly where the market gets lazy. No substrate, no package. No package, no GPU. No GPU, no model. Stunningly simple. People still manage to miss it.

Score   Company Ticker  Directness      Bottleneck role
88      Ajinomoto       2802.T / AJINY  I       ABF film/materials for advanced substrates.
86      Ibiden  4062.T  I       Advanced substrates for high-end processors.
82      Unimicron       3037.TW I       ABF substrates / PCBs.
80      Nan Ya PCB      8046.TW I       Substrates / PCBs.
78      AT&S    ATS.VI  I       High-end IC substrates.
76      Shinko Electric 6967.T  I       Advanced substrates; verify current public listing/liquidity before using.
74      Kinsus  3189.TW I       IC substrates.
72      TTM Technologies        TTMI    I       PCBs, data-center/networking hardware.
71      Fujikura        5803.T  I       Cabling/interconnect materials.
70      Sumitomo Electric       5802.T  I       Optical/electrical cabling.
68      Rogers  ROG     I       Advanced materials for electronics.
66      DuPont  DD      I       Electronics materials; broad, less pure.

## Layer 6 — Networking, switching, interconnect, optics

This is the second big "don't miss it" category. Training clusters are not just GPUs sitting in a room admiring themselves. They need low-latency, high-bandwidth interconnect. At OpenAI scale, networking is compute.

Score   Company Ticker  Directness      Bottleneck role
95      NVIDIA  NVDA    D       NVLink, InfiniBand, Spectrum-X Ethernet, NICs.
94      Broadcom        AVGO    D       Ethernet switching silicon, retimers, custom rack systems.
86      Arista Networks ANET    I/E     AI Ethernet switching for hyperscalers.
84      Marvell MRVL    I       DSPs, custom silicon, optical interconnect silicon.
84      Amphenol        APH     E       High-speed interconnect/cables/connectors; named in NVIDIA infrastructure ecosystem.
83      Credo Technology        CRDO    I/E     Active electrical cables, retimers, AI cluster connectivity. Smaller but important.
82      Astera Labs     ALAB    I/E     PCIe/CXL retimers and connectivity.
82      Coherent        COHR    I/E     Lasers/transceivers, 800G/1.6T optical.
82      Lumentum        LITE    I/E     Lasers/optical components for AI data centers.
80      Fabrinet        FN      I/E     Optical module manufacturing. Quiet picks-and-shovels name.
78      Innolight       300308.SZ       I       Leading optical module supplier.
76      Eoptolink       300502.SZ       I       Optical transceivers.
75      Cisco   CSCO    I       Ethernet/networking; large but less pure.
74      Ciena   CIEN    I       Optical transport/backbone.
74      Applied Optoelectronics AAOI    I       Smaller optical module exposure; higher torque, higher execution risk.
72      Lattice Semiconductor   LSCC    I       FPGAs/control logic; derivative.
70      Accelink        002281.SZ       I       Optical components/modules.
69      Nokia   NOK     I       Network infrastructure; lower purity.
68      Corning GLW     I       Fiber/glass; important but very broad.

Takeaway: highest quality layer is NVDA / AVGO / ANET / MRVL / APH. Highest smaller-player torque is CRDO / ALAB / FN / AAOI / COHR / LITE.

## Layer 7 — Servers, ODMs, racks, liquid-cooled systems

NVIDIA's ecosystem explicitly includes server makers, ODMs, rack integrators, and power/cooling component suppliers for GB200-class systems. Supermicro's GB200 NVL72 materials describe a rack with 72 Blackwell GPUs, 36 Grace CPUs, NVLink switching, and fully integrated liquid cooling.

Score   Company Ticker  Directness      Bottleneck role
86      Dell    DELL    E       AI servers, enterprise/AI factory systems.
84      Hon Hai / Foxconn       2317.TW / HNHPF E       ODM/rack manufacturing; Schneider AI data-center partnership.
84      Quanta  2382.TW E       AI server ODM.
82      Super Micro Computer    SMCI    E       Liquid-cooled NVIDIA systems; high torque, higher governance/volatility risk.
82      Wiwynn  6669.TW E       Hyperscale AI servers.
82      Wistron 3231.TW E       AI server manufacturing.
80      Hewlett Packard Enterprise      HPE     E       AI servers/systems.
78      Lenovo  0992.HK / LNVGY E       AI servers.
76      Inventec        2356.TW E       Server ODM.
76      Lite-On 2301.TW E       Power supplies/cooling components.
75      Delta Electronics       2308.TW E       Power supplies, thermal, data-center infrastructure.
74      Pegatron        4938.TW E       ODM/server manufacturing.
72      Gigabyte        2376.TW E       AI servers/motherboards.
71      ASUS    2357.TW E       Server platforms.
70      ASRock  3515.TW E       ASRock Rack server platforms.

Takeaway: direct public leverage is split between DELL, SMCI, Foxconn, Quanta, Wiwynn, Wistron, Delta. If OpenAI/Stargate deployment schedules slip, this layer will show it fast through backlog, lead times, and margin commentary.

## Layer 8 — Power, grid, switchgear, turbines, cooling

This is the most underappreciated non-chip layer. Stargate is measured in gigawatts, not "number of GPUs." Oracle's Abilene fact sheet says the site includes GE Vernova turbines for reliability/backup and uses closed-loop non-evaporative liquid cooling.

Score   Company Ticker  Directness      Bottleneck role
92      Vertiv  VRT     E/I     High-density AI cooling/power infrastructure.
91      Schneider Electric      SU.PA / SBGSY   E/I     UPS, switchgear, PDUs, precision cooling, energy software.
90      Eaton   ETN     I       Switchgear, breakers, UPS, electrical distribution.
89      GE Vernova      GEV     D/E     Gas turbines / grid systems; cited for Abilene turbines.
87      Quanta Services PWR     I       Grid interconnection, transmission, utility construction.
86      ABB     ABBNY   I       Electrification, switchgear, automation.
84      Siemens Energy  ENR.DE / SMNEY  I       Turbines, grid equipment, transformers.
82      Delta Electronics       2308.TW E       Power supplies, thermal management.
82      EMCOR   EME     I       Mechanical/electrical construction for data centers.
82      Modine  MOD     I       Thermal management / data-center cooling. Smaller high-torque name.
81      Powell Industries       POWL    I       Switchgear/electrical systems. Smaller bottleneck candidate.
80      Hammond Power Solutions HPS.A / HMDPF   I       Transformers. Smaller, real bottleneck.
78      nVent   NVT     I       Enclosures/electrical infrastructure.
78      Dover   DOV     E/I     CPC liquid-cooling connectors and industrial components.
78      Comfort Systems FIX     I       Mechanical/electrical contractor for data centers.
77      Cummins CMI     I       Backup generation / engines.
77      Caterpillar     CAT     I       Backup power/gensets; less pure.
76      Trane Technologies      TT      I       Cooling/HVAC; broad but relevant.
76      Johnson Controls        JCI     I       Cooling/building systems.
75      Carrier CARR    I       HVAC/cooling; broad.
74      Bloom Energy    BE      I       Fuel cells / onsite power optionality. Not confirmed OpenAI.
74      Constellation Energy    CEG     I       Nuclear/clean power supply optionality.
73      Vistra  VST     I       Power generation exposure.
72      Talen Energy    TLN     I       Data-center/nuclear power optionality.
72      NextEra Energy  NEE     I       Renewables/grid power; broad, lower purity.
70      Wärtsilä        WRT1V.HE        I       Power systems/engines; derivative.

Takeaway: for physical bottlenecks outside chips, the best watchlist is VRT / ETN / Schneider / GEV / PWR / MOD / POWL / Hammond / EME / FIX. That is where the "AI is software" crowd gets humbled by copper, gas turbines, transformers, and permitting. Very undignified for the cloud. Very profitable if you map it early.

## Layer 9 — Construction, financing, land, interconnection

OpenAI/Stargate has explicitly said it is seeking partners across land, power, construction, equipment, and related buildout needs. The public-company exposure here is real, but less clean because many actual site developers are private.

Score   Company Ticker  Directness      Bottleneck role
87      Quanta Services PWR     I       Power construction/interconnection.
82      EMCOR   EME     I       Data-center electrical/mechanical construction.
78      Comfort Systems FIX     I       Mechanical systems/HVAC construction.
75      Fluor   FLR     I       Large project EPC exposure.
74      Jacobs  J       I       Engineering/project management.
72      AECOM   ACM     I       Infrastructure engineering.
70      MasTec  MTZ     I       Power/fiber/infrastructure construction.
68      Blue Owl Capital        OWL     I       Infrastructure/private credit financing exposure.
67      JPMorgan        JPM     I       Project finance; too broad, but part of financing ecosystem.
65      Brookfield Infrastructure       BIP / BIPC      I       Infrastructure capital, power/data-center adjacency.
64      DigitalBridge   DBRG    I       Digital infrastructure investment; less direct.

## Layer 10 — Fiber, backbone, data transport, edge connectivity

Score   Company Ticker  Directness      Bottleneck role
78      Ciena   CIEN    I       Optical transport/backbone.
75      Lumen   LUMN    I       Fiber network capacity; high debt/turnaround risk.
74      Corning GLW     I       Fiber/glass.
72      Nokia   NOK     I       Optical/network equipment.
70      Cisco   CSCO    I       Enterprise/networking; less pure than Arista/Broadcom.
68      Juniper JNPR    I       Networking; acquisition/arb dynamics reduce standalone purity.
66      American Tower  AMT     I       Edge/connectivity adjacency, not core OpenAI bottleneck.

## "Don't leave out smaller players" list

These are the names I would not want missing from an OpenAI supply-chain screener, even if they are not direct OpenAI suppliers:

Company Ticker  Score   Why it belongs
Credo   CRDO    83      AI cluster connectivity / active electrical cables.
Astera Labs     ALAB    82      PCIe/CXL retimers and connectivity.
Fabrinet        FN      80      Optical module manufacturing.
Applied Optoelectronics AAOI    74      Higher-beta optical module exposure.
Modine  MOD     82      Data-center thermal/cooling torque.
Powell  POWL    81      Switchgear / electrical systems.
Hammond Power   HPS.A / HMDPF   80      Transformer bottleneck.
Camtek  CAMT    76      Advanced packaging inspection.
Nova    NVMI    75      Metrology/process control.
Onto Innovation ONTO    74      Advanced packaging/process control.
FormFactor      FORM    75      Probe cards / test.
Amkor   AMKR    76      Advanced packaging / OSAT.
Ultra Clean     UCTT    72      Semi equipment subsystems.
Ichor   ICHR    70      Semi fluid delivery subsystems.
VAT Group       VACN.SW 71      Vacuum valves, very niche equipment bottleneck.
Aehr Test       AEHR    68      Burn-in/test optionality.
TTM Technologies        TTMI    72      PCBs/networking hardware.
Silicon Motion  SIMO    68      Storage controller derivative.
Phison  8299.TWO        69      SSD controller exposure.

## Best bottleneck clusters by investability

### 1. Hard bottleneck / lower ambiguity

NVDA, TSM, AVGO, ASML, SK Hynix, AMD, ORCL, MSFT, AMZN, CRWV

These are the "no cute stuff" names. Direct OpenAI exposure is strongest in NVDA, AVGO, AMD, ORCL, MSFT, AMZN, CRWV. Upstream monopoly/chokepoint exposure is strongest in TSM, ASML, SK Hynix.

### 2. Physical infrastructure bottlenecks

VRT, ETN, Schneider, GEV, PWR, EME, FIX, MOD, POWL, Hammond

This is the layer that can delay the whole thing. Chips can be ordered. Power cannot be wished into existence by a keynote.

### 3. AI networking / optical bottlenecks

AVGO, ANET, MRVL, APH, CRDO, ALAB, COHR, LITE, FN, AAOI

This is probably the best "second-order OpenAI" basket. The direct OpenAI docs talk about GPU/custom silicon, but clusters do not scale without networking and optics. This is where smaller names can move like they found caffeine and leverage at the same time.

### 4. Advanced packaging / test / substrates

TSM, AMAT, LRCX, KLAC, TEL, ASMIY, BESI, AMKR, ASX, CAMT, NVMI, ONTO, FORM, Ajinomoto, Ibiden, Unimicron

This is the "quiet choke point" layer. Not as meme-able as GPUs, but usually more structurally constrained.

## Clean final ranking: top public OpenAI bottleneck map

Rank    Ticker  Company Score   Layer
1       NVDA    NVIDIA  100     GPU/network systems
2       TSM     TSMC    98      Foundry/CoWoS
3       AVGO    Broadcom        97      Custom silicon/Ethernet
4       ASML    ASML    96      EUV lithography
5       ORCL    Oracle  95      Stargate cloud/data centers
6       000660.KS       SK Hynix        95      HBM
7       MSFT    Microsoft       94      Azure/OpenAI cloud
8       AMD     AMD     92      GPUs
9       VRT     Vertiv  92      Power/cooling
10      AMZN    Amazon  91      AWS/Trainium
11      SU.PA   Schneider Electric      91      Electrical/cooling
12      CRWV    CoreWeave       90      GPU cloud
13      AMAT    Applied Materials       90      Semi equipment
14      GEV     GE Vernova      89      Turbines/grid
15      MU      Micron  88      HBM/DRAM
16      LRCX    Lam Research    88      Semi equipment
17      KLAC    KLA     88      Inspection/metrology
18      2802.T  Ajinomoto       88      ABF materials
19      PWR     Quanta Services 87      Grid construction
20      ANET    Arista Networks 86      AI Ethernet switching
21      ETN     Eaton   86      Electrical gear
22      005930.KS       Samsung Electronics     86      HBM/foundry
23      LRCX    Lam Research    88      Semi equipment (duplicate removed; see #16)
24      8035.T  Tokyo Electron  84      Semi equipment
25      MRVL    Marvell 84      Custom silicon/optical

## Macro Overlay

Regime: Risk-On, but increasingly crowded at the top.

SPY/QQQ have been near highs with VIX in the mid-teens during the period this map was built. Mega-cap tech earnings have supported the AI infrastructure narrative. The risk is not "the theme is wrong." The risk is "the obvious names are already expensive and the real mispricing has migrated to second-order suppliers."

The structural thesis: AI training clusters need petawatts, not just petaflops. Power, substrate, packaging, cooling, and fiber are the gating factors as model scale approaches trillion-parameter territory. OpenAI's disclosed commitments confirm this is measured in gigawatts and hundreds of billions of dollars.

## Social Sentiment Overlay

Sentiment: Extremely bullish on the theme.
Temperature: Euphoric in NVDA/AVGO/TSM; hot in VRT/ETN/ANET; warm-to-cool in substrate/packaging/smaller names.
Crowding risk: Very high in the top 10. Medium in power/cooling. Lowest in the substrate/PCB/smaller-player layer.

## Trade Grade

Theme grade: A / 88.
Current blanket buy grade: C / 52. Many top names are extended. The correct approach is watchlist construction, not shotgun buying.

## Position Size

For a concentrated OpenAI-infrastructure basket:

Core disclosed names (NVDA, AVGO, AMD, ORCL, MSFT, AMZN, CRWV): 40–50% of basket, but only after chart/valuation confirmation. No stop = no trade.
Physical infrastructure (VRT, ETN, GEV, Schneider, PWR, MOD, POWL): 25–35%.
Optical/networking (ANET, MRVL, CRDO, ALAB, COHR, LITE, FN, AAOI): 15–20%.
Substrate/packaging/test (Ajinomoto, Ibiden, Amkor, CAMT, NVMI, ONTO): 10–15%.

Portfolio risk check: At full deployment, this entire basket should be treated as one factor in a portfolio, not independent positions. In a risk-off AI capex scare, NVDA + AVGO + TSM + VRT + ETN + ANET will trade as one correlated unit, just with extra ticker symbols.

## Biggest Risk to Thesis

The real risks are not "AI fails":

OpenAI revenue model fails to scale — subscription/API pricing disappoints, hyperscaler margin pressure mounts.
Stargate timeline slips — power interconnection, permitting, construction execution.
Silicon oversupply — if TSMC CoWoS capacity expands faster than demand, scarcity premium evaporates.
Competitor buildout (Meta, Google, Amazon) cannibalizes the AI market before OpenAI's infrastructure investment pays off.
Regulatory/political risk — data privacy, AI safety regulations, geopolitical semiconductor restrictions.
Financing risk — if capital markets tighten, Stargate's $500B stated ambition shrinks materially.
