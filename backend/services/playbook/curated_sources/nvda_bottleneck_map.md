# NVDA Critical Supply Chain Bottleneck Map — Public Companies Only

Bias: NVDA is not one supply chain. It is several stacked bottlenecks: TSMC/CoWoS → HBM → ABF substrates → OSAT/test → optics/networking → power/cooling → ODM/rack assembly. The best "under-the-floorboards" alpha is usually not the obvious mega-cap spine. It is the smaller public companies controlling scarce substrate, testing, thermal, optical, and rack-scale capacity.

Macro Overlay: AI infrastructure remains structurally risk-on, but the trade is crowded. Mega-cap winners are already worshipped like golden calves. The cleaner opportunity is finding public bottleneck suppliers with revenue acceleration, capacity tightness, and misunderstood NVDA/AI exposure, which fits your Macro → Sector → Bottleneck framework.

NVIDIA's own filing names the core hard dependencies: it is fabless, relies on external foundries, assembly, testing, packaging, procures memory/substrates/components directly, uses TSMC and Samsung for wafer fabrication, SK hynix, Micron, Samsung for memory, CoWoS for advanced packaging, and contractors including Hon Hai/Foxconn, Wistron, and Fabrinet. NVIDIA's rack systems are now full infrastructure products, not just chips: GB200 NVL72 connects 36 Grace CPUs and 72 Blackwell GPUs in a rack-scale liquid-cooled NVLink domain. TSMC describes CoWoS as integrating logic chiplets with HBM on interposers for AI/HPC, and Reuters has repeatedly identified advanced packaging/CoWoS capacity as a key NVIDIA bottleneck.

## Scoring Legend

Bottleneck Score = criticality to NVDA × scarcity × concentration × substitution difficulty × public-company investability.

Score   Meaning
95–100  System-level choke point. If this breaks, NVDA shipments slow.
85–94   Mission-critical bottleneck with high pricing/capacity leverage.
75–84   Critical layer supplier; strong read-through but more replaceable.
65–74   Important second-order enabler.
<65     Relevant, but less direct or less scarce.

Linkage codes: D = direct NVIDIA named/sourced, E = NVIDIA ecosystem/recommended vendor, I = indirect enabler of NVDA capacity.

## 1. Highest-Conviction NVDA Bottlenecks Ranked

Rank    Company Ticker  Layer   Link    Bottleneck Score        Why it matters
1       Taiwan Semiconductor    TSM / 2330.TW   Foundry + CoWoS D       100     Core Blackwell/Hopper logic and CoWoS packaging gatekeeper.
2       SK hynix        000660.KS       HBM     D       98      Most important HBM supplier; Reuters notes SK hynix is NVIDIA's largest memory partner.
3       ASML    ASML    EUV lithography I       96      Controls EUV tool supply for TSMC/Samsung/SK hynix/Micron advanced-node and DRAM expansion.
4       Micron  MU      HBM / memory / SSD      D       94      Direct NVIDIA memory supplier; HBM share is rising and strategically important.
5       Samsung Electronics     005930.KS       HBM + foundry   D       93      Dual role: memory supplier and secondary foundry option.
6       Hon Hai / Foxconn       2317.TW / HNHPF AI server assembly      D       92      Direct contractor; key GB200/AI-server rack-scale manufacturer.
7       Applied Materials       AMAT    WFE / packaging tools   I       92      Critical deposition, CMP, etch, packaging-process tooling.
8       Lam Research    LRCX    Etch/deposition I       91      Needed for advanced logic and HBM DRAM process steps.
9       Ajinomoto       2802.T  ABF film        I       91      ABF film is foundational to high-end package substrates. Small but nasty bottleneck.
10      Tokyo Electron  8035.T  WFE     I       90      Coater/developer, deposition, etch, clean exposure to advanced nodes.
11      KLA     KLAC    Process control I       90      Yield/metrology bottleneck. More complexity = more inspection.
12      ASE / SPIL      ASX / 3711.TW   OSAT / packaging / test D       89      SPIL named in NVIDIA's U.S. manufacturing ecosystem; ASE is public parent.
13      Wistron 3231.TW AI server assembly      D       89      Direct contractor; Reuters cites Wistron as NVIDIA supplier with AI server orders into 2027.
14      Ibiden  4062.T  ABF substrate   I       89      One of the highest-quality high-end substrate names.
15      Lumentum        LITE    Lasers / CPO    D/E     88      NVIDIA invested with purchase commitments/access rights to optical capacity.
16      Coherent        COHR    Lasers / optics D/E     88      Same NVIDIA optical-capacity deal category as Lumentum.
17      Amkor   AMKR    Advanced packaging/test D       87      NVIDIA-linked U.S. advanced packaging partner; Arizona CoWoS/3D-IC read-through.
18      Advantest       6857.T  GPU/HBM testing I       86      Test capacity becomes a bottleneck as HBM/GPU complexity explodes.
19      Vertiv  VRT     Power/cooling   E       86      Co-developed 7MW GB200 NVL72 reference architecture with NVIDIA.
20      Delta Electronics       2308.TW Power/cooling   E/I     85      One of the most important data-center power and thermal suppliers.
21      Unimicron       3037.TW ABF / IC substrates     I       85      Major substrate supplier; strong AI package read-through.
22      Fabrinet        FN      Optical manufacturing / systems D/E     84      Direct NVIDIA contractor and named optical ecosystem participant.
23      Corning GLW     Optical glass/fiber     E       83      Named in NVIDIA silicon-photonics ecosystem.
24      LITEON  2301.TW Power systems / cooling E       83      Shows NVIDIA GB200 NVL72 power system tailored for Blackwell.
25      DISCO   6146.T  Wafer thinning/dicing   I       82      Critical for HBM stacks and advanced packaging.
26      ASM International       ASMI.AS ALD / epi tools I       82      Advanced deposition bottleneck for leading-edge semis.
27      Eaton   ETN     Cooling / power E       82      NVIDIA-recommended vendor for GB200 NVL72 thermal/cooling solutions.
28      Sumitomo Electric       5802.T  Optical components      E       81      Named in NVIDIA photonics ecosystem.
29      Browave 3163.TWO        Optical components      E       80      Small public NVIDIA silicon-photonics ecosystem name.
30      Eoptolink       300502.SZ       Optical transceivers    E       80      NVIDIA says pluggable transceiver technologies are supported by Eoptolink and peers.
31      Zhongji Innolight       300308.SZ       Optical transceivers    E       80      Same pluggable optics bucket; important AI networking exposure.
32      Samsung Electro-Mechanics       009150.KS       Substrates      I       79      Advanced package substrate exposure.
33      Nan Ya PCB      8046.TW ABF / PCB       I       79      Public substrate/PCB supplier; AI package bottleneck exposure.
34      Kinsus  3189.TW IC substrates   I       78      Smaller substrate bottleneck name.
35      AT&S    ATS.VI  IC substrates   I       78      European high-end substrate supplier.
36      ABB     ABBN.SW / ABB   Data-center power       E/I     78      Important for high-voltage/power architecture around AI factories.
37      Schneider Electric      SU.PA   Data-center power       I       78      Data-center electrical infrastructure.
38      Auras   3324.TWO        Thermal modules I       77      Smaller thermal/cooling supplier with AI server read-through.
39      nVent   NVT     Liquid cooling / enclosures     I       77      Rack infrastructure and liquid-cooling exposure.
40      Asia Vital Components   3017.TW Thermal / cooling       I       76      Smaller AI-server thermal supplier.

## 2. Layer-by-Layer Full Public Map

### Layer A — Design, EDA, IP, Validation

These are not the biggest physical bottlenecks, but they are hard to substitute. NVIDIA depends on advanced design software, IP, simulation, verification, and validation tooling before any chip reaches TSMC.

Company Ticker  Role    Link    Score
Synopsys        SNPS    EDA, verification, IP, simulation       I       75
Cadence CDNS    EDA, verification, system design        I       75
Arm     ARM     CPU/IP ecosystem; Grace CPU relevance   I       72
Siemens SIE.DE  EDA / verification via Siemens EDA      I       68
Keysight        KEYS    High-speed validation/test instrumentation      I       63
Teradyne        TER     Semiconductor/system test       I       73
Advantest       6857.T  GPU/HBM test systems    I       86
FormFactor      FORM    Probe cards / wafer test        I       73

Investment read: SNPS/CDNS are phenomenal businesses, but less "hidden." Advantest/FormFactor are closer to the physical bottleneck.

### Layer B — Advanced Foundry / Logic Manufacturing

NVIDIA is fabless and relies on external foundries; it names TSMC and Samsung for wafer fabrication.

Company Ticker  Role    Link    Score
TSMC    TSM / 2330.TW   Primary foundry, advanced nodes, CoWoS  D       100
Samsung Electronics     005930.KS       Secondary foundry + memory      D       93
UMC     UMC / 2303.TW   Mature-node ancillary semis     I       65
GlobalFoundries GFS     Mature/specialty process ecosystem      I       62
Intel   INTC    Future foundry/packaging optionality, not core NVDA supplier    Watch   60

Key point: TSMC is the beating heart. Samsung is strategically important as a second-source option, but NVDA's most critical production remains TSMC-led.

### Layer C — Advanced Packaging / CoWoS / OSAT

This is the nastiest bottleneck in the stack. Blackwell-class GPUs require advanced packaging, HBM integration, interposers, substrates, test, and yield management. Reuters specifically identified NVIDIA's Blackwell as using CoWoS and said packaging remains a bottleneck.

Company Ticker  Role    Link    Score
TSMC    TSM / 2330.TW   CoWoS, 3D packaging     D       100
ASE Technology / SPIL   ASX / 3711.TW   Packaging/test; SPIL named in NVIDIA U.S. ecosystem     D       89
Amkor   AMKR    Advanced packaging/test; NVIDIA-linked Arizona partner  D       87
Powertech Technology    6239.TW Memory/logic packaging and test I       70
KYEC    2449.TW IC test / backend services      I       72
ChipMOS IMOS / 8150.TW  Backend test/assembly   I       58
JCET    600584.SS       OSAT    I       62
Tongfu Microelectronics 002156.SZ       OSAT    I       60
Tianshui Huatian        002185.SZ       OSAT    I       55

Investment read: TSMC is obvious. AMKR/ASE/SPIL/KYEC are more interesting as packaging/test capacity plays, especially if the U.S. localization theme accelerates.

### Layer D — HBM / DRAM / Memory

NVIDIA names SK hynix, Micron, and Samsung as memory suppliers. Reuters reported that NVIDIA has struck memory supply deals with SK hynix and that memory bottlenecks could persist for years; it also cited HBM share estimates of SK hynix at 58%, Samsung at 21%, and Micron at 21%.

Company Ticker  Role    Link    Score
SK hynix        000660.KS       HBM leader, NVIDIA memory partner       D       98
Micron  MU      HBM, DRAM, SSD  D       94
Samsung Electronics     005930.KS       HBM, DRAM, foundry      D       93
Nanya Technology        2408.TW DRAM exposure, not core HBM     I       55
Winbond 2344.TW Specialty memory, peripheral exposure   I       50
Kioxia  285A.T  NAND/storage, lower directness  I       58
Western Digital WDC     NAND/storage    I       58
Seagate STX     Storage systems, not core HBM   I       55

Investment read: SK hynix is the cleanest HBM bottleneck. Micron has the better U.S.-listed accessibility. Samsung is critical but less pure because it is too broad.

### Layer E — ABF Substrates, IC Substrates, PCB, Laminates

This is where smaller public companies matter. ABF substrate constraints can bottleneck high-end GPUs even if wafers and HBM are available. The key public substrate universe includes Ajinomoto, Ibiden, Unimicron, Nan Ya PCB, AT&S, Kinsus, Samsung Electro-Mechanics, and others. Public market research lists Unimicron, Ibiden, AT&S, Nan Ya PCB, and Shinko among the largest ABF substrate players, but Shinko is excluded here because it was delisted in 2025.

Company Ticker  Role    Link    Score
Ajinomoto       2802.T  ABF film supplier       I       91
Ibiden  4062.T  High-end ABF substrates I       89
Unimicron       3037.TW ABF / IC substrates     I       85
Nan Ya PCB      8046.TW ABF / PCB       I       79
Samsung Electro-Mechanics       009150.KS       Package substrates      I       79
Kinsus  3189.TW IC substrates   I       78
AT&S    ATS.VI  IC substrates   I       78
Elite Material  2383.TW Copper-clad laminate / high-speed PCB materials I       74
Zhen Ding       4958.TW PCB / server board exposure     I       74
Compeq  2313.TW PCB / server board exposure     I       73
Gold Circuit    2368.TW PCB / server board exposure     I       72
Tripod Technology       3044.TW PCB / server board exposure     I       71
Doosan Corp     000150.KS       High-end substrate/CCL materials        I       76
ITEQ    6213.TW PCB laminate materials  I       68
SKC     011790.KS       Glass substrate / advanced materials optionality        I       63
AGC     5201.T  Glass materials / future substrate optionality  I       62

Investment read: This is one of the best places to hunt. Ajinomoto, Ibiden, Unimicron, Nan Ya, Kinsus, AT&S, Elite Material are the "boring picks-and-shovels" that make the AI gods leave the cloud and enter physical reality. Very unsexy. Very useful. The best combo.

### Layer F — Semiconductor Equipment / Process Tools

ASML, Applied Materials, Lam, Tokyo Electron, and KLA are the main public equipment choke points for advanced logic, HBM, and packaging capacity. ASML supplies critical lithography tools to TSMC, Samsung, Intel and others, and has been expanding into advanced packaging opportunities. SK hynix has also placed major ASML EUV orders tied to HBM and advanced DRAM capacity.

Company Ticker  Role    Link    Score
ASML    ASML    EUV lithography I       96
Applied Materials       AMAT    Deposition, CMP, packaging tools        I       92
Lam Research    LRCX    Etch/deposition I       91
Tokyo Electron  8035.T  Coater/developer, etch, deposition, clean       I       90
KLA     KLAC    Inspection/metrology/process control    I       90
DISCO   6146.T  Wafer dicing, grinding, thinning        I       82
ASM International       ASMI.AS ALD / epi deposition    I       82
Lasertec        6920.T  Mask inspection I       82
SCREEN Holdings 7735.T  Cleaning/coater/developer tools I       80
BE Semiconductor        BESI.AS Hybrid bonding / die attach     I       76
Onto Innovation ONTO    Advanced packaging metrology/inspection I       75
Camtek  CAMT    Inspection/metrology    I       74
ASMPT   0522.HK Bonding/assembly equipment      I       74
Nova    NVMI    Process metrology       I       73
Towa    6315.T  Molding / semiconductor packaging equipment     I       73
Kulicke & Soffa KLIC    Wire/die bonding        I       72
SÜSS MicroTec   SMHN.DE Lithography/bonding for advanced packaging      I       70
Kokusai Electric        6525.T  Deposition tools        I       70
Tokyo Seimitsu / Accretech      7729.T  Dicing/metrology        I       70
VAT Group       VACN.SW Vacuum valves for semiconductor tools   I       69
MKS Instruments MKSI    Subsystems for process tools    I       68
Nikon   7731.T  Lithography/metrology, less advanced-node dominant      I       65
Canon   7751.T  Lithography, imprint optionality        I       65
Axcelis ACLS    Ion implantation        I       63
ACM Research    ACMR    Cleaning tools  I       62
Hitachi 6501.T  Metrology/tools via Hitachi High-Tech   I       62
Horiba  6856.T  Process/control instrumentation I       60

Investment read: Equipment names are high quality but many are already institutional consensus. The more interesting mid-cap torque sits in Camtek, Onto, Nova, BESI, SÜSS, Towa, KLIC, VAT, MKS depending on valuation/technical setup.

### Layer G — Semiconductor Materials / Wafers / Chemicals / Gases

These are second-order bottlenecks, but second-order does not mean unimportant. No wafers, photoresists, slurries, masks, gases, or ABF inputs = no AI chip supply.

Company Ticker  Role    Link    Score
Shin-Etsu Chemical      4063.T  Silicon wafers, chemicals       I       85
SUMCO   3436.T  Silicon wafers  I       82
Entegris        ENTG    Filtration, specialty materials I       81
HOYA    7741.T  EUV masks / blanks      I       80
GlobalWafers    6488.TWO        Silicon wafers  I       80
Merck KGaA      MRK.DE  Semiconductor chemicals/materials       I       78
Resonac 4004.T  Packaging/materials     I       78
Fujifilm        4901.T  Photoresists/materials  I       76
Siltronic       WAF.DE  Silicon wafers  I       76
Tokyo Ohka Kogyo        4186.T  Photoresists    I       75
Toppan  7911.T  Photomasks / packaging materials        I       74
Dai Nippon Printing     7912.T  Photomasks / materials  I       74
Sumitomo Chemical       4005.T  Materials / chemicals   I       72
Mitsui Chemicals        4183.T  Packaging/materials     I       72
Air Liquide     AI.PA   Semiconductor gases     I       72
Linde   LIN     Semiconductor gases     I       72
Mitsubishi Gas Chemical 4182.T  Chemicals / substrate materials I       70
Fujimi  5384.T  CMP slurries    I       70
Air Products    APD     Industrial gases        I       68
Dongjin Semichem        005290.KS       Photoresists/chemicals  I       65
Soulbrain       357780.KQ       Etchants/semiconductor chemicals        I       63

Investment read: Best public bottleneck quality here is Shin-Etsu, SUMCO, Entegris, HOYA, GlobalWafers, Resonac, Merck KGaA, Tokyo Ohka. Some are slow grinders, not moonshots. Still, the rails matter.

### Layer H — Rack/System Assembly, ODMs, AI Servers

NVIDIA's supply chain has moved from "ship GPU" to "ship full AI factory blocks." It named Hon Hai/Foxconn, Wistron, and Fabrinet as contractors in its annual filing. NVIDIA also announced U.S. supercomputer manufacturing with Foxconn in Houston and Wistron in Dallas, plus TSMC Arizona Blackwell production, Amkor, and SPIL.

Company Ticker  Role    Link    Score
Hon Hai / Foxconn       2317.TW / HNHPF AI server/rack manufacturing    D       92
Wistron 3231.TW AI server manufacturing D       89
Quanta Computer 2382.TW AI server ODM   I       85
Wiwynn  6669.TW AI server/rack ODM      I       83
Inventec        2356.TW Server ODM      I       80
Foxconn Industrial Internet     601138.SS       Industrial/server manufacturing I       78
Pegatron        4938.TW Server/ODM exposure     I       77
Super Micro Computer    SMCI    NVIDIA system integrator/server platform        E/I     77
Dell    DELL    AI server integrator    E/I     76
Compal  2324.TW ODM/server exposure     I       74
Gigabyte        2376.TW GPU/server systems      E/I     72
ASUSTeK 2357.TW GPU/server systems      E/I     70
HPE     HPE     AI systems / enterprise integration     E/I     70
Lenovo  0992.HK AI servers / enterprise systems E/I     68
Inspur  000977.SZ       AI server exposure, geopolitical risk   I       60

Investment read: Wistron/Wiwynn/Quanta/Foxconn are the better public ODM bottleneck basket. SMCI is higher beta but carries more headline/accounting risk. DELL/HPE are safer but less asymmetric.

### Layer I — Power, Cooling, Liquid Cooling, Rack Infrastructure

This layer is becoming as important as silicon. NVIDIA contributed GB200 NVL72 and liquid-cooled rack designs to OCP and worked with Vertiv on a reference architecture to reduce implementation time. Vertiv co-developed a 7MW GB200 NVL72 reference architecture with NVIDIA. Eaton is a recommended vendor for GB200 NVL72 cooling solutions. LITEON has shown NVIDIA GB200 NVL72 power systems tailored for Blackwell.

Company Ticker  Role    Link    Score
Vertiv  VRT     Rack power/cooling reference architecture       E       86
Delta Electronics       2308.TW Power supplies, thermal, data-center power      E/I     85
LITEON  2301.TW GB200 power systems, liquid cooling ecosystem   E       83
Eaton   ETN     Cooling/power components, GB200 recommended vendor      E       82
ABB     ABBN.SW / ABB   Electrical infrastructure / high-voltage data center power      E/I     78
Schneider Electric      SU.PA   Data-center electrical infrastructure   I       78
nVent   NVT     Enclosures, liquid cooling infrastructure       I       77
Auras   3324.TWO        Thermal modules I       77
Asia Vital Components   3017.TW Fans, cooling, thermal modules  I       76
AcBel Polytech  6282.TW Power supplies  I       73
Chicony Power   6412.TW Power supplies  I       72
Modine  MOD     Data-center thermal management  I       72
Amphenol        APH     High-speed/power connectors     I       73
TE Connectivity TEL     Connectors / power interconnect I       70
BizLink 3665.TW Cables/interconnect     I       69
Luxshare        002475.SZ       Interconnect/cables     I       66
Nidec   6594.T  Motors/fans/cooling components  I       66
Johnson Controls        JCI     HVAC / facility cooling I       66
Carrier CARR    HVAC / cooling  I       64
Legrand LR.PA   Electrical infrastructure       I       64
Advanced Energy AEIS    Precision power I       63
Bel Fuse        BELFB   Power/connectivity components   I       60

Investment read: The more direct NVDA stack is VRT, Delta, Lite-On, Eaton, Auras, AVC, nVent. This is one of the best baskets because the AI rack has become a power/cooling product with GPUs attached. Silicon gets the headlines; heat gets the invoice.

### Layer J — Networking, Photonics, Optical Transceivers, CPO

NVIDIA's silicon photonics ecosystem explicitly includes TSMC, Browave, Coherent, Corning, Fabrinet, Foxconn, Lumentum, SENKO, SPIL, Sumitomo Electric, and TFC Communication, while pluggable transceiver support includes Coherent, Eoptolink, Fabrinet, and Innolight. SENKO and TFC are not included in the scored public list unless a clean public listing is available; private/no-clear-public names are excluded. NVIDIA also agreed to invest $2B each in Lumentum and Coherent with purchase commitments and optical-capacity access rights.

Company Ticker  Role    Link    Score
Lumentum        LITE    Lasers for optical networking/CPO       D/E     88
Coherent        COHR    Lasers, transceivers, optical components        D/E     88
Fabrinet        FN      Optical manufacturing; direct contractor        D/E     84
Corning GLW     Optical fiber/glass     E       83
Sumitomo Electric       5802.T  Optical fiber/components        E       81
Browave 3163.TWO        Optical components; NVIDIA ecosystem    E       80
Eoptolink       300502.SZ       Optical transceivers    E       80
Zhongji Innolight       300308.SZ       Optical transceivers    E       80
Foxconn 2317.TW Optical/rack ecosystem + manufacturing  D/E     75
ASE / SPIL      ASX / 3711.TW   Silicon photonics packaging/test ecosystem      E       80
TSMC    TSM / 2330.TW   Silicon photonics manufacturing ecosystem       E       85
Broadcom        AVGO    Networking silicon, DSP/switch ecosystem        I       70
Marvell MRVL    DSP/custom silicon/networking   I       70
Credo   CRDO    AEC/DSP connectivity, AI networking exposure    I       68
MACOM   MTSI    Optical/RF/analog components    I       65
Semtech SMTC    Signal integrity/optical components     I       60
Applied Optoelectronics AAOI    Optical transceivers/components I       62

Investment read: Best public basket: LITE, COHR, FN, GLW, Browave, Eoptolink, Innolight, Sumitomo Electric. The smaller names are where the torque lives, but liquidity/geopolitical/China-market risks are higher.

### Layer K — Storage, SSDs, Peripheral Data Infrastructure

This is not as critical as HBM or CoWoS, but NVDA rack systems still need storage and memory hierarchy around the compute fabric.

Company Ticker  Role    Link    Score
Micron  MU      Memory + SSD/storage    D/I     74
Samsung Electronics     005930.KS       Memory + NAND/SSD       D/I     72
SK hynix / Solidigm     000660.KS       NAND/SSD + HBM  D/I     70
Kioxia  285A.T  NAND/storage    I       58
Western Digital WDC     NAND/storage    I       58
Seagate STX     Storage I       55

Investment read: Storage is relevant, but HBM is the real memory bottleneck. Don't confuse "data center storage exposure" with "NVDA choke point." That's how people accidentally buy a spoon in a gold rush.

## 3. Excluded Because Not Public / Not Cleanly Public / Delisted

Company Why excluded
CoolIT  Important liquid cooling player, but private.
SENKO   Named in NVIDIA photonics ecosystem, but not a clean public common-stock listing.
Samtec  Private interconnect/cabling supplier.
Molex   Owned by private Koch Industries.
JSR     Taken private.
Shinko Electric Historically key substrate/packaging supplier, but delisted in 2025.
TFC Communication       Named by NVIDIA ecosystem, but no clean public listing confirmed.
Boyd    Private thermal management supplier.
Celestica?      Public, but I would score it lower unless tied to a specific NVDA rack/customer program; more general AI hardware exposure than confirmed NVDA bottleneck.

## 4. Best Bottleneck Baskets by Asymmetry

### A. "Obvious spine" — highest certainty, lower hidden-alpha

Basket  Companies
Foundry / HBM / mega-cap spine  TSMC, SK hynix, Micron, Samsung, ASML, AMAT, LRCX, TEL, KLA

Use case: Core exposure. These are the toll booths. But everyone knows they are toll booths, so don't expect undiscovered mispricing by default.

### B. "Under-the-floorboards" substrate basket — highest bottleneck purity

Basket  Companies
ABF / IC substrates / CCL       Ajinomoto, Ibiden, Unimicron, Nan Ya PCB, Kinsus, AT&S, Samsung Electro-Mechanics, Elite Material, Doosan, Compeq, Gold Circuit, Zhen Ding

Use case: These are the boring picks-and-shovels that make the AI gods leave the cloud and enter physical reality. Very unsexy. Very useful. The best combo.

### C. Power/cooling "AI factory" basket

Basket  Companies
Power / cooling / rack infra    Vertiv, Delta Electronics, LITEON, Eaton, Auras, AVC, nVent, Modine, ABB, Schneider

Use case: The AI rack has become a power/cooling product with GPUs attached. Silicon gets the headlines; heat gets the invoice.

### D. Optical networking / CPO basket

Basket  Companies
Optics / photonics / networking LITE, COHR, FN, GLW, Browave, Eoptolink, Innolight, Sumitomo Electric, AVGO, MRVL, CRDO

Use case: AI networking is becoming as important as silicon. The smaller names are where the torque lives.

### E. Advanced packaging / test / metrology basket

Basket  Companies
Packaging / test / inspection   Amkor, ASE/SPIL, DISCO, Onto, Camtek, Nova, FormFactor, Advantest, BESI, Towa, KLIC, SCREEN, Lasertec

Use case: CoWoS and HBM integration are the most important short-term bottlenecks after TSMC capacity itself.

### F. ODM / rack assembly basket

Basket  Companies
AI server/rack ODMs     Foxconn, Wistron, Quanta, Wiwynn, Inventec, Pegatron, SMCI, DELL

Use case: Wistron/Wiwynn/Quanta/Foxconn are the better public ODM bottleneck basket. SMCI is higher beta but carries more headline/accounting risk.
