# Anthropic Supply-Chain Map — Public Companies Only

Core truth: Anthropic's real constraint is not "model talent." It is secured compute + power + packaging + HBM + networking + data-center execution. Anthropic has explicitly diversified across AWS Trainium, Google TPUs, Nvidia GPUs, Microsoft Azure, CoreWeave, and Fluidstack-built U.S. infrastructure. AWS says Project Rainier is already running Anthropic workloads with nearly 500,000 Trainium2 chips and Anthropic expected to use 1M+ Trainium2 chips; Anthropic also expanded with AWS for up to 5GW of Trainium capacity. Google/Broadcom signed a separate multi-gigawatt TPU expansion starting 2027, and Microsoft/Nvidia/Azure added a $30B Azure compute + up to 1GW path. CoreWeave also has a multi-year agreement to run Claude workloads.

My bottleneck score is not a buy rating. It ranks how hard the layer is to replace and how directly it constrains Anthropic's scaling.

Score meaning: 95–100 = existential chokepoint, 85–94 = mission-critical, 75–84 = major bottleneck, 60–74 = important but replaceable, below 60 = useful exposure, not a true bottleneck.

## 1. Master bottleneck ranking

Rank    Company Ticker  Layer   Bottleneck score        Why it matters to Anthropic
1       Amazon  AMZN    Primary cloud + Trainium        100     AWS is Anthropic's primary training/cloud partner; Project Rainier/Trainium is the largest direct compute pipe.
2       Alphabet / Google       GOOGL   TPU cloud + financing + infra   98      Google TPUs are the second strategic compute pillar; also backs Fluidstack/TeraWulf/Cipher-style infrastructure.
3       Broadcom        AVGO    Google TPU ASIC + networking silicon    97      Directly tied to Anthropic's next-gen TPU deal with Google; also core Ethernet switching silicon.
4       TSMC    TSM     Foundry + CoWoS packaging       96      AWS Trainium, Google TPU, Nvidia GPUs, Broadcom ASICs, and most AI silicon roads run through TSMC/advanced packaging.
5       Nvidia  NVDA    GPU compute + networking        95      Anthropic explicitly uses Nvidia GPUs; Azure/Claude expansion is Nvidia-powered.
6       SK hynix        000660.KS       HBM     94      HBM is mandatory for frontier accelerators; SK hynix remains the most strategically important HBM supplier.
7       ASML    ASML    EUV lithography 93      No leading-edge 3nm/2nm AI silicon scaling without ASML tools.
8       Microsoft       MSFT    Azure distribution + Nvidia compute     92      $30B Azure compute commitment and Claude distribution through Microsoft ecosystem.
9       CoreWeave       CRWV    Nvidia cloud capacity   91      Direct multi-year Anthropic compute provider; scarce production-scale GPU cloud.
10      Micron  MU      HBM / DRAM / NAND       90      HBM catch-up supplier; also broader AI memory/storage beneficiary.
11      Samsung Electronics     005930.KS       HBM / DRAM / foundry    89      Critical HBM/DRAM supplier and potential foundry alternative.
12      TeraWulf        WULF    Power-rich AI data centers      88      Fluidstack/Google-backed Lake Mariner capacity likely relevant to Anthropic's $50B buildout path.
13      Cipher Mining   CIFR    Power-rich AI data centers      86      Fluidstack/Google-backed Texas HPC/data-center capacity; possible Anthropic/Google TPU infrastructure path.
14      Vertiv  VRT     Power + liquid cooling  86      AI racks fail without power/cooling; high-density thermal infra is a gating item.
15      Arista Networks ANET    AI Ethernet networking  85      Large-scale training/inference clusters need high-performance switching fabrics.
16      Marvell MRVL    Custom silicon / optical / SerDes       84      Reported/rumored AWS Trainium custom silicon relevance; major custom ASIC and optical DSP player.
17      Applied Materials       AMAT    WFE + advanced packaging tools  84      Tools enabling leading-edge logic, HBM, packaging, hybrid bonding.
18      Lam Research    LRCX    Etch/deposition WFE     83      Critical for leading-edge logic and memory process intensity.
19      KLA     KLAC    Inspection/metrology    83      Yield bottleneck for advanced semis; no yield, no scale.
20      Tokyo Electron  8035.T  WFE     82      One of the front-end equipment majors in TSMC/memory supply chain.
21      ASM International       ASMI.AS ALD / advanced deposition       81      Atomic-layer deposition increasingly critical at advanced nodes.
22      BE Semiconductor        BESI.AS Hybrid bonding / die attach     81      Advanced packaging and hybrid bonding are increasingly strategic.
23      Advantest       6857.T  Semiconductor test      80      AI chips are expensive; test throughput/yield is a hidden bottleneck.
24      Teradyne        TER     Semiconductor test      78      More indirect than Advantest for AI, but still important test exposure.
25      Corning GLW     Fiber / optical cabling 78      Amazon, Nvidia, Meta-style AI data centers are locking down fiber; Amazon signed a multibillion-dollar Corning fiber deal.
26      Eaton   ETN     Electrical equipment    78      Switchgear, UPS, transformers, power distribution.
27      Schneider Electric      SU.PA   Electrical + cooling systems    77      End-to-end data-center power infrastructure.
28      ABB     ABBN.SW / ABB   Grid/power equipment    76      Electrification, switchgear, transformers, automation.
29      Powell Industries       POWL    Switchgear / power systems      75      Smaller, high-beta electrical bottleneck name.
30      Hubbell HUBB    Grid/electrical components      74      Utility/grid equipment supplier.
31      Quanta Computer 2382.TW AI server ODM   74      Hyperscale AI server manufacturing exposure.
32      Foxconn / Hon Hai       2317.TW AI server/rack manufacturing    74      Major AI server/rack ODM scale.
33      Wiwynn  6669.TW Cloud server ODM        73      Hyperscale server exposure.
34      Wistron 3231.TW AI server manufacturing 72      Nvidia/AI server manufacturing capacity.
35      Inventec        2356.TW Server ODM      70      Server manufacturing exposure.
36      Dell    DELL    AI servers/storage      70      Large AI server backlog and enterprise AI infra.
37      Super Micro     SMCI    AI servers/liquid cooling       69      Fast AI server integrator, but less direct to Anthropic hyperscale custom buildouts.
38      HPE     HPE     Servers + Juniper networking    68      Enterprise AI infra and networking exposure.
39      Cisco   CSCO    Networking      67      AI networking, optics, enterprise/hyperscale adjacency.
40      Ciena   CIEN    Optical transport       67      DCI/backbone optical connectivity.
41      Coherent        COHR    Optical modules/lasers  66      800G/1.6T optical component exposure.
42      Lumentum        LITE    Lasers/optical components       65      Optical connectivity and datacenter photonics.
43      Fabrinet        FN      Optical manufacturing   64      Contract manufacturing for optical modules.
44      Innolight       300308.SZ       Optical transceivers    64      Big AI optical transceiver supplier; China listing.
45      Eoptolink       300502.SZ       Optical transceivers    63      800G/1.6T optical module exposure.
46      Western Digital WDC     Nearline HDD / storage  63      AI data lakes/checkpoints drive storage demand; HDD supply is tightening.
47      Seagate STX     Nearline HDD    63      Same AI data/storage bottleneck; strong pricing power.
48      Pure Storage    PSTG    High-performance storage        61      AI storage systems; less direct to Anthropic cloud buildouts.
49      NetApp  NTAP    Enterprise/cloud storage        59      More enterprise AI than frontier compute bottleneck.
50      AMD     AMD     GPUs / CPUs / networking adjacency      58      Important AI compute supplier, but not a confirmed core Anthropic platform versus Trainium/TPU/Nvidia.

## 2. Direct Anthropic exposure bucket

These are the names where I would start. They are not all cheap, but they are closest to the money flow.

Company Ticker  Directness      Bottleneck score        Notes
Amazon  AMZN    Confirmed direct        100     AWS primary cloud/training partner; Trainium2/3; Project Rainier; up to 5GW.
Alphabet        GOOGL   Confirmed direct        98      Google Cloud TPUs; Google-backed financing around Fluidstack/TeraWulf/Cipher.
Broadcom        AVGO    Confirmed direct via Google TPU deal    97      TPU ASIC supplier/design partner and AI networking silicon.
Microsoft       MSFT    Confirmed direct        92      Claude on Azure; $30B compute capacity; Nvidia-powered.
Nvidia  NVDA    Confirmed direct/partner        95      Anthropic uses Nvidia GPUs; Microsoft/Azure deal is Nvidia-powered.
CoreWeave       CRWV    Confirmed direct        91      Multi-year Anthropic cloud compute agreement.
TeraWulf        WULF    Indirect/likely infrastructure  88      Fluidstack + Google-backed Lake Mariner; likely relevant to Anthropic's U.S. custom infra path, but not officially named by Anthropic as the site owner.
Cipher Mining   CIFR    Indirect/likely infrastructure  86      Fluidstack + Google-backed Texas capacity; likely relevant, same caveat.

The Fluidstack angle matters because Anthropic announced a $50B U.S. infrastructure buildout with Fluidstack in Texas and New York, with sites coming online through 2026. TeraWulf has Google-backed Fluidstack leases at Lake Mariner, and Cipher has a Google-backed Fluidstack Texas agreement. That is not "Anthropic officially named WULF/CIFR." It is high-probability infrastructure adjacency. Translation: interesting, but don't tattoo it on your forehead.

## 3. Layer-by-layer map

### A. Cloud / compute platforms

Ticker  Company Score   Role
AMZN    Amazon / AWS    100     Primary cloud, Trainium, Project Rainier, Bedrock distribution.
GOOGL   Alphabet / Google Cloud 98      TPUs, Google Cloud, Vertex/Marketplace, infrastructure financing.
MSFT    Microsoft Azure 92      Claude on Azure/Microsoft Foundry, enterprise distribution, Nvidia compute.
CRWV    CoreWeave       91      Nvidia cloud capacity for Claude workloads.
NVDA    Nvidia  95      GPUs, networking, Azure-powered Claude path.
AVGO    Broadcom        97      Google TPU supply/design, AI switching silicon.

Bottleneck read: AMZN/GOOGL/AVGO/NVDA/MSFT/CRWV are the top "Anthropic compute cartel." If Claude demand keeps exploding, these are the public pipes.

### B. AI accelerators and custom silicon

Ticker  Company Score   Exposure
AMZN    Amazon / Annapurna Labs 100     Trainium/Inferentia platform used by Anthropic.
GOOGL   Google  98      TPU platform, including Ironwood/next-gen capacity.
AVGO    Broadcom        97      Co-develops/supplies Google custom AI chips through 2031; Anthropic TPU capacity tied to Broadcom/Google.
NVDA    Nvidia  95      GPUs used by Anthropic via cloud partners; Azure deal powered by Nvidia.
MRVL    Marvell 84      Custom silicon/SerDes/optical DSP; reported AWS Trainium custom-silicon relevance.
AMD     AMD     58      AI GPU/CPU player, but weaker direct Anthropic evidence.

Winner-takeaway: AVGO is the cleanest public "custom AI silicon" derivative on Anthropic outside AMZN/GOOGL themselves.

### C. Foundry, CoWoS, advanced packaging

Ticker  Company Score   Role
TSM     TSMC    96      Leading-edge foundry and CoWoS advanced packaging.
INTC    Intel   68      Potential advanced packaging alternative via EMIB; less direct today.
3711.TW / ASX   ASE Technology  73      OSAT/advanced packaging ecosystem.
AMKR    Amkor   70      OSAT advanced packaging; potential overflow capacity.
600584.SS       JCET    55      China OSAT; less useful for U.S.-controlled frontier AI supply chain.

TSMC's CoWoS integrates logic chiplets with HBM stacks for AI/supercomputing packages. CoWoS and HBM remain two of the most important AI infrastructure bottlenecks; multiple sources describe CoWoS as oversubscribed into 2026.

### D. HBM / DRAM / memory

Ticker  Company Score   Role
000660.KS       SK hynix        94      Leading HBM supplier; critical to Nvidia and AI accelerators.
MU      Micron  90      HBM3E/HBM4 ramp; U.S. memory exposure.
005930.KS       Samsung Electronics     89      HBM/DRAM/NAND; catch-up supplier.
WDC     Western Digital 63      AI storage/nearline HDD, not HBM.
STX     Seagate 63      AI storage/nearline HDD, not HBM.
SIMO    Silicon Motion  55      SSD controllers; indirect NAND/data-center storage exposure.

SK hynix recently shipped next-gen HBM4E samples, and Reuters still frames SK hynix as the primary Nvidia HBM supplier competing with Samsung and Micron. For Anthropic, HBM is not optional. Every serious compute path—GPU, TPU, Trainium—needs high-bandwidth memory.

### E. Semiconductor equipment chokepoints

Ticker  Company Score   Role
ASML    ASML    93      EUV lithography monopoly for leading-edge nodes.
AMAT    Applied Materials       84      Deposition, etch, packaging, hybrid bonding.
LRCX    Lam Research    83      Etch/deposition for logic and memory.
KLAC    KLA     83      Inspection/metrology/yield.
8035.T  Tokyo Electron  82      Etch/deposition/clean/coater-developer.
ASMI.AS ASM International       81      ALD, advanced deposition.
BESI.AS BE Semiconductor        81      Hybrid bonding, die attach.
6857.T  Advantest       80      AI chip test.
TER     Teradyne        78      Semiconductor test.
FORM    FormFactor      66      Probe cards/test interface.
CAMT    Camtek  65      Inspection/metrology, advanced packaging exposure.
NVMI    Nova    64      Metrology/process control.
ONTO    Onto Innovation 63      Packaging/metrology/process control.
MKSI    MKS Instruments 62      Subsystems/process solutions.
UCTT    Ultra Clean     58      Subsystems and services for WFE.

Semicap is not "Anthropic-specific," but it is the upstream constraint that decides whether TSMC, SK hynix, Micron, Samsung, and Broadcom can add capacity. Chip equipment names have been rallying on AI-driven WFE expectations.

### F. ABF substrates, interposers, package materials

Ticker  Company Score   Role
2802.T  Ajinomoto       82      ABF film material; hidden substrate chokepoint.
4062.T  Ibiden  79      High-end IC substrates.
3037.TW Unimicron       76      ABF substrates.
3189.TW Kinsus  70      IC substrates.
8046.TW Nan Ya PCB      68      Substrate/PCB exposure.
6971.T  Kyocera 67      Ceramic/substrate/packaging materials.
ATS.VI  AT&S    66      High-end substrates/PCBs.
TTMI    TTM Technologies        60      PCB/data-center electronics exposure.

This is the "boring thing that breaks the shiny thing" layer. Advanced AI packages need high-end substrates; if ABF/substrates are short, chips can exist in theory and still not ship in volume.

### G. Networking silicon, switches, optics

Ticker  Company Score   Role
AVGO    Broadcom        88      Tomahawk/Jericho Ethernet silicon; TPU ecosystem.
NVDA    Nvidia  86      Spectrum-X, InfiniBand/Ethernet, NICs.
ANET    Arista  85      AI Ethernet switch systems.
MRVL    Marvell 80      Optical DSP, SerDes, custom silicon.
CSCO    Cisco   67      Networking systems.
HPE     HPE / Juniper   68      Juniper switching/routing + AI networking.
CIEN    Ciena   67      Optical transport/DCI.
GLW     Corning 78      Fiber/cabling; hyperscaler optical bottleneck.
COHR    Coherent        66      Lasers/transceivers/optical components.
LITE    Lumentum        65      Lasers/photonic components.
FN      Fabrinet        64      Optical module manufacturing.
300308.SZ       Innolight       64      Optical transceivers.
300502.SZ       Eoptolink       63      Optical transceivers.
002281.SZ       Accelink        58      Optical components/modules.

AI clusters are becoming network-limited. Broadcom's Jericho4 was designed for distributed AI computing across data centers, and Arista is pushing 800G/3.2T AI networking systems.

### H. AI server, rack, and system integration

Ticker  Company Score   Role
2317.TW Foxconn / Hon Hai       74      AI server/rack manufacturing.
2382.TW Quanta  74      Hyperscale AI server ODM.
6669.TW Wiwynn  73      Cloud server ODM.
3231.TW Wistron 72      AI server manufacturing.
2356.TW Inventec        70      Server ODM.
DELL    Dell    70      AI server/storage systems.
SMCI    Super Micro     69      AI server integration/liquid cooling.
HPE     HPE     68      Enterprise AI servers/networking.
LNVGY / 0992.HK Lenovo  61      AI servers, enterprise systems.
PENG    Penguin Solutions       58      Niche AI/HPC integration.

AWS Project Rainier specifically required Taiwan supply-chain mobilization from chip design and advanced packaging to server assembly. That keeps ODMs relevant even if Anthropic never signs their purchase orders directly.

### I. Data-center power, cooling, electrical equipment

Ticker  Company Score   Role
VRT     Vertiv  86      Power, thermal, liquid cooling.
ETN     Eaton   78      Switchgear, UPS, power distribution.
SU.PA   Schneider Electric      77      Data-center power/cooling systems.
ABB / ABBN.SW   ABB     76      Grid, switchgear, automation.
POWL    Powell Industries       75      Switchgear/electrical systems.
HUBB    Hubbell 74      Grid/electrical components.
LEGRY / LR.PA   Legrand 70      PDUs, racks, data-center electrical.
ROK     Rockwell Automation     63      Industrial automation.
EMR     Emerson 62      Automation/power infrastructure adjacency.
TT      Trane Technologies      61      HVAC/cooling infrastructure.
JCI     Johnson Controls        60      HVAC/building systems.
CARR    Carrier 59      Cooling/HVAC.
MOD     Modine  58      Thermal management; smaller high-beta cooling exposure.

Power is now the ugliest bottleneck. Goldman estimates U.S. data-center power demand rising from 31GW in 2025 to 66GW in 2027, and FERC is pushing grid operators to overhaul connection rules for large loads like AI data centers. Data-center electrical equipment demand is projected to surge, with transformers/switchgear becoming long-lead constraints.

### J. Power-rich data-center developers / neocloud / colocation

Ticker  Company Score   Role
CRWV    CoreWeave       91      Direct Anthropic compute deal; GPU cloud.
WULF    TeraWulf        88      Google/Fluidstack-backed Lake Mariner AI capacity.
CIFR    Cipher Mining   86      Google/Fluidstack-backed Texas AI capacity.
NBIS    Nebius  73      Neocloud capacity; not confirmed Anthropic-specific.
CORZ    Core Scientific 72      AI/HPC colocation via CoreWeave ecosystem; not Anthropic-specific.
IREN    IREN    70      Power-rich AI data-center pivot; no direct Anthropic link.
APLD    Applied Digital 69      AI data-center developer; no direct Anthropic link.
WULF    TeraWulf        88      Repeated because it belongs in both Anthropic-adjacent and power-rich buckets.
RIOT    Riot Platforms  57      Power assets, but weaker AI conversion visibility.
CLSK    CleanSpark      52      Power/crypto assets, less proven AI hosting.
HUT     Hut 8   55      AI hosting optionality, weaker directness.
BTDR    Bitdeer 54      Power/compute optionality, weaker Anthropic link.
EQIX    Equinix 62      Colocation giant; less fit for hyperscale frontier training.
DLR     Digital Realty  62      Data-center REIT; less direct to Anthropic-specific compute.

This is where the smaller public names live. WULF and CIFR are the best Anthropic-adjacent small/mid-cap infrastructure angles because of the Fluidstack + Google financing trail. But they are execution-risk monsters. A great contract plus bad delivery is still a bad trade wearing a tuxedo.

### K. Storage / data layer

Ticker  Company Score   Role
STX     Seagate 63      Nearline HDD capacity/pricing.
WDC     Western Digital 63      Nearline HDD capacity/pricing.
PSTG    Pure Storage    61      High-performance AI storage.
NTAP    NetApp  59      Enterprise/cloud storage.
DELL    Dell    58      Storage + AI servers.
IBM     IBM     55      Storage/software/enterprise AI, weaker Anthropic bottleneck.

Storage is not the top Anthropic bottleneck, but it is becoming tighter as AI training and inference create huge data/checkpoint/logging needs. Western Digital and Seagate have rallied on HDD shortages tied to data centers and AI.

## 4. Best "public-only Anthropic bottleneck basket"

If I were building a focused research basket—not blindly buying today—I'd divide it like this:

### Tier 1: Highest-confidence Anthropic rails

AMZN, GOOGL, AVGO, TSM, NVDA, MSFT, CRWV

These are the clearest public beneficiaries of Anthropic's scaling. They are also mostly mega-cap or already crowded, so upside may be less explosive.

### Tier 2: Critical upstream bottlenecks

SK hynix, MU, ASML, AMAT, LRCX, KLAC, ASMI, BESI, Advantest, Ajinomoto, Ibiden, Unimicron

This is the "shovels for the shovel makers" layer. Less headline excitement, more real bottleneck.

### Tier 3: Power/data-center asymmetry

WULF, CIFR, VRT, ETN, POWL, HUBB, Schneider, ABB

This is probably where the most asymmetric public opportunities sit, because Anthropic's compute contracts are now measured in gigawatts, not "number of GPUs." The catch: construction execution, financing, dilution, and grid interconnect risk can absolutely nuke weaker players.

### Tier 4: Optical/networking upside

ANET, AVGO, MRVL, GLW, COHR, LITE, FN, CIEN, HPE, CSCO

If clusters scale from single-campus to multi-campus training/inference, bandwidth becomes a tax on everything. Optical and Ethernet are not side quests anymore.

## 5. Smaller players not to ignore

These are the "don't leave out the little guys" names worth tracking:

Ticker  Company Why it matters  Score
WULF    TeraWulf        Google/Fluidstack-backed Lake Mariner AI data-center capacity   88
CIFR    Cipher Mining   Google/Fluidstack-backed Texas AI capacity      86
POWL    Powell Industries       Switchgear/power equipment bottleneck   75
MOD     Modine  Cooling/thermal management      58
PENG    Penguin Solutions       Niche HPC/AI integration        58
CAMT    Camtek  Advanced packaging inspection/metrology 65
NVMI    Nova    Process control/metrology       64
ONTO    Onto Innovation Packaging/metrology     63
FORM    FormFactor      Probe cards/test        66
UCTT    Ultra Clean     Semicap subsystems      58
TTMI    TTM Technologies        PCBs/data-center electronics    60
FN      Fabrinet        Optical manufacturing   64
LITE    Lumentum        Datacenter lasers/photonics     65
COHR    Coherent        Lasers/transceivers/materials   66
ATS.VI  AT&S    High-end substrates     66
3189.TW Kinsus  IC substrates   70
8046.TW Nan Ya PCB      PCB/substrate exposure  68

## 6. The real bottleneck hierarchy

My ranking of what actually constrains Anthropic:

Secured gigawatts of compute — AWS, Google, Microsoft/Nvidia, CoreWeave.
Power/interconnection — grid access, transformers, switchgear, liquid cooling.
Advanced packaging / CoWoS / HBM integration — TSMC, SK hynix, Micron, Samsung, equipment suppliers.
Custom AI silicon — Trainium, TPU, Nvidia GPUs, Broadcom/Marvell.
Networking bandwidth — Ethernet, optics, fiber, DCI.
Server/rack manufacturing — Taiwan ODMs, Dell/SMCI/HPE.
Storage — HDD/SSD/performance storage.
Software distribution — AWS Bedrock, Google Cloud, Azure Foundry, Microsoft Copilot/GitHub ecosystem.

The punchline: Anthropic is becoming a multi-cloud, multi-chip, multi-gigawatt infrastructure buyer. The market will probably overfocus on "which model is best" and underprice the boring constraints that decide whether Claude can actually serve demand. That is classic power-law investing: a few structural bottlenecks capture disproportionate economics.

## 7. Trade/investing overlay

Bias: Bullish long-term on the Anthropic infrastructure chain. Highest conviction is AMZN / GOOGL / AVGO / TSM / NVDA / CRWV / WULF / CIFR / VRT / SK hynix / MU.

Macro overlay: Neutral-to-risk-on. QQQ and SPY are both trading near highs in the latest quote snapshot, while long bonds are not signaling a full panic bid. But AI infrastructure is crowded, and crowded trades can punch you in the mouth before paying rent.

Technical summary: I would not buy this entire basket blindly. For individual names, I'd require Stage 2 structure: above major moving averages, breakout or controlled pullback, volume confirmation, and a defined stop. Your own Stage Analysis notes emphasize that the best opportunities come from Stage 1-to-Stage 2 breakouts with volume, and that Stage 4 names should be avoided.

Best setup:
For asymmetric upside, the cleanest research targets are:

WULF / CIFR — highest small/mid-cap Anthropic-adjacent data-center torque.
AVGO — cleanest direct TPU/custom silicon derivative.
VRT / POWL / ETN — power equipment bottleneck.
SK hynix / MU — HBM constraint.
GLW / COHR / LITE / FN — optical/fiber bandwidth bottleneck.

Invalidation:
For WULF/CIFR specifically: failed construction milestones, financing stress, dilution worse than modeled, Google/Fluidstack lease changes, or inability to secure power/interconnect. For AVGO/NVDA/TSM: Anthropic/Google/AWS spend slowdown or evidence that compute demand is overbuilt.

Social sentiment overlay: Hot to euphoric in AI infra. Crowd risk is high in NVDA, AVGO, VRT, CRWV, WULF, CIFR. High sentiment does not kill the thesis, but it kills sloppy entries.

Trade grade:
As a theme, Anthropic critical infrastructure scores A / 86.
As a current blanket buy, it scores C because many names are extended/crowded and need chart confirmation.

Position size:
For a concentrated basket, I would approach it like this:

Tier 1 confirmed rails (AMZN, GOOGL, AVGO, NVDA, MSFT, CRWV): largest sizing, but even here use defined stops and don't chase vertical moves.
Tier 2 upstream semis (TSM, SK hynix, MU, ASML, AMAT, LRCX, KLAC): medium sizing; less volatile, but less asymmetric.
Tier 3 power/infra (WULF, CIFR, VRT, ETN, POWL): smaller sizing; highest torque, highest execution risk.
Tier 4 optics/networking (ANET, COHR, LITE, FN, GLW, MRVL): medium-small; real bottlenecks, but often already known and partially priced.

Portfolio risk check: This entire theme trades as one correlated AI-infrastructure factor in risk-off scenarios. Do not let it become more than 25–30% of a concentrated portfolio. Correlation-adjusted risk is what kills you here, not individual position size.

Biggest risk to the thesis: Anthropic's model revenue disappoints (ARC-AGI plateau risk, GPT-5/Gemini competition), AWS/Google cut infrastructure commitments, power interconnection delays push data-center timelines 12–24 months, or AI capex broadly disappoints after the 2025–2026 rush.
