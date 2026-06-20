# Meta Platforms Supply-Chain Bottleneck Map
**Source date:** 2026-06-20 | **Curated by:** Phase-2 static map

## Context
Meta 2026 capex: $125B–$145B. Uses Nvidia Blackwell/GB200/GB300, AMD Instinct (6GW multi-year agreement), Broadcom MTIA custom silicon (>1GW initial deployment). Confirmed power: Entergy 5.2GW Louisiana gas plants, NextEra ~2.5GW renewables, Williams gas infrastructure, CoreWeave $21B+$14.2B capacity, Nebius $12B+$15B.
Legend: C=confirmed/direct; P=public partner/contracted; I=indirect upstream; E=ecosystem.

## Highest-Score Public Bottlenecks
| Score | Company | Ticker | Layer | Link |
|-------|---------|--------|-------|------|
| 100 | Broadcom | AVGO | MTIA/XPU/Ethernet/packaging | C |
| 99 | AMD | AMD | AI GPUs; 6GW agreement | C |
| 98 | Nvidia | NVDA | Blackwell/GB200/GB300 | C |
| 98 | TSMC | TSM/2330.TW | Foundry/CoWoS | I |
| 96 | SK hynix | 000660.KS | HBM | I |
| 95 | Arista Networks | ANET | Ethernet AI switching | C |
| 94 | Corning | GLW | Fiber optic; $6B anchor deal | C |
| 93 | Entergy | ETR | 5.2GW gas generation; confirmed | C |
| 92 | Micron | MU | HBM/DRAM/NAND | I |
| 92 | Vertiv | VRT | Power/thermal/liquid cooling | E/I |
| 91 | Samsung Electronics | 005930.KS | HBM/DRAM/NAND/foundry | I |
| 91 | Eaton | ETN | UPS/electrical | E/I |
| 90 | Schneider Electric | SU.PA | Power/cooling/energy | E/I |
| 90 | CoreWeave | CRWV | Neocloud capacity | C/P |
| 89 | Nebius | NBIS | Neocloud GPU capacity | C/P |
| 88 | Williams | WMB | Gas infrastructure; Ohio datacenter | C/P |
| 88 | NextEra Energy | NEE | Renewable PPAs; ~2.5GW | C |
| 87 | Alphabet/Google | GOOGL | Cloud capacity contracts | P |
| 86 | Coherent | COHR | Optical components/lasers | I/E |
| 86 | Lumentum | LITE | Lasers/optical components | I/E |
| 85 | GE Vernova | GEV | Gas turbines/grid equipment | E/I |
| 85 | Quanta Computer | 2382.TW | AI server/rack ODM | I |
| 84 | Hon Hai/Foxconn | 2317.TW | AI server manufacturing | I |
| 84 | Wiwynn | 6669.TW | Hyperscale AI servers | I |
| 84 | Marvell | MRVL | DSPs/custom silicon | I/E |
| 83 | ASE Technology | ASX/3711.TW | OSAT/advanced packaging | I |
| 83 | Astera Labs | ALAB | PCIe/CXL retimers | I/E |
| 83 | Credo | CRDO | AECs/SerDes | I/E |
| 82 | Amkor | AMKR | Advanced packaging/OSAT | I |
| 82 | Applied Optoelectronics | AAOI | 800G optical transceivers | E/I |
| 82 | Amphenol | APH | Connectors/cables | I/E |
| 82 | Caterpillar | CAT | Backup/on-site power | E/I |
| 81 | Cummins | CMI | Generators/power systems | E/I |
| 81 | Wistron | 3231.TW | AI server systems | I |
| 80 | Oracle | ORCL | Cloud capacity (talks) | Watch |
| 80 | TE Connectivity | TEL | Connectors/sensors/cabling | I/E |
| 80 | Super Micro | SMCI | AI servers | E/I |
| 79 | Modine | MOD | Datacenter cooling | E/I |
| 79 | Delta Electronics | 2308.TW | Power supplies/thermal | I/E |
| 78 | Powell Industries | POWL | Switchgear/electrical | E/I |
| 78 | Unimicron | 3037.TW | IC substrates | I |
| 78 | MACOM | MTSI | Optical/semi connectivity | I/E |
| 77 | Hammond Power | HPS.A.TO | Transformers | E/I |
| 77 | Ibiden | 4062.T | IC substrates | I |
| 76 | nVent | NVT | Enclosures/liquid cooling | E/I |
| 76 | Celestica | CLS | AI hardware manufacturing | E/I |
| 76 | Johnson Controls | JCI | Building systems/cooling | E/I |
| 76 | Trane Technologies | TT | HVAC/cooling | E/I |
| 75 | Soitec | SOI.PA | Engineered substrates/photonics | I/E |
| 75 | Hubbell | HUBB | Electrical infrastructure | E/I |
| 75 | SPX Technologies | SPXC | Cooling/engineered equipment | E/I |
| 74 | Dell | DELL | Servers/enterprise infra | E/I |
| 74 | HPE | HPE | Servers/networking | E/I |
| 74 | Seagate | STX | HDD/storage | I/E |
| 74 | Western Digital | WDC | HDD/NAND/storage | I/E |
| 73 | Advanced Energy Industries | AEIS | Precision power conversion | I/E |
| 72 | Tower Semiconductor | TSEM | Specialty foundry/photonics | I/E |
| 72 | GlobalFoundries | GFS | Specialty foundry/photonics | I/E |
| 72 | Semtech | SMTC | Signal integrity/connectivity | I/E |
| 70 | Enplas | 6961.T | Micro-lens arrays/test sockets | I/E |
| 70 | Aehr Test Systems | AEHR | Semiconductor test burn-in | E/I |
| 70 | MaxLinear | MXL | Connectivity semis | E/I |

## Layer 1 — AI Compute Accelerators & Custom Silicon
- AVGO, AMD, NVDA, TSM, SK hynix/000660.KS, MU, Samsung/005930.KS
- Secondary: ARM, MRVL, INTC, QCOM
- Semi tools: ASML, AMAT, LRCX, KLAC, 8035.T (TEL), ASMI.AS, BESI.AS, DISCO/6146.T, Advantest/6857.T, TER, CAMT

## Layer 2 — Foundry, CoWoS, Substrates, OSAT
- TSM, Samsung/005930.KS, INTC, GFS, TSEM
- OSAT: ASE/ASX, AMKR, Powertech/6239.TW
- Substrates: Unimicron/3037.TW, Ibiden/4062.T, Kinsus/3189.TW, Nan Ya PCB/8046.TW
- Test/inspection: Advantest/6857.T, TER, CAMT, AEHR, KLAC

## Layer 3 — HBM, DRAM, NAND, Storage
- HBM/DRAM: SK hynix/000660.KS, MU, Samsung/005930.KS
- NAND/SSD: Samsung, MU, WDC, Kioxia/285A.T
- HDD: STX, WDC
- Storage systems: NTAP, PSTG, DELL, HPE

## Layer 4 — AI Networking
- Switch systems: ANET, CSCO, HPE, Nokia/NOK
- Switch silicon/NICs: AVGO, MRVL, MTSI, CRDO, ALAB, SMTC, MXL
- Cables/connectors: APH, TEL, GLW, Bel Fuse/BELFB, Littelfuse/LFUS
- Interconnect: CIEN, Nokia/NOK, NEC/6701.T

## Layer 5 — Optical Networking
- Fiber/cable: GLW, Prysmian/PRY.MI, Furukawa/5801.T, Sumitomo/5802.T
- Transceivers: COHR, LITE, FN, AAOI, Innolight/300308.SZ, Eoptolink/300502.SZ, TFC/300394.SZ, Accelink/002281.SZ, Broadex/300548.SZ
- Lasers: COHR, LITE, MTSI, SMTC, Hamamatsu/6965.T, Yuanjie/688498.SS, Shijia/688313.SS
- Silicon photonics: SOI.PA, TSEM, GFS, HIMX, Enplas/6961.T

## Layer 6 — Power Generation & Gas Infrastructure
- Confirmed Meta: ETR (Entergy), NEE (NextEra), WMB (Williams)
- Power generation: GEV, Siemens Energy/ENR.DE, Mitsubishi Heavy/7011.T, CAT, CMI, GNRC
- IPPs: CEG, VST, TLN, NRG, AES, BEP, CWEN
- Renewables: NEE, AES, BEP, Orsted/ORSTED.CO, Iberdrola/IBE.MC, RWE/RWE.DE, EDPR/EDPR.LS
- Gas infra: WMB, ET, KMI, TRP, ENB, LNG, EQT

## Layer 7 — Electrical Infrastructure
- Power/UPS: VRT, ETN, SU.PA, ABB, Siemens, 2308.TW (Delta), LR.PA (Legrand)
- Switchgear/transformers: POWL, HPS.A.TO, HUBB, RRX, NVT
- Power semis: MPWR, VICR, AEIS, ON, STM, IFX.DE, Rohm/6963.T
- Protection/connectors: LFUS, BELFB, APH, TEL

## Layer 8 — Cooling & Thermal Management
- VRT, SU.PA, ETN, 2308.TW (Delta), TT, CARR, JCI, MOD, SPXC, MTRS.ST (Munters), ALFA.ST (Alfa Laval), BEAN.SW (Belimo), NVT, APH, TEL, FLEX, JBL

## Layer 9 — AI Servers, Rack Integration, ODMs
- Taiwan ODMs: Foxconn/2317.TW, Quanta/2382.TW, Wiwynn/6669.TW, Wistron/3231.TW, Inventec/2356.TW, Pegatron/4938.TW
- OEMs: SMCI, DELL, HPE, Lenovo/0992.HK
- EMS: CLS, JBL, FLEX, SANM, Plexus/PLXS

## Layer 10 — Neocloud / Leased Compute Capacity
- CRWV, NBIS, ORCL, EQIX, DLR, IRM, GDS, VNET

## Layer 11 — Construction, EPC, Grid
- PWR, MTZ, MYRG, EME, FIX, IESC, STRL, APG, GVA, J, ACM, WSP.TO, VMC, MLM, EXP, CRH, NUE, STLD

## Layer 12 — Reality Labs (Ray-Ban Meta, Quest, AR)
| Score | Company | Ticker | Role |
|-------|---------|--------|------|
| 84 | EssilorLuxottica | EL.PA | Ray-Ban Meta smart glasses partner |
| 82 | Qualcomm | QCOM | Snapdragon XR2/AR1 for Quest/glasses |
| 79 | Goertek | 002241.SZ | Quest manufacturing |
| 76 | Sony | 6758.T | Image sensors for XR/wearables |
| 72 | STMicroelectronics | STM | MEMS/sensors |
| 70 | BOE Technology | 000725.SZ | XR/display supply chain |
| 68 | LG Display | 034220.KS | XR/display exposure |
| 67 | Sunny Optical | 2382.HK | Lenses/camera modules |
| 65 | AAC Technologies | 2018.HK | Audio/acoustics |
| 64 | Himax Technologies | HIMX | Display drivers/microdisplay |
| 60 | Kopin Corporation | KOPN | Microdisplays AR |
