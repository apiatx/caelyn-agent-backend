# Phase 1 Correction Report — Treasury Auction Hardening

**Date:** 2026-07-30  
**Prior commits:** `72993b1b`, `67a0e370`, `d676f01f`  
**New commit:** `2aa05078`  
**Files changed:** `backend/services/catalyst_calendar_service.py`, `backend/tests/test_calendar_curation.py`  

---

## Problem

The previous pattern `\bauction\b AND NOT \bcorporate\b` was still too broad. It treated any US event containing the word "auction" (without "corporate") as a Treasury auction. This misclassified generic auctions like "Oil Lease Auction", "Spectrum Auction", "Government Asset Auction", and "Municipal Bond Auction" as `treasury_auction/major`.

## Fix

Require explicit Treasury security semantics alongside auction semantics. The new pattern uses two branches:

```
Branch 1: \btreasury\b AND \bauction\b   (any order, may be separated)
   → matches "10-Year Treasury Auction", "Auction of 3-Month Treasury Bills"

Branch 2: \b(?:bill\s+auction|note\s+auction|bond\s+auction)\b
          AND NOT \b(?:corporate|municipal)\b
   → matches "20-Year Bond Auction", "5-Year Note Auction"
   → rejects "Corporate Bond Auction", "Municipal Bond Auction"
```

Foreign auctions are already handled by the foreign-country gate at step 2, before this check runs.

## Regression coverage

### Positive (must be treasury_auction/major)

| Title | Result |
|---|---|
| 10-Year Treasury Auction | treasury_auction/major |
| Treasury Bill Auction | treasury_auction/major |
| 20-Year Bond Auction | treasury_auction/major |
| 5-Year Note Auction | treasury_auction/major |
| Auction of 3-Month Treasury Bills | treasury_auction/major |
| Auction of 10-Year Treasury Notes | treasury_auction/major |
| Auction of 30-Year Treasury Bonds | treasury_auction/major |

### Negative (must NOT be treasury_auction)

| Title | Country | Expected |
|---|---|---|
| Oil Lease Auction | US | other_us |
| Spectrum Auction | US | other_us |
| Government Asset Auction | US | other_us |
| Corporate Bond Auction | US | other_us |
| Municipal Bond Auction | US | other_us |
| German Bund Auction | DE | foreign/context |
| UK Gilt Auction | GB | foreign/context |
| Japanese Government Bond Auction | JP | foreign/context |
| European Bond Auction | EU | foreign/context |
| Treasury Bond Yield | US | other_us |
| Treasury Note Rate | US | other_us |
| Treasury Bill Rate | US | other_us |
| Treasury Yield Snapshot | US | treasury_snapshot/context |

## Test results

```
pytest -q backend/tests/test_calendar_curation.py  →  79 passed in 0.64s
pytest -q backend/tests/test_top_catalysts.py      →  17 passed in 0.03s
git diff --check                                    →  no output
```

4 new tests: `test_treasury_auction_of_notes`, `test_treasury_auction_of_bonds`, `test_municipal_bond_auction_not_treasury`, `test_generic_auction_not_treasury`.

## Final git status

```
## main...origin/main [ahead 4]
```

No staged changes. Only the two authorized files are in the commit. All 34+ pre-existing dirty files are unmodified and un-staged.

## Commit

```
2aa05078 Fix treasury auction classifier: require explicit Treasury security semantics
```
