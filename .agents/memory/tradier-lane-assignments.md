---
name: Tradier lane assignments post-Phase3 triage
description: Correct lane assignments for each Tradier call site after Phase 3/4A budget regression fix
---

## Rule
Screener hub OI enrichment (get_option_expirations + get_option_chain in
screener_hub_service.py) must be tagged `saved_options`, NOT `maintenance`.

The supplement loop options scan (run_live_scan in main.py ~line 12266) must be
tagged `maintenance`.

options_flow (40 RPM) is exclusively for the master screener chains.

**Why:**
maintenance at 10 RPM was saturated by OI enrichment (3 calls/symbol × continuous
screener ticking), leaving 0 budget for supplement loop. Coverage dropped to
cold-restart baseline and never recovered within a session.
Moving OI enrichment to saved_options (25 RPM, normally 0-3 used) gives supplement
loop an uncontested maintenance lane. Maintenance is now 0/20 in steady state.

**How to apply:**
- If OI enrichment moves back to maintenance, supplement scan will starve again.
- If supplement scan moves to options_flow, it competes with master screener (both
  saturate to 40/40 immediately).
- Maintenance default should be 20 RPM (raised from 10 in Phase 3 triage).
- The _tradier_key() and _tradier_base() functions in sector_rotation/providers.py
  must remain — theme_rs_service.py imports them for its unmanaged history call path.
