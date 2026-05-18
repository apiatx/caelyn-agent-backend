"""Options transaction parsing and ledger engine.

Parses OCC-style option rows from brokerage CSVs and applies average-cost
accounting per option contract (OCC key). Completely separate from the
stock/equity ledger — option rows never pollute stock position outputs.

Action mapping (brokerage → canonical):
  buy to open   → BUY_OPEN    opens / increases a long option position
  sell to close → SELL_CLOSE  closes / reduces a long option position
  sell to open  → SELL_OPEN   short position (tracked basic, no full accounting)
  buy to close  → BUY_CLOSE   closes a short position (basic)
  expired       → EXPIRED     option expires worthless — full loss of cost basis

Contract size: CONTRACT_MULTIPLIER = 100 (standard US equity options).
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
import uuid
from collections import defaultdict
from datetime import date, datetime
from typing import Any

CONTRACT_MULTIPLIER = 100
CONTRACTS_EPSILON   = 0.001   # residuals below this are treated as fully closed

_LONG_OPEN_ACTIONS   = frozenset({"buy to open", "opening purchase"})
_LONG_CLOSE_ACTIONS  = frozenset({"sell to close", "closing sale"})
_SHORT_OPEN_ACTIONS  = frozenset({"sell to open", "opening sale", "opening short"})
_SHORT_CLOSE_ACTIONS = frozenset({"buy to close", "closing purchase"})
_EXPIRED_ACTIONS     = frozenset({"expired", "option expired"})

_ALL_OPTION_ACTIONS = (
    _LONG_OPEN_ACTIONS | _LONG_CLOSE_ACTIONS |
    _SHORT_OPEN_ACTIONS | _SHORT_CLOSE_ACTIONS |
    _EXPIRED_ACTIONS
)


def _is_option_action(action: str) -> bool:
    return action.strip().lower() in _ALL_OPTION_ACTIONS


def _classify_option_action(action: str) -> str:
    a = action.strip().lower()
    if a in _LONG_OPEN_ACTIONS:   return "BUY_OPEN"
    if a in _LONG_CLOSE_ACTIONS:  return "SELL_CLOSE"
    if a in _SHORT_OPEN_ACTIONS:  return "SELL_OPEN"
    if a in _SHORT_CLOSE_ACTIONS: return "BUY_CLOSE"
    if a in _EXPIRED_ACTIONS:     return "EXPIRED"
    return "UNKNOWN"


# ── OCC symbol parsers ────────────────────────────────────────────────────────

# Pattern 1 — Schwab human-readable: "BWEN 07/17/2026 2.50 C"
#   or two-digit year:                "BWEN 07/17/26 2.50 C"
_OCC_READABLE_RE = re.compile(
    r'^([A-Z0-9]{1,6})\s+'         # underlying ticker
    r'(\d{1,2}/\d{2}/\d{2,4})\s+' # expiration  MM/DD/YYYY or MM/DD/YY
    r'([\d.]+)\s+'                  # strike price
    r'([CP])$',                     # C = CALL, P = PUT
    re.IGNORECASE,
)

# Pattern 2 — OCC compact: "BWEN260717C00002500"
#   underlying (up to 6) + YYMMDD + C/P + 8-digit strike (× 1000)
_OCC_COMPACT_RE = re.compile(
    r'^([A-Z0-9]{1,6})(\d{6})([CP])(\d{8})$',
    re.IGNORECASE,
)


def parse_option_symbol(raw_symbol: str) -> dict | None:
    """Parse an OCC-style option symbol into structured metadata.

    Returns None if the symbol cannot be recognised as an option contract.

    Returned dict keys:
      underlying, expiration_date (ISO YYYY-MM-DD), strike (float),
      option_type (CALL | PUT), occ_key, display_symbol
    """
    s = raw_symbol.strip()

    # ── Pattern 1: human-readable ─────────────────────────────────────────
    m = _OCC_READABLE_RE.match(s)
    if m:
        underlying, exp_raw, strike_raw, cp = m.groups()
        underlying = underlying.upper()
        strike     = float(strike_raw)
        opt_type   = "CALL" if cp.upper() == "C" else "PUT"

        exp_date: str | None = None
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                exp_date = datetime.strptime(exp_raw, fmt).date().isoformat()
                break
            except ValueError:
                pass
        if not exp_date:
            return None

        occ_key = f"{underlying}_{exp_date}_{strike:.4f}_{opt_type}"
        display = f"{underlying} {exp_raw} {strike_raw} {'C' if opt_type == 'CALL' else 'P'}"
        return {
            "underlying":      underlying,
            "expiration_date": exp_date,
            "strike":          strike,
            "option_type":     opt_type,
            "occ_key":         occ_key,
            "display_symbol":  display,
        }

    # ── Pattern 2: OCC compact ────────────────────────────────────────────
    m = _OCC_COMPACT_RE.match(s)
    if m:
        underlying, yymmdd, cp, strike_raw = m.groups()
        underlying = underlying.upper()
        strike     = int(strike_raw) / 1000.0
        opt_type   = "CALL" if cp.upper() == "C" else "PUT"
        try:
            exp_date = datetime.strptime(yymmdd, "%y%m%d").date().isoformat()
        except ValueError:
            return None

        occ_key = f"{underlying}_{exp_date}_{strike:.4f}_{opt_type}"
        display = f"{underlying} {exp_date} {strike:.2f} {'C' if opt_type == 'CALL' else 'P'}"
        return {
            "underlying":      underlying,
            "expiration_date": exp_date,
            "strike":          strike,
            "option_type":     opt_type,
            "occ_key":         occ_key,
            "display_symbol":  display,
        }

    return None


# ── Shared helpers ────────────────────────────────────────────────────────────

def _parse_float(raw: str) -> float:
    """Parse numeric strings including -$1,234.56 and $(1,234.56)."""
    if not raw:
        return 0.0
    s = raw.strip().replace(",", "").replace(" ", "")
    negative = False
    if s.startswith("-"):
        negative = True
        s = s[1:]
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    s = s.lstrip("$")
    try:
        val = float(s)
        return -val if negative else val
    except (ValueError, TypeError):
        return 0.0


def _parse_date(raw: str) -> str | None:
    if not raw:
        return None
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass
    if "T" in raw:
        return _parse_date(raw.split("T")[0])
    return None


def _option_fingerprint(
    occ_key:    str,
    trade_date: str | None,
    action:     str,
    contracts:  float,
    premium:    float,
) -> str:
    key = "|".join([
        occ_key,
        (trade_date or "")[:10],
        action,
        f"{round(contracts, 4):.4f}",
        f"{round(premium, 6):.6f}",
    ])
    return hashlib.md5(key.encode()).hexdigest()


# ── Step 1: Normalize option rows from CSV ────────────────────────────────────

def normalize_option_rows(csv_text: str, source_file: str = "") -> dict:
    """Parse option rows from a brokerage CSV into normalized objects.

    Identifies rows by option action phrase (Buy to Open, Sell to Close, etc.)
    OR by OCC-style symbol pattern (ticker + space + date + strike + C/P).
    Stock rows are not touched — this function is additive only.

    Returns::

        {
          "transactions":          list[dict],  # normalized option txns
          "ignored":               list[dict],  # skipped rows
          "errors":                list[dict],  # symbol/action parse errors
          "rows_total":            int,
          "rows_parsed":           int,
          "option_rows_detected":  int,
        }
    """
    lines = csv_text.splitlines()

    # Locate the CSV header row (first non-empty line parseable as headers)
    header_idx: int | None = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            sample = next(csv.reader([stripped]))
            if len(sample) >= 4 and any(h.strip() for h in sample):
                header_idx = i
                break
        except Exception:
            pass

    if header_idx is None:
        return {
            "transactions": [], "ignored": [], "errors": [],
            "rows_total": 0, "rows_parsed": 0, "option_rows_detected": 0,
        }

    reader   = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
    raw_cols = reader.fieldnames or []

    def _find(*candidates: str) -> str | None:
        lc = {c.strip().lower(): c for c in raw_cols}
        for name in candidates:
            if name.lower() in lc:
                return lc[name.lower()]
        return None

    col_date   = _find("date", "trade date", "as of date", "settled date")
    col_action = _find("action", "type", "transaction type", "trans type")
    col_symbol = _find("symbol", "ticker", "security")
    col_desc   = _find("description", "desc", "security description")
    col_qty    = _find("quantity", "qty", "shares", "contracts")
    col_price  = _find("price", "price per share", "exec price")
    col_fees   = _find("fees & comm", "fees & commissions", "fees", "commission", "fee")
    col_amount = _find("amount", "net amount", "total amount", "value")

    def _v(row: dict, col: str | None) -> str:
        return (row.get(col, "") or "").strip() if col else ""

    transactions:        list[dict] = []
    ignored:             list[dict] = []
    errors:              list[dict] = []
    option_rows_detected             = 0
    row_index                        = 0

    for row in reader:
        row_index   += 1
        raw_action   = _v(row, col_action)
        raw_symbol   = _v(row, col_symbol)
        raw_desc     = _v(row, col_desc)

        is_opt_action  = _is_option_action(raw_action)
        # OCC readable symbol: "BWEN 07/17/2026 2.50 C" — space followed by digit
        is_opt_symbol  = bool(re.search(r'\s+\d', raw_symbol))

        if not is_opt_action and not is_opt_symbol:
            continue   # not an option row — stock ledger handles it

        option_rows_detected += 1

        # Parse OCC symbol metadata
        parsed_sym = parse_option_symbol(raw_symbol)
        if parsed_sym is None:
            errors.append({
                "row_index":  row_index,
                "raw_symbol": raw_symbol,
                "raw_action": raw_action,
                "reason":     f"Cannot parse option symbol: {raw_symbol!r}",
            })
            continue

        # Classify action
        opt_action = _classify_option_action(raw_action)
        if opt_action == "UNKNOWN":
            errors.append({
                "row_index": row_index,
                "occ_key":   parsed_sym["occ_key"],
                "raw_action": raw_action,
                "reason":    f"Unknown option action: {raw_action!r}",
            })
            continue

        # Parse quantity (contracts)
        qty_raw = _v(row, col_qty).replace(",", "").replace("(", "-").replace(")", "")
        try:
            contracts = abs(float(qty_raw)) if qty_raw else 0.0
        except (ValueError, TypeError):
            contracts = 0.0

        # Parse premium (per-share price, multiply × 100 for contract value)
        premium = abs(_parse_float(_v(row, col_price)))
        fees    = abs(_parse_float(_v(row, col_fees))) if col_fees else 0.0
        amount  = abs(_parse_float(_v(row, col_amount)))

        # Back-calculate premium from amount if missing
        # BUY: amount = contracts × premium × 100 + fees
        # SELL: amount = contracts × premium × 100 − fees
        if premium == 0 and contracts > 0:
            net_notional = amount - fees if amount > fees else amount
            if net_notional > 0:
                premium = round(net_notional / (contracts * CONTRACT_MULTIPLIER), 6)

        # EXPIRED with qty=0 or blank — use contracts_open from context; store 0
        # The ledger engine handles the actual accounting.

        if contracts == 0 and opt_action not in ("EXPIRED",):
            ignored.append({
                "row_index": row_index,
                "occ_key":   parsed_sym["occ_key"],
                "reason":    "zero contracts",
            })
            continue

        trade_date = _parse_date(_v(row, col_date))
        fp         = _option_fingerprint(
            parsed_sym["occ_key"], trade_date, opt_action, contracts, premium
        )

        transactions.append({
            "transaction_id":      str(uuid.uuid4()),
            "fingerprint":         fp,
            "source_file":         source_file,
            "raw_row_index":       row_index,
            # ── Contract metadata ──────────────────────────────────────────
            **parsed_sym,
            # ── Transaction fields ─────────────────────────────────────────
            "trade_date":          trade_date,
            "raw_action":          raw_action,
            "option_action":       opt_action,
            "contracts":           contracts,
            "premium":             premium,
            "fees":                fees,
            "gross_amount":        amount,
            "contract_multiplier": CONTRACT_MULTIPLIER,
            "raw_description":     raw_desc,
        })

    return {
        "transactions":         transactions,
        "ignored":              ignored,
        "errors":               errors,
        "rows_total":           row_index,
        "rows_parsed":          len(transactions),
        "option_rows_detected": option_rows_detected,
    }


# ── Step 2: Deduplicate ───────────────────────────────────────────────────────

def deduplicate_option_transactions(
    transactions: list[dict],
    existing_fingerprints: set[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (unique, duplicates). Fingerprint-based dedup."""
    seen: set[str] = set(existing_fingerprints or set())
    unique: list[dict] = []
    dupes:  list[dict] = []
    for txn in transactions:
        fp = txn.get("fingerprint") or ""
        if fp and fp in seen:
            dupes.append(txn)
        else:
            unique.append(txn)
            if fp:
                seen.add(fp)
    return unique, dupes


# ── Step 3: Build per-contract option ledgers (average-cost) ─────────────────

def build_option_ledgers(transactions: list[dict]) -> dict[str, dict]:
    """Apply average-cost accounting per OCC key.

    Returns dict[occ_key → ledger]. Each ledger::

        {
          occ_key, underlying, expiration_date, strike, option_type,
          display_symbol, contracts_open, avg_premium, cost_basis,
          contracts_bought, contracts_sold, total_buy_cost, realized_pnl,
          first_entry_date, last_entry_date, last_exit_date,
          closed_events, accounting_errors, final_status,
          is_short,
        }

    final_status values:
      "open"                    — contracts remain, no sells yet
      "partially_closed_open"   — contracts remain, some sold
      "fully_closed"            — all contracts closed via Sell to Close
      "expired"                 — position expired worthless
      "short_option_tracked_basic" — Sell to Open position (no full accounting)
      "orphan_expired"          — Expired row with no opening Buy to Open found
    """
    by_key: dict[str, list[dict]] = defaultdict(list)
    for txn in transactions:
        by_key[txn["occ_key"]].append(txn)

    ledgers: dict[str, dict] = {}

    for occ_key, txns in by_key.items():
        txns_sorted = sorted(
            txns,
            key=lambda t: (t.get("trade_date") or "9999-12-31", t["option_action"]),
        )

        first           = txns_sorted[0]
        underlying      = first["underlying"]
        expiration_date = first["expiration_date"]
        strike          = first["strike"]
        option_type     = first["option_type"]
        display_symbol  = first["display_symbol"]
        mult            = CONTRACT_MULTIPLIER

        contracts_open:   float = 0.0
        cost_basis:       float = 0.0
        avg_premium:      float = 0.0
        total_buy_cost:   float = 0.0
        realized_pnl:     float = 0.0
        contracts_bought: float = 0.0
        contracts_sold:   float = 0.0

        entry_dates:   list[str]  = []
        exit_dates:    list[str]  = []
        closed_events: list[dict] = []
        errors:        list[dict] = []

        has_short_open = any(t["option_action"] == "SELL_OPEN"  for t in txns_sorted)
        has_long_open  = any(t["option_action"] == "BUY_OPEN"   for t in txns_sorted)
        has_expired    = any(t["option_action"] == "EXPIRED"     for t in txns_sorted)

        for txn in txns_sorted:
            action    = txn["option_action"]
            contracts = float(txn["contracts"])
            premium   = float(txn["premium"])
            fees      = float(txn["fees"])
            tdate     = txn.get("trade_date")

            if action == "BUY_OPEN":
                lot_cost        = round(contracts * premium * mult + fees, 2)
                contracts_open  = round(contracts_open  + contracts, 8)
                cost_basis      = round(cost_basis      + lot_cost,  2)
                total_buy_cost  = round(total_buy_cost  + lot_cost,  2)
                contracts_bought = round(contracts_bought + contracts, 8)
                # Recompute avg_premium = remaining_cost_basis / (contracts_open × mult)
                avg_premium     = (
                    round(cost_basis / (contracts_open * mult), 6)
                    if contracts_open > CONTRACTS_EPSILON else 0.0
                )
                if tdate:
                    entry_dates.append(tdate)

            elif action == "SELL_CLOSE":
                # Oversell guard
                if contracts > contracts_open + CONTRACTS_EPSILON:
                    errors.append({
                        "type":                  "over_close",
                        "occ_key":               occ_key,
                        "contracts_attempted":   contracts,
                        "contracts_available":   contracts_open,
                    })
                    contracts = max(contracts_open, 0.0)

                proceeds        = round(contracts * premium * mult - fees, 2)
                cost_basis_sold = round(avg_premium * contracts * mult, 2)
                trade_pnl       = round(proceeds - cost_basis_sold, 2)
                pnl_pct         = (
                    round(trade_pnl / cost_basis_sold * 100, 4)
                    if cost_basis_sold else None
                )

                contracts_open  = round(contracts_open - contracts, 8)
                cost_basis      = round(cost_basis - cost_basis_sold, 2)
                realized_pnl    = round(realized_pnl + trade_pnl, 2)
                contracts_sold  = round(contracts_sold + contracts, 8)

                if tdate:
                    exit_dates.append(tdate)

                contracts_after = round(max(contracts_open, 0.0), 8)
                is_full_close   = contracts_after <= CONTRACTS_EPSILON

                closed_events.append({
                    "occ_key":                   occ_key,
                    "underlying":                underlying,
                    "display_symbol":            display_symbol,
                    "option_symbol":             display_symbol,
                    "expiration_date":           expiration_date,
                    "strike":                    strike,
                    "option_type":               option_type,
                    "contracts_closed":          contracts,
                    "entry_date":                min(entry_dates) if entry_dates else None,
                    "exit_date":                 tdate,
                    "avg_entry_premium":         avg_premium,
                    "exit_premium":              premium,
                    "cost_basis_sold":           cost_basis_sold,
                    "proceeds":                  proceeds,
                    "fees":                      fees,
                    "realized_pnl":              trade_pnl,
                    "realized_pnl_pct":          pnl_pct,
                    "contracts_remaining_after": contracts_after,
                    "is_full_close":             is_full_close,
                    "contract_multiplier":       mult,
                    "close_type":                "sell_to_close",
                })

            elif action == "SELL_OPEN":
                # Short option — track the position but no P&L accounting
                contracts_open = round(contracts_open - contracts, 8)
                if tdate:
                    entry_dates.append(tdate)

            elif action == "BUY_CLOSE":
                # Close short — basic tracking
                contracts_open = round(contracts_open + contracts, 8)
                if tdate:
                    exit_dates.append(tdate)

            elif action == "EXPIRED":
                # Option expired worthless
                if contracts_open > CONTRACTS_EPSILON:
                    # Close the remaining open position at 0
                    expired_basis   = round(cost_basis, 2)
                    trade_pnl       = -expired_basis
                    pnl_pct         = -100.0 if expired_basis > 0 else None
                    exp_contracts   = contracts_open

                    contracts_sold  = round(contracts_sold + exp_contracts, 8)
                    realized_pnl    = round(realized_pnl + trade_pnl, 2)

                    closed_events.append({
                        "occ_key":                   occ_key,
                        "underlying":                underlying,
                        "display_symbol":            display_symbol,
                        "option_symbol":             display_symbol,
                        "expiration_date":           expiration_date,
                        "strike":                    strike,
                        "option_type":               option_type,
                        "contracts_closed":          exp_contracts,
                        "entry_date":                min(entry_dates) if entry_dates else None,
                        "exit_date":                 tdate or expiration_date,
                        "avg_entry_premium":         avg_premium,
                        "exit_premium":              0.0,
                        "cost_basis_sold":           expired_basis,
                        "proceeds":                  0.0,
                        "fees":                      0.0,
                        "realized_pnl":              trade_pnl,
                        "realized_pnl_pct":          pnl_pct,
                        "contracts_remaining_after": 0.0,
                        "is_full_close":             True,
                        "contract_multiplier":       mult,
                        "close_type":                "expired",
                    })
                    if tdate:
                        exit_dates.append(tdate)

                contracts_open = 0.0
                cost_basis     = 0.0

        # ── Ensure cost_basis stays non-negative (rounding guard) ──────────
        cost_basis = max(cost_basis, 0.0)

        # ── Classify final status ─────────────────────────────────────────
        if has_short_open and not has_long_open:
            final_status = "short_option_tracked_basic"
        elif has_expired and not has_long_open and contracts_bought == 0:
            # Expired row with no prior BUY_OPEN in this CSV
            final_status = "orphan_expired"
        elif contracts_open <= CONTRACTS_EPSILON and closed_events:
            final_status = (
                "expired"
                if any(ev.get("close_type") == "expired" for ev in closed_events)
                else "fully_closed"
            )
        elif contracts_open > CONTRACTS_EPSILON and contracts_sold > 0:
            final_status = "partially_closed_open"
        elif contracts_open > CONTRACTS_EPSILON:
            final_status = "open"
        else:
            final_status = "no_activity"

        ledgers[occ_key] = {
            "occ_key":           occ_key,
            "underlying":        underlying,
            "expiration_date":   expiration_date,
            "strike":            strike,
            "option_type":       option_type,
            "display_symbol":    display_symbol,
            "contracts_open":    round(max(contracts_open, 0.0), 8),
            "avg_premium":       round(avg_premium, 6),
            "cost_basis":        round(cost_basis, 2),
            "contracts_bought":  round(contracts_bought, 8),
            "contracts_sold":    round(contracts_sold, 8),
            "total_buy_cost":    round(total_buy_cost, 2),
            "realized_pnl":      round(realized_pnl, 2),
            "first_entry_date":  min(entry_dates) if entry_dates else None,
            "last_entry_date":   max(entry_dates) if entry_dates else None,
            "last_exit_date":    max(exit_dates)  if exit_dates  else None,
            "closed_events":     closed_events,
            "accounting_errors": errors,
            "final_status":      final_status,
            "is_short":          has_short_open and not has_long_open,
        }

    return ledgers


# ── Step 4: Classify option positions ────────────────────────────────────────

def classify_option_positions(ledgers: dict[str, dict]) -> dict:
    """Derive open / partially-closed / fully-closed lists from option ledgers.

    Returns::

        {
          "open_positions":             list[dict],  # open + partially open
          "partially_closed_positions": list[dict],  # subset of open with some closes
          "fully_closed_positions":     list[dict],  # all contracts closed or expired
          "closed_trade_records":       list[dict],  # one per close event
        }
    """
    open_positions:   list[dict] = []
    partially_closed: list[dict] = []
    fully_closed:     list[dict] = []
    all_closed_events: list[dict] = []

    for occ_key, ledger in ledgers.items():
        status = ledger["final_status"]
        mult   = CONTRACT_MULTIPLIER

        base = {
            "occ_key":           occ_key,
            "underlying":        ledger["underlying"],
            "display_symbol":    ledger["display_symbol"],
            "option_symbol":     ledger["display_symbol"],
            "expiration_date":   ledger["expiration_date"],
            "strike":            ledger["strike"],
            "option_type":       ledger["option_type"],
            "contracts_bought":  ledger["contracts_bought"],
            "contracts_sold":    ledger["contracts_sold"],
            "contracts_open":    ledger["contracts_open"],
            "avg_premium":       ledger["avg_premium"],
            "cost_basis":        ledger["cost_basis"],
            "total_buy_cost":    ledger["total_buy_cost"],
            "realized_pnl":      ledger["realized_pnl"],
            "first_entry_date":  ledger["first_entry_date"],
            "last_entry_date":   ledger["last_entry_date"],
            "last_exit_date":    ledger["last_exit_date"],
            "final_status":      status,
            "classification":    status,
            "contract_multiplier": mult,
        }

        if status in ("open", "partially_closed_open", "short_option_tracked_basic"):
            _cb = ledger["contracts_bought"]
            rec = {
                **base,
                "percent_closed":    (
                    round(ledger["contracts_sold"]  / _cb * 100, 2) if _cb else None
                ),
                "percent_remaining": (
                    round(ledger["contracts_open"]  / _cb * 100, 2) if _cb else None
                ),
                "sell_events_count": len(ledger["closed_events"]),
            }
            open_positions.append(rec)
            if status == "partially_closed_open":
                partially_closed.append(rec)

        elif status in ("fully_closed", "expired", "orphan_expired"):
            _events   = ledger["closed_events"]
            _last     = _events[-1] if _events else {}
            _cb       = ledger["contracts_bought"]
            _tc       = ledger["total_buy_cost"]
            pnl_pct   = (
                round(ledger["realized_pnl"] / _tc * 100, 4)
                if _tc else None
            )
            holding_days: int | None = None
            if ledger["first_entry_date"] and ledger["last_exit_date"]:
                try:
                    holding_days = (
                        date.fromisoformat(ledger["last_exit_date"])
                        - date.fromisoformat(ledger["first_entry_date"])
                    ).days
                except Exception:
                    pass

            fully_closed.append({
                **base,
                "total_contracts_bought": _cb,
                "total_contracts_sold":   ledger["contracts_sold"],
                "avg_entry_premium":      (
                    round(_tc / (_cb * mult), 6) if _cb else 0.0
                ),
                "last_exit_premium":      _last.get("exit_premium"),
                "total_cost_basis":       _tc,
                "total_proceeds":         round(
                    sum(e.get("proceeds", 0) for e in _events), 2
                ),
                "realized_pnl_pct":       pnl_pct,
                "final_exit_date":        ledger["last_exit_date"],
                "holding_period_days":    holding_days,
                "close_type":             "expired" if status in ("expired", "orphan_expired") else "sold",
                "sell_events_count":      len(_events),
            })

        for ev in ledger["closed_events"]:
            all_closed_events.append({**ev, "final_option_status": status})

    return {
        "open_positions":             open_positions,
        "partially_closed_positions": partially_closed,
        "fully_closed_positions":     fully_closed,
        "closed_trade_records":       all_closed_events,
    }
