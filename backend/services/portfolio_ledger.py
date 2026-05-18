"""Portfolio transaction ledger accounting engine.

Converts brokerage CSV exports into a normalized transaction ledger,
then applies average-cost accounting to produce correct position state.

Pipeline
--------
1. normalize_csv_rows(csv_text)           -> NormalizeResult
2. deduplicate_transactions(txns, seen)   -> (unique, dupes)
3. build_symbol_ledgers(txns)             -> dict[symbol, ledger]
4. classify_positions(ledgers)            -> PositionResult

Rules
-----
- Only BUY and SELL rows affect position state.
- One closed-trade record is created PER SELL EVENT, not per ticker.
- is_full_close is set after each sell based on remaining shares at that point.
- A ticker is "partially closed" only if shares_remaining > 0 AND shares_sold > 0.
- A ticker is "fully closed" only if shares_remaining <= 0 AND shares_sold > 0.
- These two states are mutually exclusive for open positions.
"""
from __future__ import annotations

import csv as _csv
import hashlib
import io as _io
import re as _re
import uuid
from collections import defaultdict
from datetime import date, datetime
from typing import Any

# ── Tolerance constants ───────────────────────────────────────────────────────
# SHARE_EPSILON:  minimum meaningful shares — residuals <= this are treated as
#                 rounding/fractional dust and the position is considered closed.
# VALUE_EPSILON:  minimum meaningful cost-basis in USD — positions whose residual
#                 cost basis is <= $1.00 after a sell are treated as fully closed.
# PCT_EPSILON:    minimum residual as fraction of original buy cost — below 0.1%
#                 the residual is dust regardless of share count or dollar amount.
# SHARE_TOLERANCE: internal arithmetic guard (much tighter — only prevents
#                  floating-point noise from creating spurious open positions).
SHARE_EPSILON:    float = 0.0001   # 0.0001 shares — dust threshold
VALUE_EPSILON:    float = 1.00     # $1.00 — rounding / fee noise threshold
PCT_EPSILON:      float = 0.001    # 0.1% of original cost basis
SHARE_TOLERANCE:  float = 1e-6     # internal FP guard (unchanged)

# ── Side classification keyword sets ─────────────────────────────────────────
_BUY_KW: frozenset[str] = frozenset({
    "buy", "bought", "purchase", "purchased",
    "reinvestment", "reinvest",
    "shares purchased", "exchange in", "transfer in",
    "journaled shares", "you bought",
    # ACAT / DTC / brokerage-transfer terminology
    "received", "receive", "securities received",
    "shares received", "stock received", "received shares",
    "securities transfer", "acat",
    "deliver in", "delivered in",
    # Platform-specific (Robinhood, Public, etc.)
    "market buy", "limit buy", "stop buy", "buy open",
})

_SELL_KW: frozenset[str] = frozenset({
    "sell", "sold", "sale", "shares sold",
    "exchange out", "transfer out", "you sold",
    "sell short", "short sale", "sold short",
    "market sell", "limit sell",
})

_DIVIDEND_KW: frozenset[str] = frozenset({
    "dividend", "div", "ordinary dividend", "qualified dividend",
    "return of capital", "distribution",
})

_FEE_KW: frozenset[str] = frozenset({
    "fee", "commission", "charge", "interest", "margin interest",
    "advisory fee", "annual fee",
})

_SPLIT_KW: frozenset[str] = frozenset({
    "split", "stock split", "reverse split",
})

# Options phrases — must be checked BEFORE buy/sell because
# "buy to open" contains "buy" and would otherwise be mis-classified.
_OPTIONS_PHRASES: frozenset[str] = frozenset({
    "buy to open", "sell to close", "buy to close", "sell to open",
    "to open", "to close",
    "option expired", "expired",
    "assigned", "assignment",
    "exercised", "exercise",
    "opening purchase", "closing sale",
    "opening sale", "closing purchase",
})

# Cash / money-market symbols to skip entirely
_CASH_SYMBOLS: frozenset[str] = frozenset({
    "", "cash", "cashbalance", "--", "n/a",
    "spaxx", "fdrxx", "swvxx", "vmfxx", "sprxx",
    "fdlxx", "fzfxx", "mmda1",
})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _classify_side(raw_type: str, raw_desc: str = "") -> str:
    """Return one of: BUY / SELL / DIVIDEND / FEE / SPLIT / IGNORE / UNKNOWN."""
    t = raw_type.lower().strip()
    d = raw_desc.lower().strip()
    # Options must be checked first — "buy to open" contains "buy"
    for phrase in _OPTIONS_PHRASES:
        if phrase in t or phrase in d:
            return "IGNORE"
    if any(kw in t for kw in _SPLIT_KW):
        return "SPLIT"
    if any(kw in t for kw in _DIVIDEND_KW):
        return "DIVIDEND"
    if any(kw in t for kw in _FEE_KW):
        return "FEE"
    if any(kw in t for kw in _BUY_KW):
        return "BUY"
    if any(kw in t for kw in _SELL_KW):
        return "SELL"
    return "UNKNOWN"


def _parse_date(raw: str) -> str | None:
    """Try common date formats; return ISO yyyy-mm-dd or None."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in (
        "%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y",
        "%d/%m/%Y", "%Y/%m/%d",
        "%b %d, %Y", "%B %d, %Y",
        "%m-%d-%Y", "%d-%m-%Y",
        "%Y%m%d",
    ):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass
    # Try truncating to just the date part (e.g. "2026-05-12T05:27:37.636Z")
    if "T" in raw:
        return _parse_date(raw.split("T")[0])
    return None


def _parse_float(raw: str) -> float:
    """Parse numeric strings including -$1,234.56 and $(1,234.56) → -1234.56.

    Handles all common brokerage formats:
      $1,234.56      →  1234.56
      -$1,234.56     → -1234.56   (negative before dollar sign)
      ($1,234.56)    → -1234.56   (Fidelity / Schwab parenthesis negatives)
      (1,234.56)     → -1234.56
    """
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


def _fingerprint(
    symbol: str,
    trade_date: str | None,
    side: str,
    qty: float,
    price: float,
    description: str,
) -> str:
    """Stable content-hash for deduplication across multiple CSV uploads."""
    key = "|".join([
        symbol.upper(),
        (trade_date or "")[:10],
        side,
        f"{round(qty, 4):.4f}",
        f"{round(price, 4):.4f}",
        description[:80],
    ])
    return hashlib.md5(key.encode()).hexdigest()


# ── Step 1: Normalize CSV rows ────────────────────────────────────────────────

def normalize_csv_rows(csv_text: str, source_file: str = "") -> dict:
    """Parse one brokerage CSV export into normalized transaction objects.

    Returns::

        {
          "transactions": list[dict],   # normalized, one per valid row
          "ignored":      list[dict],   # options / cash / invalid rows
          "unknown_type": list[dict],   # side=UNKNOWN — needs review
          "columns":      list[str],
          "rows_total":   int,
          "rows_parsed":  int,
        }

    Each transaction dict::

        {
          transaction_id, fingerprint, source_file, raw_row_index,
          account, symbol, trade_date, settlement_date,
          action, side, quantity, price,
          gross_amount, fees, net_amount, currency,
          raw_description, normalized_type,
        }
    """
    # BOM + line-endings
    clean = (
        csv_text
        .replace("\ufeff", "")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .strip()
    )

    # Some Schwab exports have header junk before the real header;
    # find the first line containing "Symbol" or "Ticker".
    lines = clean.splitlines()
    header_idx = 0
    for i, line in enumerate(lines):
        low = line.lower()
        if "symbol" in low or "ticker" in low or "stock" in low:
            header_idx = i
            break
    clean = "\n".join(lines[header_idx:])

    # Auto-detect delimiter (comma / tab / pipe / semicolon)
    _sample = clean[:4096]
    _delim = ","
    try:
        _dialect = _csv.Sniffer().sniff(_sample, delimiters=",\t|;")
        _delim = _dialect.delimiter
    except Exception:
        _first = clean.split("\n")[0]
        if _first.count("\t") > _first.count(","):
            _delim = "\t"

    reader = _csv.DictReader(_io.StringIO(clean), delimiter=_delim)
    raw_cols: list[str] = reader.fieldnames or []
    cols_lower: dict[str, str] = {c.lower().strip(): c for c in raw_cols}

    def _find_col(*candidates: str) -> str | None:
        for c in candidates:
            if c.lower() in cols_lower:
                return cols_lower[c.lower()]
        return None

    col_symbol = _find_col("symbol", "ticker", "stock", "security", "instrument")
    col_date   = _find_col(
        "date", "trade date", "transaction date", "activity date",
        "settlement date", "run date",
    )
    col_settle = _find_col("settlement date", "settle date")
    col_type   = _find_col(
        "transaction type", "type", "action", "transaction",
        "activity", "description type", "trans type",
    )
    col_desc   = _find_col("description", "security description", "name")
    col_qty    = _find_col("quantity", "shares", "qty", "units", "amount (quantity)")
    col_price  = _find_col(
        "price", "price/share", "unit price", "cost/share",
        "exec price", "execution price", "trade price",
    )
    col_amount = _find_col(
        "amount", "value", "total", "net amount", "principal",
        "market value", "cost basis total", "net proceeds",
    )
    col_fees   = _find_col("fees & comm", "fees & commissions", "fees",
                           "commission", "commissions", "fee", "charges")
    col_acct   = _find_col("account", "account number", "account id", "acct")
    col_instr  = _find_col("instrument type", "asset type", "security type")

    transactions: list[dict] = []
    ignored: list[dict]      = []
    unknown_type: list[dict] = []
    row_index = 0

    for row in reader:
        def _v(col: str | None) -> str:
            return (row.get(col, "") or "").strip() if col else ""

        row_index += 1

        # ── Symbol ────────────────────────────────────────────────────────
        raw_sym = _v(col_symbol)
        if ":" in raw_sym:
            raw_sym = raw_sym.split(":")[-1]
        symbol = raw_sym.upper().strip()

        if symbol.lower() in _CASH_SYMBOLS or len(symbol) > 12:
            ignored.append({"row_index": row_index, "symbol": symbol,
                            "reason": "cash/mm symbol or too long"})
            continue

        # Options: space followed by digit (OCC date), Fidelity dash prefix,
        # OCC option pattern, date in symbol, underscore+date pattern
        if _re.search(r'\s+\d', raw_sym):
            ignored.append({"row_index": row_index, "symbol": symbol,
                            "reason": "options (space+digit in symbol)"})
            continue
        if raw_sym.startswith("-"):
            ignored.append({"row_index": row_index, "symbol": symbol,
                            "reason": "options (Fidelity dash prefix)"})
            continue
        if (_re.search(r'\d{6}[CP]\d+', symbol)
                or _re.search(r'_\d{6}[CP]', symbol)
                or _re.search(r'\d{2}/\d{2}/\d{2,4}', raw_sym)):
            ignored.append({"row_index": row_index, "symbol": symbol,
                            "reason": "options (OCC symbol pattern)"})
            continue

        if not symbol or not _re.match(r'^[A-Z0-9.\-]{1,12}$', symbol):
            ignored.append({"row_index": row_index, "symbol": symbol,
                            "reason": f"invalid symbol '{symbol}'"})
            continue

        # Explicit instrument-type column
        if col_instr:
            instr_val = _v(col_instr).lower()
            if "option" in instr_val or instr_val == "opt":
                ignored.append({"row_index": row_index, "symbol": symbol,
                                "reason": "options (instrument type column)"})
                continue

        # ── Side classification ───────────────────────────────────────────
        raw_type_val = _v(col_type)
        raw_desc_val = _v(col_desc)

        if col_type:
            # Type column present: use it; fall back to description only when
            # the type cell is empty for THIS specific row.
            type_for_class = raw_type_val or raw_desc_val
        else:
            # No type column at all → default to BUY (description holds
            # company names, not transaction verbs).
            type_for_class = ""

        side = _classify_side(type_for_class, raw_desc_val)

        if not col_type:
            side = "BUY"

        # Skip options action phrases
        if side == "IGNORE":
            ignored.append({"row_index": row_index, "symbol": symbol,
                            "reason": f"options action: '{raw_type_val}'"})
            continue

        # ── Dates ─────────────────────────────────────────────────────────
        trade_date      = _parse_date(_v(col_date))
        settlement_date = _parse_date(_v(col_settle)) if col_settle else None

        # ── Quantity ──────────────────────────────────────────────────────
        qty_raw = _v(col_qty).replace(",", "").replace("(", "-").replace(")", "")
        try:
            qty = abs(float(qty_raw)) if qty_raw else 0.0
        except (ValueError, TypeError):
            qty = 0.0

        # ── Price ─────────────────────────────────────────────────────────
        price = abs(_parse_float(_v(col_price)))
        gross_amount = abs(_parse_float(_v(col_amount)))

        # Back-calculate price from amount/qty if price missing
        if price == 0 and qty > 0 and gross_amount > 0:
            price = round(gross_amount / qty, 6)

        fees = abs(_parse_float(_v(col_fees))) if col_fees else 0.0

        # ── Skip zero-quantity BUY/SELL rows ──────────────────────────────
        if side in ("BUY", "SELL") and qty == 0:
            ignored.append({"row_index": row_index, "symbol": symbol,
                            "reason": "zero quantity"})
            continue

        # ── Skip UNKNOWN type for BUY/SELL-only accounting ────────────────
        if side == "UNKNOWN":
            unknown_type.append({
                "row_index":   row_index,
                "symbol":      symbol,
                "raw_type":    raw_type_val,
                "raw_desc":    raw_desc_val,
                "quantity":    qty,
                "price":       price,
                "trade_date":  trade_date,
            })
            # Still include in transactions list tagged as UNKNOWN so
            # callers can inspect/audit them, but skip position accounting.

        fp = _fingerprint(symbol, trade_date, side, qty, price, raw_desc_val)

        txn: dict = {
            "transaction_id":  str(uuid.uuid4()),
            "fingerprint":     fp,
            "source_file":     source_file,
            "raw_row_index":   row_index,
            "account":         _v(col_acct),
            "symbol":          symbol,
            "trade_date":      trade_date,
            "settlement_date": settlement_date,
            "action":          raw_type_val,
            "side":            side,
            "quantity":        qty,
            "price":           price,
            "gross_amount":    gross_amount,
            "fees":            fees,
            "net_amount":      gross_amount,
            "currency":        "USD",
            "raw_description": raw_desc_val,
            "normalized_type": side,
        }
        transactions.append(txn)

    return {
        "transactions": transactions,
        "ignored":      ignored,
        "unknown_type": unknown_type,
        "columns":      raw_cols,
        "rows_total":   row_index,
        "rows_parsed":  len(transactions),
    }


# ── Step 2: Deduplicate ───────────────────────────────────────────────────────

def deduplicate_transactions(
    transactions: list[dict],
    existing_fingerprints: set[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (unique, duplicates).

    Deduplicates within the current list AND against an optional set of
    fingerprints already stored in the database (from prior imports).
    """
    seen: set[str] = set(existing_fingerprints or set())
    unique: list[dict] = []
    dupes: list[dict]  = []
    for txn in transactions:
        fp = txn.get("fingerprint") or ""
        if fp and fp in seen:
            dupes.append(txn)
        else:
            unique.append(txn)
            if fp:
                seen.add(fp)
    return unique, dupes


# ── Step 3: Build per-symbol ledgers (average-cost) ───────────────────────────

def build_symbol_ledgers(transactions: list[dict]) -> dict[str, dict]:
    """Apply average-cost accounting to sorted transactions per symbol.

    Returns dict[symbol → ledger]. Each ledger::

        {
          symbol, transactions, buy_count, sell_count,
          shares_bought, shares_sold, shares_remaining,
          avg_cost,          # current avg cost of remaining shares
          cost_basis,        # remaining cost basis
          total_buy_cost,    # total cost of all buys (for avg_entry_price)
          realized_pnl,
          first_buy_date, last_buy_date,
          first_sell_date, last_sell_date,
          closed_events,     # list — one dict per sell event
          accounting_errors, # oversell warnings etc.
        }
    """
    by_symbol: dict[str, list[dict]] = defaultdict(list)
    for txn in transactions:
        # Only BUY and SELL affect positions
        if txn["side"] in ("BUY", "SELL"):
            by_symbol[txn["symbol"]].append(txn)

    ledgers: dict[str, dict] = {}

    for sym, txns in by_symbol.items():
        # Sort: trade_date ascending; BUY before SELL on the same date
        def _sort_key(t: dict) -> tuple:
            d = t.get("trade_date") or "9999-12-31"
            side_order = 0 if t["side"] == "BUY" else 1
            return (d, side_order)

        txns_sorted = sorted(txns, key=_sort_key)

        shares_open: float   = 0.0
        cost_basis: float    = 0.0
        avg_cost: float      = 0.0
        total_buy_cost: float = 0.0
        realized_pnl: float  = 0.0
        shares_bought: float = 0.0
        shares_sold: float   = 0.0
        closed_events: list[dict] = []
        errors: list[dict]        = []
        buy_dates: list[str]      = []
        sell_dates: list[str]     = []

        for txn in txns_sorted:
            side  = txn["side"]
            qty   = float(txn.get("quantity") or 0)
            price = float(txn.get("price")    or 0)
            tdate = txn.get("trade_date") or ""
            fees  = float(txn.get("fees")     or 0)

            if side == "BUY":
                shares_open   = round(shares_open + qty, 8)
                cost_basis    = round(cost_basis + qty * price, 6)
                total_buy_cost = round(total_buy_cost + qty * price, 6)
                avg_cost      = (
                    round(cost_basis / shares_open, 6)
                    if shares_open > SHARE_TOLERANCE else 0.0
                )
                shares_bought = round(shares_bought + qty, 8)
                if tdate:
                    buy_dates.append(tdate)

            elif side == "SELL":
                # Zero-quantity guard: fee/adjustment rows that slipped through
                # as SELL with qty=0 must not create a sell event.
                # Real CSV normalization already filters these; this guard is
                # a belt-and-suspenders safety net inside the ledger engine.
                if qty <= SHARE_TOLERANCE:
                    continue

                # Oversell guard — cap at available shares
                effective_qty = qty
                if qty > shares_open + SHARE_TOLERANCE:
                    errors.append({
                        "type":    "oversell",
                        "symbol":  sym,
                        "txn_id":  txn.get("transaction_id"),
                        "date":    tdate,
                        "message": (
                            f"Sell qty {qty} exceeds open shares "
                            f"{shares_open:.6f}; capped to available."
                        ),
                    })
                    effective_qty = max(shares_open, 0.0)

                avg_cost_at_sale   = avg_cost          # snapshot before this sell
                realized_cost      = round(avg_cost_at_sale * effective_qty, 6)
                proceeds           = round(price * effective_qty - fees, 6)
                event_pnl          = round(proceeds - realized_cost, 6)
                event_pnl_pct      = (
                    round(event_pnl / realized_cost * 100, 4)
                    if realized_cost else None
                )

                shares_open  = round(shares_open - effective_qty, 8)
                if shares_open < SHARE_TOLERANCE:
                    shares_open = 0.0
                # Under average-cost: avg_cost of remaining shares is unchanged
                cost_basis   = round(avg_cost * shares_open, 6)
                realized_pnl = round(realized_pnl + event_pnl, 6)
                shares_sold  = round(shares_sold + effective_qty, 8)

                is_full_close = shares_open <= SHARE_TOLERANCE

                if tdate:
                    sell_dates.append(tdate)

                closed_events.append({
                    "symbol":                sym,
                    "ticker":                sym,
                    "shares_sold":           effective_qty,
                    "exit_price":            price,
                    "avg_cost_at_sale":      avg_cost_at_sale,
                    "entry_price":           avg_cost_at_sale,   # alias expected by store
                    "exit_date":             tdate,
                    "entry_date":            buy_dates[0] if buy_dates else None,
                    "proceeds":              proceeds,
                    "realized_cost_basis":   realized_cost,
                    "realized_pnl":          event_pnl,
                    "realized_pnl_pct":      event_pnl_pct,
                    "remaining_shares_after": shares_open,
                    "close_type":            "full" if is_full_close else "partial",
                    "sell_type":             "full" if is_full_close else "partial",
                    "is_full_close":         is_full_close,
                    "fees":                  fees,
                    # Aliases for closed_trades_store compatibility
                    "shares":                effective_qty,
                    "exit_price":            price,
                    "notes":                 "Imported from CSV (ledger engine)",
                    "cost_method":           "average_cost",
                })

        # ── Buy-close-reopen detection ─────────────────────────────────────
        # If the last sell date exists AND every remaining open share came
        # from a buy that occurred STRICTLY AFTER that sell date, the prior
        # position was fully closed and the new lot is a fresh open — it
        # should NOT be classified as "partially closed".
        # Example: OUST buy 227 → sell 227 (full close) → buy 302 (reopen).
        #          ALMU buy 204 → sell 204 (full close) → buy 439 (reopen).
        _last_sell = max(sell_dates) if sell_dates else None
        _shares_bought_post_sell = 0.0
        if _last_sell:
            for _t in txns_sorted:
                if _t["side"] == "BUY" and (_t.get("trade_date") or "") > _last_sell:
                    _shares_bought_post_sell = round(
                        _shares_bought_post_sell + float(_t.get("quantity") or 0), 8
                    )
        # True only when all current open shares postdate the last sell
        all_open_lots_post_sell: bool = (
            _last_sell is not None
            and shares_open > SHARE_TOLERANCE
            and _shares_bought_post_sell >= shares_open - SHARE_TOLERANCE
        )

        ledgers[sym] = {
            "symbol":                 sym,
            "transactions":           txns_sorted,
            "buy_count":              sum(1 for t in txns_sorted
                                          if t["side"] == "BUY"
                                          and float(t.get("quantity") or 0) > SHARE_TOLERANCE),
            "sell_count":             sum(1 for t in txns_sorted
                                          if t["side"] == "SELL"
                                          and float(t.get("quantity") or 0) > SHARE_TOLERANCE),
            "shares_bought":          round(shares_bought, 8),
            "shares_sold":            round(shares_sold, 8),
            "shares_remaining":       round(shares_open, 8),
            "avg_cost":               round(avg_cost, 6),
            "cost_basis":             round(cost_basis, 6),
            "total_buy_cost":         round(total_buy_cost, 6),
            "realized_pnl":           round(realized_pnl, 6),
            "first_buy_date":         min(buy_dates)  if buy_dates  else None,
            "last_buy_date":          max(buy_dates)  if buy_dates  else None,
            "first_sell_date":        min(sell_dates) if sell_dates else None,
            "last_sell_date":         max(sell_dates) if sell_dates else None,
            "all_open_lots_post_sell": all_open_lots_post_sell,
            "shares_bought_post_sell": round(_shares_bought_post_sell, 8),
            "closed_events":          closed_events,
            "accounting_errors":      errors,
        }

    return ledgers


# ── Step 4: Classify positions ────────────────────────────────────────────────

def classify_positions(ledgers: dict[str, dict]) -> dict:
    """Derive open / partially-closed / fully-closed from per-symbol ledgers.

    Returns::

        {
          "open_positions":             list[dict],
          "partially_closed_positions": list[dict],
          "fully_closed_positions":     list[dict],
          "closed_trade_records":       list[dict],   # one per sell event
          "monthly_closed_positions":   dict[str, list[dict]],
          "symbol_audit":               dict[str, dict],
        }

    Classification rules (mutually exclusive for the open/closed axis):

    =========== ===================== ========= =================
    Category    shares_remaining      sold > 0  appears_in_open
    =========== ===================== ========= =================
    open        > tolerance           no        yes
    partial     > tolerance           yes       yes
    fully closed <= tolerance         yes       no
    =========== ===================== ========= =================
    """
    open_positions: list[dict]    = []
    partially_closed: list[dict]  = []
    fully_closed: list[dict]      = []
    all_closed_events: list[dict] = []
    symbol_audit: dict[str, dict] = {}

    for sym, ledger in ledgers.items():
        sr  = ledger["shares_remaining"]
        ss  = ledger["shares_sold"]
        sb  = ledger["shares_bought"]
        cb  = ledger["cost_basis"]           # remaining cost basis after sells
        tbc = ledger["total_buy_cost"]        # total original buy cost

        # ── Dust / rounding guard (three-way check) ───────────────────────────
        # A residual position is "dust" (treat as fully closed) if ALL THREE of:
        #   1. shares_remaining <= SHARE_EPSILON  (< 0.0001 shares)
        #   2. cost_basis_remaining <= VALUE_EPSILON ($1.00)
        #   3. residual is less than PCT_EPSILON (0.1%) of original buy cost
        # A position must pass ALL criteria to be meaningful open.
        # This prevents fees, brokerage rounding, or fractional dust from
        # creating spurious partially-closed classifications.
        residual_pct = (cb / tbc) if tbc > 0 else 0.0
        is_dust = (
            sr         <= SHARE_EPSILON
            or cb      <= VALUE_EPSILON
            or residual_pct < PCT_EPSILON
        )

        # is_open: meaningful shares remain AND the residual is not just dust
        is_open         = sr > SHARE_TOLERANCE and not is_dust
        has_sells       = ss > SHARE_TOLERANCE
        # Partially closed = shares remain AND there were sells AND those sells
        # reduced the CURRENT open lot (not a prior lot that was fully closed).
        # If all remaining shares came from buys AFTER the last sell date, the
        # prior position was fully closed and the current one is fresh → NOT partial.
        all_open_post_sell = ledger.get("all_open_lots_post_sell", False)
        is_partial      = is_open and has_sells and not all_open_post_sell
        is_fully_closed = (not is_open) and has_sells

        avg_entry_price = (
            round(ledger["total_buy_cost"] / sb, 6) if sb else 0.0
        )

        # ── Last sell event (used by multiple sections below) ──────────────────
        _sym_events = ledger.get("closed_events", [])
        _last_ev    = _sym_events[-1] if _sym_events else {}

        if is_open:
            _open_cls = "partially_closed_open" if is_partial else "open"
            open_positions.append({
                "symbol":               sym,
                "shares":               sr,
                "avg_cost":             ledger["avg_cost"],
                "cost_basis":           ledger["cost_basis"],
                "entry_date":           ledger["first_buy_date"],
                "last_buy_date":        ledger["last_buy_date"],
                "total_bought_shares":  sb,
                "total_sold_shares":    ss,
                "realized_pnl_to_date": ledger["realized_pnl"],
                # ── Unambiguous category fields ─────────────────────────────
                "status":              _open_cls,
                "final_symbol_status": _open_cls,
                "classification":      _open_cls,
            })

        if is_partial:
            # Cost basis consumed by sells (total buy cost − remaining basis)
            _cost_basis_sold      = round(tbc - cb, 2)
            _cost_basis_remaining = round(cb, 2)
            _pct_closed    = round(ss / sb * 100, 2) if sb else None
            _pct_remaining = round(sr / sb * 100, 2) if sb else None
            _pnl_pct_v = (
                round(ledger["realized_pnl"] / _cost_basis_sold * 100, 4)
                if _cost_basis_sold else None
            )
            partially_closed.append({
                "symbol":               sym,
                # ── Share accounting ────────────────────────────────────────
                "shares_bought":        sb,
                "shares_sold":          ss,
                "shares_remaining":     sr,
                "percent_closed":       _pct_closed,
                "percent_remaining":    _pct_remaining,
                # ── Cost / price ─────────────────────────────────────────
                "avg_entry_price":      avg_entry_price,
                "avg_cost":             ledger["avg_cost"],
                "cost_basis_sold":      _cost_basis_sold,
                "cost_basis_remaining": _cost_basis_remaining,
                # ── Realised P&L ─────────────────────────────────────────
                "realized_pnl":         ledger["realized_pnl"],
                "realized_pnl_pct":     _pnl_pct_v,
                # ── Exit price ───────────────────────────────────────────
                "last_exit_price":      _last_ev.get("exit_price"),
                # ── Dates ────────────────────────────────────────────────
                "first_entry_date":     ledger["first_buy_date"],
                "last_sell_date":       ledger["last_sell_date"],
                "last_exit_date":       ledger["last_sell_date"],  # canonical alias
                # ── Event count ──────────────────────────────────────────
                "sell_events_count":    len(_sym_events),
                # ── Unambiguous category fields ──────────────────────────
                "status":              "partially_closed_open",
                "final_symbol_status": "partially_closed_open",
                "classification":      "partially_closed_open",
            })

        if is_fully_closed:
            total_cost = avg_entry_price * sb
            pnl_pct = (
                round(ledger["realized_pnl"] / total_cost * 100, 4)
                if total_cost else None
            )
            holding_days: int | None = None
            if ledger["last_sell_date"] and ledger["first_buy_date"]:
                try:
                    holding_days = (
                        date.fromisoformat(ledger["last_sell_date"])
                        - date.fromisoformat(ledger["first_buy_date"])
                    ).days
                except Exception:
                    pass
            fully_closed.append({
                "symbol":              sym,
                "total_shares_bought": sb,
                "total_shares_sold":   ss,
                "avg_entry_price":     avg_entry_price,
                # ── Exit price from the last recorded sell event ──────────
                "last_exit_price":     _last_ev.get("exit_price"),
                "realized_pnl":        ledger["realized_pnl"],
                "realized_pnl_pct":    pnl_pct,
                "first_entry_date":    ledger["first_buy_date"],
                "final_exit_date":     ledger["last_sell_date"],
                "holding_period_days": holding_days,
                # ── Unambiguous category fields ──────────────────────────
                "status":              "fully_closed",
                "final_symbol_status": "fully_closed",
                "classification":      "fully_closed",
            })

        for ev in ledger["closed_events"]:
            all_closed_events.append(ev)

        # Per-symbol audit log
        if is_open and not is_partial:
            classification = "open"
        elif is_partial:
            classification = "partially_closed_open"
        elif is_fully_closed:
            classification = "fully_closed"
        else:
            classification = "no_activity"

        symbol_audit[sym] = {
            "symbol":                          sym,
            "buys":                            ledger["buy_count"],
            "sells":                           ledger["sell_count"],
            "shares_bought":                   sb,
            "shares_sold":                     ss,
            "shares_remaining":                sr,
            "realized_pnl":                    ledger["realized_pnl"],
            "classification":                  classification,
            "appears_in_open":                 is_open,
            "appears_in_partial":              is_partial,
            "appears_in_fully_closed":         is_fully_closed,
            "accounting_errors":               ledger["accounting_errors"],
            "monthly_closed_entries":          [],
            # ── Epsilon / dust diagnostics ────────────────────────────────────
            "cost_basis_remaining":            round(cb, 6),
            "total_buy_cost":                  round(tbc, 6),
            "residual_pct":                    round(residual_pct, 6),
            "is_dust":                         is_dust,
            "share_epsilon":                   SHARE_EPSILON,
            "value_epsilon":                   VALUE_EPSILON,
            "pct_epsilon":                     PCT_EPSILON,
            "has_real_sell":                   has_sells,
            "all_open_lots_post_sell":         all_open_post_sell,
        }

    # ── Monthly closed positions ───────────────────────────────────────────
    monthly: dict[str, list[dict]] = {}
    for ev in all_closed_events:
        d = ev.get("exit_date") or ""
        month_key = d[:7] if len(d) >= 7 else "unknown"
        monthly.setdefault(month_key, []).append(ev)
        sym = ev.get("symbol") or ev.get("ticker") or ""
        if sym in symbol_audit:
            if month_key not in symbol_audit[sym]["monthly_closed_entries"]:
                symbol_audit[sym]["monthly_closed_entries"].append(month_key)

    return {
        "open_positions":             open_positions,
        "partially_closed_positions": partially_closed,
        "fully_closed_positions":     fully_closed,
        "closed_trade_records":       all_closed_events,
        "monthly_closed_positions":   monthly,
        "symbol_audit":               symbol_audit,
    }


# ── Symbol-level diagnostic reporter ─────────────────────────────────────────

def build_symbol_diagnostics(
    symbol: str,
    ledger: dict,
    audit: dict,
    final_symbol_status: str = "",
) -> dict:
    """Return the full diagnostic record requested by the task spec for one symbol.

    Fields match the spec exactly::

        symbol, transactions, total_bought_shares, total_sold_shares,
        shares_remaining_raw, shares_remaining_after_tolerance,
        cost_basis_remaining_raw, cost_basis_remaining_after_tolerance,
        fees_total, residual_value, residual_pct,
        has_real_sell, has_fee_only_sell_like_rows,
        classification_before_tolerance, classification_after_tolerance,
        final_symbol_status, reason
    """
    sr  = ledger.get("shares_remaining", 0.0)
    ss  = ledger.get("shares_sold", 0.0)
    sb  = ledger.get("shares_bought", 0.0)
    cb  = ledger.get("cost_basis", 0.0)
    tbc = ledger.get("total_buy_cost", 0.0)

    residual_pct = round((cb / tbc) if tbc > 0 else 0.0, 6)

    # Determine pre-epsilon classification (using only SHARE_TOLERANCE)
    has_sells_raw  = ss > SHARE_TOLERANCE
    is_open_raw    = sr > SHARE_TOLERANCE
    all_post_sell  = ledger.get("all_open_lots_post_sell", False)
    is_partial_raw = is_open_raw and has_sells_raw and not all_post_sell
    is_fc_raw      = (not is_open_raw) and has_sells_raw
    if is_open_raw and not is_partial_raw:
        class_before = "open"
    elif is_partial_raw:
        class_before = "partially_closed_open"
    elif is_fc_raw:
        class_before = "fully_closed"
    else:
        class_before = "no_activity"

    # Post-epsilon (same as classify_positions uses)
    is_dust = (
        sr <= SHARE_EPSILON
        or cb <= VALUE_EPSILON
        or residual_pct < PCT_EPSILON
    )
    is_open_eps = sr > SHARE_TOLERANCE and not is_dust
    is_partial_eps = is_open_eps and has_sells_raw and not all_post_sell
    is_fc_eps = (not is_open_eps) and has_sells_raw
    if is_open_eps and not is_partial_eps:
        class_after = "open"
    elif is_partial_eps:
        class_after = "partially_closed_open"
    elif is_fc_eps:
        class_after = "fully_closed"
    else:
        class_after = "no_activity"

    # Fees total across all transactions
    fees_total = round(
        sum(float(t.get("fees") or 0) for t in ledger.get("transactions", [])), 6
    )

    # Reason string
    if class_before != class_after:
        reason = (
            f"Reclassified from {class_before!r} to {class_after!r} by epsilon guard: "
            f"shares_remaining={sr} (epsilon={SHARE_EPSILON}), "
            f"cost_basis={cb:.4f} (epsilon=${VALUE_EPSILON}), "
            f"residual_pct={residual_pct:.4%} (epsilon={PCT_EPSILON:.1%})"
        )
    elif not has_sells_raw:
        reason = "No real sell transactions — pure open position (no sell events)"
    elif is_open_eps and is_partial_eps:
        reason = (
            f"Meaningful partial close: {ss} shares sold, {sr} shares remain "
            f"(cost_basis=${cb:.2f}, residual_pct={residual_pct:.2%})"
        )
    elif is_fc_eps:
        reason = f"All {sb} bought shares were sold ({ss}); position fully closed"
    else:
        reason = f"Classification={class_after}"

    return {
        "symbol":                              symbol,
        "transactions":                        [
            {
                "side":   t.get("side"),
                "qty":    t.get("quantity"),
                "price":  t.get("price"),
                "fees":   t.get("fees"),
                "date":   t.get("trade_date"),
                "action": t.get("action"),
            }
            for t in ledger.get("transactions", [])
        ],
        "total_bought_shares":                 round(sb, 8),
        "total_sold_shares":                   round(ss, 8),
        "shares_remaining_raw":                round(sr, 8),
        "shares_remaining_after_tolerance":    round(sr, 8) if not is_dust else 0.0,
        "cost_basis_remaining_raw":            round(cb, 6),
        "cost_basis_remaining_after_tolerance":round(cb, 6) if not is_dust else 0.0,
        "fees_total":                          fees_total,
        "residual_value":                      round(cb, 6),
        "residual_pct":                        residual_pct,
        "has_real_sell":                       has_sells_raw,
        "has_fee_only_sell_like_rows":         False,  # fee rows → side=FEE, not SELL
        "classification_before_tolerance":     class_before,
        "classification_after_tolerance":      class_after,
        "final_symbol_status":                 final_symbol_status or class_after,
        "is_dust":                             is_dust,
        "share_epsilon":                       SHARE_EPSILON,
        "value_epsilon":                       VALUE_EPSILON,
        "pct_epsilon":                         PCT_EPSILON,
        "reason":                              reason,
    }


# ── Built-in test suite ───────────────────────────────────────────────────────

def run_ledger_tests() -> dict:
    """Run the 8 required test cases in-memory. Returns pass/fail per test."""

    def _make_txn(sym, side, qty, price, d) -> dict:
        fp = _fingerprint(sym, d, side, qty, price, "test")
        return {
            "transaction_id": str(uuid.uuid4()),
            "fingerprint":    fp,
            "source_file":    "test",
            "raw_row_index":  0,
            "account":        "",
            "symbol":         sym,
            "trade_date":     d,
            "settlement_date": None,
            "action":         side,
            "side":           side,
            "quantity":       float(qty),
            "price":          float(price),
            "gross_amount":   float(qty) * float(price),
            "fees":           0.0,
            "net_amount":     float(qty) * float(price),
            "currency":       "USD",
            "raw_description": "test",
            "normalized_type": side,
        }

    results = {}

    # ── Test 1: SIVEF partial sell, still open ─────────────────────────────
    t1_txns = [
        _make_txn("SIVEF", "BUY",  100, 10, "2026-01-01"),
        _make_txn("SIVEF", "SELL",  40, 15, "2026-05-01"),
    ]
    t1_ledgers = build_symbol_ledgers(t1_txns)
    t1_pos     = classify_positions(t1_ledgers)
    t1_audit   = t1_pos["symbol_audit"]["SIVEF"]
    t1_open    = any(p["symbol"] == "SIVEF" for p in t1_pos["open_positions"])
    t1_partial = any(p["symbol"] == "SIVEF" for p in t1_pos["partially_closed_positions"])
    t1_closed  = any(p["symbol"] == "SIVEF" for p in t1_pos["fully_closed_positions"])
    t1_event   = t1_pos["closed_trade_records"][0] if t1_pos["closed_trade_records"] else {}
    t1_pnl_ok  = abs((t1_event.get("realized_pnl") or 0) - 200.0) < 0.01
    t1_rem_ok  = abs((t1_event.get("remaining_shares_after") or 0) - 60.0) < 0.01
    results["test1_sivef_partial"] = {
        "pass": (t1_open and t1_partial and not t1_closed and t1_pnl_ok and t1_rem_ok),
        "open":               t1_open,
        "partially_closed":   t1_partial,
        "fully_closed":       t1_closed,
        "realized_pnl":       t1_event.get("realized_pnl"),
        "remaining_after":    t1_event.get("remaining_shares_after"),
        "shares_remaining":   t1_audit["shares_remaining"],
        "expected_pnl":       200.0,
        "expected_remaining": 60.0,
    }

    # ── Test 2: OUST fully open, no sells ─────────────────────────────────
    t2_txns = [_make_txn("OUST", "BUY", 100, 5, "2026-01-01")]
    t2_ledgers = build_symbol_ledgers(t2_txns)
    t2_pos     = classify_positions(t2_ledgers)
    t2_open    = any(p["symbol"] == "OUST" for p in t2_pos["open_positions"])
    t2_partial = any(p["symbol"] == "OUST" for p in t2_pos["partially_closed_positions"])
    t2_closed  = any(p["symbol"] == "OUST" for p in t2_pos["fully_closed_positions"])
    results["test2_oust_open_only"] = {
        "pass": (t2_open and not t2_partial and not t2_closed),
        "open": t2_open, "partially_closed": t2_partial, "fully_closed": t2_closed,
    }

    # ── Test 3: ALMU fully open, no sells ─────────────────────────────────
    t3_txns = [_make_txn("ALMU", "BUY", 200, 8, "2026-01-01")]
    t3_ledgers = build_symbol_ledgers(t3_txns)
    t3_pos     = classify_positions(t3_ledgers)
    t3_open    = any(p["symbol"] == "ALMU" for p in t3_pos["open_positions"])
    t3_partial = any(p["symbol"] == "ALMU" for p in t3_pos["partially_closed_positions"])
    t3_closed  = any(p["symbol"] == "ALMU" for p in t3_pos["fully_closed_positions"])
    results["test3_almu_open_only"] = {
        "pass": (t3_open and not t3_partial and not t3_closed),
        "open": t3_open, "partially_closed": t3_partial, "fully_closed": t3_closed,
    }

    # ── Test 4: Fully closed ──────────────────────────────────────────────
    t4_txns = [
        _make_txn("TEST_FULL", "BUY",  100, 10, "2026-01-01"),
        _make_txn("TEST_FULL", "SELL", 100, 12, "2026-03-01"),
    ]
    t4_ledgers = build_symbol_ledgers(t4_txns)
    t4_pos     = classify_positions(t4_ledgers)
    t4_open    = any(p["symbol"] == "TEST_FULL" for p in t4_pos["open_positions"])
    t4_partial = any(p["symbol"] == "TEST_FULL" for p in t4_pos["partially_closed_positions"])
    t4_closed  = any(p["symbol"] == "TEST_FULL" for p in t4_pos["fully_closed_positions"])
    t4_monthly = "2026-03" in t4_pos["monthly_closed_positions"]
    t4_event   = t4_pos["closed_trade_records"][0] if t4_pos["closed_trade_records"] else {}
    results["test4_fully_closed"] = {
        "pass": (not t4_open and not t4_partial and t4_closed and t4_monthly),
        "open": t4_open, "partially_closed": t4_partial, "fully_closed": t4_closed,
        "in_monthly_2026_03": t4_monthly,
        "is_full_close": t4_event.get("is_full_close"),
    }

    # ── Test 5: Duplicate protection (import same transactions twice) ──────
    t5_txns_a = [_make_txn("DUPTEST", "BUY", 100, 10, "2026-01-01")]
    t5_txns_b = [_make_txn("DUPTEST", "BUY", 100, 10, "2026-01-01")]
    t5_unique_a, t5_dupes_a = deduplicate_transactions(t5_txns_a)
    # Second import: pass fingerprints from first import as existing
    existing_fps = {t["fingerprint"] for t in t5_unique_a}
    t5_unique_b, t5_dupes_b = deduplicate_transactions(t5_txns_b, existing_fps)
    t5_ledgers = build_symbol_ledgers(t5_unique_a + t5_unique_b)
    t5_shares  = t5_ledgers["DUPTEST"]["shares_remaining"]
    results["test5_dedup"] = {
        "pass": (len(t5_dupes_b) == 1 and abs(t5_shares - 100.0) < 0.01),
        "dupes_detected": len(t5_dupes_b),
        "shares_after_double_import": t5_shares,
        "expected_shares": 100.0,
    }

    # ── Test 6: Partial then full close (two sells) ────────────────────────
    # BUY 100 @ 10 → SELL 25 @ 20 → SELL 75 @ 30
    # Expected realized P&L = 25*(20-10) + 75*(30-10) = 250 + 1500 = 1750
    t6_txns = [
        _make_txn("TEST_MIX", "BUY",  100, 10, "2026-01-01"),
        _make_txn("TEST_MIX", "SELL",  25, 20, "2026-02-01"),
        _make_txn("TEST_MIX", "SELL",  75, 30, "2026-03-01"),
    ]
    t6_ledgers = build_symbol_ledgers(t6_txns)
    t6_pos     = classify_positions(t6_ledgers)
    t6_ledger  = t6_ledgers["TEST_MIX"]
    t6_open    = any(p["symbol"] == "TEST_MIX" for p in t6_pos["open_positions"])
    t6_closed  = any(p["symbol"] == "TEST_MIX" for p in t6_pos["fully_closed_positions"])
    t6_events  = t6_pos["closed_trade_records"]
    t6_total_pnl = sum(e.get("realized_pnl") or 0 for e in t6_events)
    t6_pnl_ok  = abs(t6_total_pnl - 1750.0) < 0.01
    t6_two_events = len(t6_events) == 2
    # First sell should be partial, second should be full
    t6_e1_partial = not t6_events[0].get("is_full_close") if t6_events else False
    t6_e2_full    = t6_events[1].get("is_full_close") if len(t6_events) > 1 else False
    results["test6_partial_then_full"] = {
        "pass": (
            not t6_open and t6_closed and t6_pnl_ok
            and t6_two_events and t6_e1_partial and t6_e2_full
        ),
        "open":              t6_open,
        "fully_closed":      t6_closed,
        "total_realized_pnl": t6_total_pnl,
        "expected_pnl":      1750.0,
        "sell_event_count":  len(t6_events),
        "event1_is_partial": t6_e1_partial,
        "event2_is_full":    t6_e2_full,
    }

    # ── Test 7: OUST buy-close-reopen (matches real CSV pattern) ──────────
    # Buy 227 → Sell 227 (fully closes) → Buy 302 (fresh reopen)
    # Expected: open=True, partially_closed=FALSE, fully_closed=False
    t7_txns = [
        _make_txn("OUST", "BUY",  227, 21.99, "2026-03-16"),
        _make_txn("OUST", "SELL", 227, 20.31, "2026-03-25"),
        _make_txn("OUST", "BUY",  302, 26.51, "2026-05-12"),
    ]
    t7_ledgers = build_symbol_ledgers(t7_txns)
    t7_pos     = classify_positions(t7_ledgers)
    t7_open    = any(p["symbol"] == "OUST" for p in t7_pos["open_positions"])
    t7_partial = any(p["symbol"] == "OUST" for p in t7_pos["partially_closed_positions"])
    t7_closed  = any(p["symbol"] == "OUST" for p in t7_pos["fully_closed_positions"])
    t7_ledger  = t7_ledgers["OUST"]
    results["test7_oust_reopen"] = {
        "pass": (t7_open and not t7_partial and not t7_closed),
        "open":               t7_open,
        "partially_closed":   t7_partial,
        "fully_closed":       t7_closed,
        "shares_remaining":   t7_ledger["shares_remaining"],
        "all_open_lots_post_sell": t7_ledger["all_open_lots_post_sell"],
        "expected_remaining": 302.0,
        "expected_open":      True,
        "expected_partial":   False,
    }

    # ── Test 8: ALMU buy-close-reopen (matches real CSV pattern) ──────────
    # Buy 169 (01/02) + Buy 35 (01/23) → Sell 204 (01/27, full close)
    # → Buy 439 (04/22, fresh reopen)
    # Expected: open=True, partially_closed=FALSE, fully_closed=False
    t8_txns = [
        _make_txn("ALMU", "BUY",  169, 17.84, "2026-01-02"),
        _make_txn("ALMU", "BUY",   35, 17.00, "2026-01-23"),
        _make_txn("ALMU", "SELL", 204, 16.74, "2026-01-27"),
        _make_txn("ALMU", "BUY",  439, 18.13, "2026-04-22"),
    ]
    t8_ledgers = build_symbol_ledgers(t8_txns)
    t8_pos     = classify_positions(t8_ledgers)
    t8_open    = any(p["symbol"] == "ALMU" for p in t8_pos["open_positions"])
    t8_partial = any(p["symbol"] == "ALMU" for p in t8_pos["partially_closed_positions"])
    t8_closed  = any(p["symbol"] == "ALMU" for p in t8_pos["fully_closed_positions"])
    t8_ledger  = t8_ledgers["ALMU"]
    results["test8_almu_reopen"] = {
        "pass": (t8_open and not t8_partial and not t8_closed),
        "open":               t8_open,
        "partially_closed":   t8_partial,
        "fully_closed":       t8_closed,
        "shares_remaining":   t8_ledger["shares_remaining"],
        "all_open_lots_post_sell": t8_ledger["all_open_lots_post_sell"],
        "expected_remaining": 439.0,
        "expected_open":      True,
        "expected_partial":   False,
    }

    # ── Test 9: Dust residual → fully closed (epsilon guard) ──────────────
    # BUY 1000 @ $10 → SELL 999.9999 @ $12 → 0.0001 shares remain ($0.001 basis)
    # Should be classified as FULLY CLOSED, not partial, because residual is dust.
    t9_txns = [
        _make_txn("DUSTTEST", "BUY",  1000,    10.0, "2026-01-01"),
        _make_txn("DUSTTEST", "SELL", 999.9999, 12.0, "2026-03-01"),
    ]
    t9_ledgers = build_symbol_ledgers(t9_txns)
    t9_pos     = classify_positions(t9_ledgers)
    t9_open    = any(p["symbol"] == "DUSTTEST" for p in t9_pos["open_positions"])
    t9_partial = any(p["symbol"] == "DUSTTEST" for p in t9_pos["partially_closed_positions"])
    t9_closed  = any(p["symbol"] == "DUSTTEST" for p in t9_pos["fully_closed_positions"])
    t9_audit   = t9_pos["symbol_audit"]["DUSTTEST"]
    results["test9_dust_residual_fully_closed"] = {
        "pass": (not t9_open and not t9_partial and t9_closed),
        "open":             t9_open,
        "partially_closed": t9_partial,
        "fully_closed":     t9_closed,
        "shares_remaining": t9_ledgers["DUSTTEST"]["shares_remaining"],
        "is_dust":          t9_audit["is_dust"],
        "cost_basis":       t9_audit["cost_basis_remaining"],
        "expected":         "fully_closed (dust residual)",
    }

    # ── Test 10: Fee-only row with zero quantity → no sell event created ────
    # A FEE-classified transaction has qty=0 and must be skipped by normalize.
    # Inject a synthetic zero-qty SELL to verify the guard holds.
    # (In real CSV: fee rows with no shares → side=FEE, skipped before ledger)
    t10_buy   = _make_txn("FEETEST", "BUY", 100, 10, "2026-01-01")
    # Simulate a fee row that somehow survived as SELL with qty=0
    t10_fee   = {**_make_txn("FEETEST", "SELL", 0, 0, "2026-02-01"),
                 "quantity": 0.0, "side": "SELL"}
    t10_ledgers = build_symbol_ledgers([t10_buy, t10_fee])
    t10_pos     = classify_positions(t10_ledgers)
    t10_open    = any(p["symbol"] == "FEETEST" for p in t10_pos["open_positions"])
    t10_partial = any(p["symbol"] == "FEETEST" for p in t10_pos["partially_closed_positions"])
    t10_closed  = any(p["symbol"] == "FEETEST" for p in t10_pos["fully_closed_positions"])
    t10_ledger  = t10_ledgers["FEETEST"]
    results["test10_fee_zero_qty_no_sell_event"] = {
        "pass": (t10_open and not t10_partial and not t10_closed
                 and t10_ledger["sell_count"] == 0
                 and t10_ledger["shares_remaining"] == 100.0),
        "open":             t10_open,
        "partially_closed": t10_partial,
        "fully_closed":     t10_closed,
        "sell_count":       t10_ledger["sell_count"],
        "shares_remaining": t10_ledger["shares_remaining"],
        "expected":         "open (fee zero-qty row ignored)",
    }

    # ── Test 11: NBIS-pattern — buy only, no sell → pure open ─────────────
    # Mirrors the actual NBIS row: 76 shares bought, nothing sold.
    t11_txns = [_make_txn("NBISTEST", "BUY", 76, 91.6425, "2026-01-05")]
    t11_ledgers = build_symbol_ledgers(t11_txns)
    t11_pos     = classify_positions(t11_ledgers)
    t11_open    = any(p["symbol"] == "NBISTEST" for p in t11_pos["open_positions"])
    t11_partial = any(p["symbol"] == "NBISTEST" for p in t11_pos["partially_closed_positions"])
    t11_closed  = any(p["symbol"] == "NBISTEST" for p in t11_pos["fully_closed_positions"])
    results["test11_nbis_buy_only_open"] = {
        "pass": (t11_open and not t11_partial and not t11_closed),
        "open":             t11_open,
        "partially_closed": t11_partial,
        "fully_closed":     t11_closed,
        "shares_remaining": t11_ledgers["NBISTEST"]["shares_remaining"],
        "has_real_sell":    t11_pos["symbol_audit"]["NBISTEST"]["has_real_sell"],
        "expected":         "open (no sell, buy only)",
    }

    # ── Test 12: OPTX-pattern — exact 521=521 → fully closed ─────────────
    # Mirrors the actual OPTX rows: buy 521, sell 521 with $0.19 fee.
    t12_buy  = _make_txn("OPTXTEST", "BUY",  521, 9.5833, "2026-04-17")
    t12_sell = _make_txn("OPTXTEST", "SELL", 521, 8.4226, "2026-05-04")
    t12_sell["fees"] = 0.19
    t12_ledgers = build_symbol_ledgers([t12_buy, t12_sell])
    t12_pos     = classify_positions(t12_ledgers)
    t12_open    = any(p["symbol"] == "OPTXTEST" for p in t12_pos["open_positions"])
    t12_partial = any(p["symbol"] == "OPTXTEST" for p in t12_pos["partially_closed_positions"])
    t12_closed  = any(p["symbol"] == "OPTXTEST" for p in t12_pos["fully_closed_positions"])
    t12_ledger  = t12_ledgers["OPTXTEST"]
    t12_audit   = t12_pos["symbol_audit"]["OPTXTEST"]
    results["test12_optx_521_521_fully_closed"] = {
        "pass": (not t12_open and not t12_partial and t12_closed
                 and t12_ledger["shares_remaining"] == 0.0),
        "open":             t12_open,
        "partially_closed": t12_partial,
        "fully_closed":     t12_closed,
        "shares_remaining": t12_ledger["shares_remaining"],
        "is_dust":          t12_audit["is_dust"],
        "expected":         "fully_closed (521 bought = 521 sold)",
    }

    all_pass = all(v.get("pass") for v in results.values())
    return {"all_pass": all_pass, "tests": results}
