"""
Regression tests: new Watchlist ticker identity + taxonomy reliability.

Covers three connected defects in the newly-added Watchlist ticker lifecycle:

  A. DeepSeek single-ticker auto-classification reliability
       - classifier is scheduled after membership commit (single + bulk add)
       - add HTTP response never waits for DeepSeek
       - the DeepSeek V4 Flash execution path actually executes
       - provider errors are observable, never silently converted into blank theme
       - no_valid_theme is recorded as a successful outcome
       - OTC:<ticker> canonical symbols can use the single-ticker classifier
       - existing manual taxonomy assignments are never overwritten

  B. Company-name correction
       - symbol fallback is repairable from cached metadata
       - proper company name is never overwritten by the ticker symbol

  C. Canonical sector override
       - FMP sector remains default when no manual override exists
       - manual sector override wins over FMP sector
       - clearing the manual override restores FMP sector behavior
       - atomic_taxonomy_write_db persists sector without breaking
         theme/subtheme/additional theme writes
       - taxonomy cache + BULK LKG invalidation still occurs after writes

Run:
    cd backend && python -m pytest tests/test_watchlist_ticker_identity.py -v
"""
from __future__ import annotations

import asyncio
import json
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_LONG_DESC = (
    "A global industrial technology company that designs and manufactures "
    "precision sensors and controls for automotive, aerospace and industrial "
    "markets worldwide, with thousands of products sold across more than 100 countries."
)


def _get_classifier():
    import services.watchlist_theme_classifier as wc
    return wc


def _get_router():
    import services.watchlist_router as wr
    return wr


# ══════════════════════════════════════════════════════════════════════════════
# A1 — New ticker schedules classifier after membership commit
# ══════════════════════════════════════════════════════════════════════════════

class TestAddSchedulesClassifier:
    async def _run_add(self, monkeypatch, body):
        import services.watchlist_router as wr

        classifier_calls: list[tuple] = []

        async def _fake_classify(*args, **kwargs):
            classifier_calls.append((args, kwargs))
            return {"action": "classified"}

        monkeypatch.setattr("data.pg_storage.watchlist_add_ticker",
                            lambda wl, t, family_aliases=None: {"added": True, "ticker_count": 1})
        monkeypatch.setattr("data.pg_storage.is_available", lambda: True)
        monkeypatch.setattr("services.canonical_security_adapter.exchange_family_aliases",
                            lambda t: [])
        monkeypatch.setattr("services.user_earnings_service.invalidate_user_earnings",
                            lambda u: None)
        monkeypatch.setattr("services.watchlist_quote_cache.is_fmp_symbol_eligible",
                            lambda t: False)

        async def _fake_priority(syms, wl_id):
            return None

        monkeypatch.setattr("services.watchlist_router._priority_hydrate_symbols",
                            _fake_priority)
        monkeypatch.setattr("services.watchlist_theme_classifier.classify_and_assign_ticker",
                            _fake_classify)

        resp = await asyncio.wait_for(
            wr.add_ticker_endpoint("wl-add", body), timeout=5.0,
        )
        # Let the created background task run.
        await asyncio.sleep(0)
        return resp, classifier_calls

    @pytest.mark.asyncio
    async def test_add_schedules_classifier_after_commit(self, monkeypatch):
        wr = _get_router()
        body = wr._AddTickerBody(canonical_ticker="NVDA", company_name="NVIDIA Corp")
        resp, calls = await self._run_add(monkeypatch, body)

        assert resp["success"] is True
        assert resp["added"] is True
        assert calls, "classifier must be scheduled after membership commit"
        args, kwargs = calls[0]
        assert args[0] == "NVDA"
        assert args[1] == "NVIDIA Corp", "company_name from the add payload must be reused"

    @pytest.mark.asyncio
    async def test_add_response_does_not_wait_for_deepseek(self, monkeypatch):
        import services.watchlist_router as wr

        started = asyncio.Event()
        release = asyncio.Event()
        classifier_done: list[bool] = []

        async def _slow_classify(*args, **kwargs):
            started.set()
            await release.wait()
            classifier_done.append(True)
            return {"action": "classified"}

        monkeypatch.setattr("data.pg_storage.watchlist_add_ticker",
                            lambda wl, t, family_aliases=None: {"added": True, "ticker_count": 1})
        monkeypatch.setattr("data.pg_storage.is_available", lambda: True)
        monkeypatch.setattr("services.canonical_security_adapter.exchange_family_aliases",
                            lambda t: [])
        monkeypatch.setattr("services.user_earnings_service.invalidate_user_earnings",
                            lambda u: None)
        monkeypatch.setattr("services.watchlist_quote_cache.is_fmp_symbol_eligible",
                            lambda t: False)

        async def _fake_priority(syms, wl_id):
            return None

        monkeypatch.setattr("services.watchlist_router._priority_hydrate_symbols",
                            _fake_priority)
        monkeypatch.setattr("services.watchlist_theme_classifier.classify_and_assign_ticker",
                            _slow_classify)

        body = wr._AddTickerBody(canonical_ticker="NVDA", company_name="NVIDIA Corp")
        resp = await asyncio.wait_for(wr.add_ticker_endpoint("wl-add", body), timeout=5.0)

        # Response returned while the classifier task is still blocked.
        assert resp["success"] is True
        assert not classifier_done, (
            "add HTTP response must NOT wait for the DeepSeek classifier task"
        )
        assert started.is_set(), "classifier task should have started in the background"
        release.set()  # cleanup

    @pytest.mark.asyncio
    async def test_bulk_add_schedules_classifier_per_new_ticker(self, monkeypatch):
        import services.watchlist_router as wr

        classifier_calls: list[str] = []

        async def _fake_classify(ticker, *args, **kwargs):
            classifier_calls.append(ticker)
            return {"action": "classified"}

        monkeypatch.setattr("data.pg_storage.watchlist_add_ticker",
                            lambda wl, t, family_aliases=None: {"added": True, "ticker_count": 1})
        monkeypatch.setattr("data.pg_storage.is_available", lambda: True)
        monkeypatch.setattr("services.canonical_security_adapter.exchange_family_aliases",
                            lambda t: [])
        monkeypatch.setattr("services.user_earnings_service.invalidate_user_earnings",
                            lambda u: None)
        monkeypatch.setattr("services.watchlist_quote_cache.is_fmp_symbol_eligible",
                            lambda t: False)

        async def _fake_priority(syms, wl_id):
            return None

        monkeypatch.setattr("services.watchlist_router._priority_hydrate_symbols",
                            _fake_priority)
        monkeypatch.setattr("services.watchlist_theme_classifier.classify_and_assign_ticker",
                            _fake_classify)

        body = wr._BulkAddBody(tickers=["CG", "STDN", "OTC:VNPKF"])
        resp = await asyncio.wait_for(
            wr.bulk_add_tickers_endpoint("wl-bulk", body), timeout=5.0,
        )
        await asyncio.sleep(0)

        assert resp["success"] is True
        assert sorted(classifier_calls) == ["CG", "OTC:VNPKF", "STDN"], (
            "every genuinely-new bulk-added ticker must schedule classification"
        )


# ══════════════════════════════════════════════════════════════════════════════
# A3–A7 — Single-ticker classifier behavior (DeepSeek V4 Flash path)
# ══════════════════════════════════════════════════════════════════════════════

class _ClassifierHarness:
    """Common mocking harness for classify_and_assign_ticker."""

    def setup(self, monkeypatch,
              deepseek_result: Optional[dict],
              persist_result: bool = True,
              category_overrides: Optional[dict] = None,
              ticker_overrides: Optional[list] = None,
              needs_review_capture: Optional[list] = None):
        self.persist_calls: list[dict] = []
        self.deepseek_calls: list[tuple] = []

        async def _fake_deepseek(prompt, model_name):
            self.deepseek_calls.append((prompt, model_name))
            return deepseek_result

        async def _fake_persist(validated):
            self.persist_calls.append(validated)
            return persist_result

        if needs_review_capture is None:
            needs_review_capture = self.needs_review_calls = []

        def _fake_mark(symbols, reason="llm_failed"):
            needs_review_capture.append((list(symbols), reason))

        monkeypatch.setattr(
            "services.watchlist_theme_classifier._call_deepseek_taxonomy", _fake_deepseek,
        )
        monkeypatch.setattr(
            "services.watchlist_theme_classifier._persist_classification", _fake_persist,
        )
        monkeypatch.setattr(
            "services.watchlist_theme_classifier.mark_needs_review", _fake_mark,
        )
        monkeypatch.setattr(
            "services.category_overrides.get_overrides",
            lambda uid="default": dict(category_overrides or {}),
        )
        monkeypatch.setattr(
            "data.pg_storage.get_theme_ticker_overrides",
            lambda theme_id=None: list(ticker_overrides or []),
        )

    async def run(self, ticker: str, company_name: str = "", description: str = "",
                  sector: str = "") -> dict:
        wc = _get_classifier()
        return await wc.classify_and_assign_ticker(
            ticker,
            company_name=company_name,
            description=description,
            sector=sector,
            hydrate_attempts=0,
            hydrate_delay=0.0,
        )


@pytest.fixture
def harness(monkeypatch):
    h = _ClassifierHarness()
    h.needs_review_calls = []
    h.setup(monkeypatch, deepseek_result=None)
    return h


class TestDeepSeekExecutionPath:
    @pytest.mark.asyncio
    async def test_deepseek_v4_flash_path_executes(self, monkeypatch):
        h = _ClassifierHarness()
        h.setup(monkeypatch, deepseek_result={
            "ticker": "NVDA",
            "company_name": "NVIDIA Corp",
            "primary_theme_id": "semiconductors",
            "primary_subtheme_id": None,
            "additional_theme_ids": [],
            "confidence": 0.95,
            "rationale": "dominant AI accelerator silicon",
            "no_valid_theme": False,
        })
        result = await h.run("NVDA", "NVIDIA Corp", _LONG_DESC, "Technology")

        assert result["action"] == "classified", result
        assert len(h.deepseek_calls) == 1, "DeepSeek taxonomy call must execute"
        prompt, model_name = h.deepseek_calls[0]
        assert model_name == "deepseek-v4-flash", (
            f"must execute the DeepSeek V4 Flash path, got model {model_name!r}"
        )
        assert "NVIDIA Corp" in prompt and "NVDA" in prompt
        assert len(h.persist_calls) == 1
        assert h.persist_calls[0]["primary_theme_id"] == "semiconductors"

    @pytest.mark.asyncio
    async def test_provider_error_is_observable(self, monkeypatch):
        h = _ClassifierHarness()
        needs_review: list = []
        h.setup(monkeypatch, deepseek_result=None, needs_review_capture=needs_review)
        result = await h.run("NVDA", "NVIDIA Corp", _LONG_DESC, "Technology")

        assert result["action"] == "provider_failed", result
        assert result["error"], "provider failure must carry an observable error"
        assert "DeepSeek" in result["error"]
        assert "primary_theme_id" not in result, (
            "provider failure must NOT be converted into a fake/blank theme assignment"
        )
        assert h.persist_calls == [], "nothing may be persisted on provider failure"
        assert needs_review == [(["NVDA"], "deepseek_api_failure")], (
            "provider failure must be recorded in needs_review"
        )
        wc = _get_classifier()
        status = wc.get_ticker_classification_status("NVDA")
        assert status["action"] == "provider_failed"

    @pytest.mark.asyncio
    async def test_no_valid_theme_is_recorded_success(self, monkeypatch):
        h = _ClassifierHarness()
        h.setup(monkeypatch, deepseek_result={
            "ticker": "CG",
            "company_name": "The Carlyle Group Inc",
            "primary_theme_id": None,
            "primary_subtheme_id": None,
            "additional_theme_ids": [],
            "confidence": 0.0,
            "rationale": "asset manager with no thematic identity",
            "no_valid_theme": True,
        })
        result = await h.run("CG", "The Carlyle Group Inc", _LONG_DESC, "Financials")

        assert result["action"] == "no_valid_theme", result
        assert result["error"] is None, (
            "no_valid_theme is a SUCCESSFUL outcome, not an error"
        )
        assert h.persist_calls == [], "no_valid_theme must not persist fake themes"
        assert h.needs_review_calls == [], "no_valid_theme is not a needs-review error"
        wc = _get_classifier()
        status = wc.get_ticker_classification_status("CG")
        assert status["action"] == "no_valid_theme"

    @pytest.mark.asyncio
    async def test_otc_ticker_uses_single_ticker_classifier(self, monkeypatch):
        h = _ClassifierHarness()
        h.setup(monkeypatch, deepseek_result={
            "ticker": "OTC:VNPKF",
            "company_name": "Vianini Pipe Co",
            "primary_theme_id": "construction_infrastructure",
            "primary_subtheme_id": None,
            "additional_theme_ids": [],
            "confidence": 0.85,
            "rationale": "industrial pipe manufacturer",
            "no_valid_theme": False,
        })
        result = await h.run("OTC:VNPKF", "Vianini Pipe Co", _LONG_DESC, "Industrials")

        assert result["action"] == "classified", result
        assert len(h.deepseek_calls) == 1
        assert "OTC:VNPKF" in h.deepseek_calls[0][0], (
            "canonical OTC:<ticker> identity must be preserved internally"
        )
        assert h.persist_calls[0]["ticker"] == "OTC:VNPKF", (
            "OTC prefix must NOT be stripped before canonical persistence"
        )

    @pytest.mark.asyncio
    async def test_existing_manual_taxonomy_never_overwritten(self, monkeypatch):
        # Case A — watchlist_category_overrides manual assignment
        h = _ClassifierHarness()
        h.setup(monkeypatch, deepseek_result=None,
                category_overrides={"NVDA": "Semiconductors"})
        result = await h.run("NVDA", "NVIDIA Corp", _LONG_DESC, "Technology")
        assert result["action"] == "already_assigned", result
        assert h.deepseek_calls == [], "no DeepSeek call for manually assigned ticker"

        # Case B — canonical theme_ticker_overrides membership
        h2 = _ClassifierHarness()
        h2.setup(monkeypatch, deepseek_result=None,
                 ticker_overrides=[
                     {"theme_id": "semiconductors", "symbol": "AMD", "action": "add",
                      "source": "manual_admin", "note": None, "created_by": None,
                      "created_at": None, "updated_at": None},
                 ])
        result2 = await h2.run("AMD", "Advanced Micro Devices", _LONG_DESC, "Technology")
        assert result2["action"] == "already_assigned", result2
        assert h2.deepseek_calls == [], "no DeepSeek call for canonical-mapped ticker"


# ══════════════════════════════════════════════════════════════════════════════
# A — metadata hydration / retry
# ══════════════════════════════════════════════════════════════════════════════

class TestMetadataHydration:
    def _mock_pg_fields(self, monkeypatch, fields_by_symbol: dict[str, dict]):
        """Mock data.pg_storage._get_conn to serve watchlist_fundamentals_cache rows."""
        class _Cursor:
            def __init__(self):
                self._row = None

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, params=None):
                sym = params[0] if params else None
                fields = fields_by_symbol.get(sym)
                self._row = (fields,) if fields is not None else None

            def fetchone(self):
                return self._row

        class _Conn:
            def cursor(self):
                return _Cursor()

        monkeypatch.setattr("data.pg_storage._get_conn", lambda caller="": _Conn())
        monkeypatch.setattr("data.pg_storage._put_conn", lambda conn: None)

    @pytest.mark.asyncio
    async def test_company_name_symbol_fallback_repairable_from_cached_metadata(self, monkeypatch):
        self._mock_pg_fields(monkeypatch, {
            "STDN": {
                "profile": {
                    "companyName": "Standard Nuclear Inc",
                    "description": _LONG_DESC,
                    "sector": "Energy",
                },
            },
        })
        wc = _get_classifier()
        company, desc, sect = await wc._hydrate_ticker_metadata("STDN", "", "", "")
        assert company == "Standard Nuclear Inc"
        assert len(desc) >= wc._MIN_DESCRIPTION_LEN
        assert sect == "Energy"

    @pytest.mark.asyncio
    async def test_classifier_proceeds_when_metadata_hydrates_from_cache(self, monkeypatch):
        self._mock_pg_fields(monkeypatch, {
            "STDN": {
                "profile": {
                    "companyName": "Standard Nuclear Inc",
                    "description": _LONG_DESC,
                    "sector": "Energy",
                },
            },
        })
        h = _ClassifierHarness()
        h.setup(monkeypatch, deepseek_result={
            "ticker": "STDN",
            "company_name": "Standard Nuclear Inc",
            "primary_theme_id": "clean_energy",
            "primary_subtheme_id": None,
            "additional_theme_ids": [],
            "confidence": 0.9,
            "rationale": "nuclear fuel",
            "no_valid_theme": False,
        })
        # company_name + description arrive ONLY from the cached profile
        result = await h.run("STDN", "", "", "")
        assert result["action"] == "classified", result
        assert len(h.deepseek_calls) == 1
        assert "Standard Nuclear Inc" in h.deepseek_calls[0][0]

    @pytest.mark.asyncio
    async def test_metadata_insufficient_is_observable_not_silent(self, monkeypatch):
        self._mock_pg_fields(monkeypatch, {})  # no cached row at all
        h = _ClassifierHarness()
        h.setup(monkeypatch, deepseek_result=None)
        result = await h.run("RRX", "Regal Rexnord Corp", "", "Industrials")
        assert result["action"] == "metadata_insufficient", result
        assert "METADATA_INSUFFICIENT" in (result["error"] or "")
        assert h.deepseek_calls == [], "no provider call on insufficient metadata"
        wc = _get_classifier()
        assert wc.get_ticker_classification_status("RRX")["action"] == "metadata_insufficient"


# ══════════════════════════════════════════════════════════════════════════════
# B — Company-name repair (canonical watchlist csv_data)
# ══════════════════════════════════════════════════════════════════════════════

class TestCompanyNameRepair:
    def _capture_update_payload(self, monkeypatch):
        """Mock the pg connection; capture the csv_data payload in the UPDATE."""
        import data.pg_storage as pgs

        captured: dict = {}

        class _Cursor:
            def __init__(self, csv_rows):
                self._rows = csv_rows

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, params=None):
                captured.setdefault("sqls", []).append(sql.strip())
                if isinstance(params, tuple) and params and sql.strip().startswith("UPDATE public.watchlist SET csv_data"):
                    captured["update_payload"] = params[0].adapted if hasattr(params[0], "adapted") else params[0]

            def fetchone(self):
                return (list(self._rows),)

            def close(self):
                pass

        class _Conn:
            def cursor(self):
                return _Cursor(captured.get("initial_rows", []))

            def commit(self):
                captured["commit"] = True

            def rollback(self):
                captured["rollback"] = True

        monkeypatch.setattr(pgs, "_get_conn", lambda caller="": _Conn())
        monkeypatch.setattr(pgs, "_put_conn", lambda conn: None)
        return captured

    def test_symbol_fallback_repaired_and_proper_name_preserved(self, monkeypatch):
        import data.pg_storage as pgs

        captured = self._capture_update_payload(monkeypatch)
        captured["initial_rows"] = [
            {"Symbol": "STDN"},
            {"Symbol": "LODE", "Name": "Comstock Inc"},
            {"Symbol": "PCT", "Company Name": "PCT"},   # ticker-as-name must be repaired
            {"Symbol": "CG", "Name": "The Carlyle Group Inc"},  # proper name preserved
        ]

        result = pgs.watchlist_set_ticker_names("wl-x", {
            "STDN": "Standard Nuclear Inc",
            "LODE": "Overwrite Attempt Corp",
            "PCT": "PureCycle Technologies Inc",
            "CG": "Carlyle Overwrite Attempt",
        })

        assert result["repaired"] == 2, result  # STDN + PCT only
        payload = captured.get("update_payload")
        assert payload is not None, "canonical watchlist row update must execute"
        rows = {str(r.get("Symbol", "")).upper(): r for r in payload}
        assert rows["STDN"].get("Name") == "Standard Nuclear Inc"
        assert rows["PCT"].get("Name") == "PureCycle Technologies Inc"
        assert rows["LODE"].get("Name") == "Comstock Inc", "proper name must not be overwritten"
        assert rows["CG"].get("Name") == "The Carlyle Group Inc", "proper name must not be overwritten"

    def test_noop_when_all_names_proper(self, monkeypatch):
        import data.pg_storage as pgs

        captured = self._capture_update_payload(monkeypatch)
        captured["initial_rows"] = [{"Symbol": "LODE", "Name": "Comstock Inc"}]
        result = pgs.watchlist_set_ticker_names("wl-x", {"LODE": "Comstock Inc"})
        assert result["repaired"] == 0
        assert "update_payload" not in captured, "no write when nothing to repair"


# ══════════════════════════════════════════════════════════════════════════════
# C — Sector override precedence in the Watchlist read path
# ══════════════════════════════════════════════════════════════════════════════

class TestSectorPrecedenceInRows:
    async def _build_rows(self, monkeypatch, sector_overrides: dict[str, str]):
        import services.watchlist_router as wr

        monkeypatch.setattr(
            "services.watchlist_quote_cache.get_watchlist_quotes",
            AsyncMock(return_value={
                "CG": {"name": "The Carlyle Group Inc", "price": 48.0,
                       "quote_source": "tradier", "quote_updated_at": "t"},
            }),
        )
        monkeypatch.setattr("services.name_overrides.get_name_overrides", lambda uid="default": {})
        monkeypatch.setattr("data.pg_storage.get_category_sector_overrides",
                            lambda uid="default": dict(sector_overrides))
        monkeypatch.setattr("data.watchlist_fundamentals_store.get_snapshots_bulk",
                            lambda syms: {
                                "CG": {
                                    "fields": {
                                        "profile": {
                                            "companyName": "The Carlyle Group Inc",
                                            "sector": "Financial Services",
                                            "industry": "Asset Management",
                                        },
                                    },
                                    "refreshed_at": "2026-08-08T00:00:00Z",
                                },
                            })
        monkeypatch.setattr("data.pg_storage.get_theme_ticker_overrides",
                            lambda theme_id=None: [])
        monkeypatch.setattr("services.theme_resolver.build_theme_resolution_context",
                            lambda: {"themes_page_map": {}, "themes_page_id_map": {},
                                     "cat_overrides": {}})
        monkeypatch.setattr(
            "services.theme_resolver.resolve_primary_theme_for_ticker",
            lambda ticker, industry=None, ctx=None: {
                "theme_name": None, "theme_id": None, "source": "no_mapping",
            },
        )
        monkeypatch.setattr("services.watchlist_router._load_cached_watchlist_market_data",
                            lambda syms: {})
        monkeypatch.setattr("services.watchlist_router._apply_rv_rank_fields",
                            AsyncMock(side_effect=lambda wl, sections, saved: sections))
        monkeypatch.setattr("services.watchlist_router._apply_volmc_rank_fields",
                            AsyncMock(side_effect=lambda wl, sections, saved: sections))

        store = {
            "id": "wl-sec",
            "tickers": ["CG"],
            "csv_data": [{"Symbol": "CG"}],
            "analysis": {},
            "saved_at": "2026-08-08T00:00:00Z",
        }
        enriched = await wr._enrich_store_with_quotes(store)
        return enriched["analysis"]["sections"][0]["tickers"][0]

    @pytest.mark.asyncio
    async def test_fmp_sector_default_when_no_manual_override(self, monkeypatch):
        row = await self._build_rows(monkeypatch, {})
        assert row["sector_id"] == "financials", row
        assert row["provider_sector_id"] == "financials"
        assert row["manual_sector_id"] is None
        assert row["sector_source"] == "fmp"

    @pytest.mark.asyncio
    async def test_manual_sector_override_wins_over_fmp(self, monkeypatch):
        row = await self._build_rows(monkeypatch, {"CG": "technology"})
        assert row["sector_id"] == "technology", (
            "manual sector override must win over provider/FMP sector"
        )
        assert row["provider_sector_id"] == "financials", (
            "provider sector must remain visible for provenance"
        )
        assert row["manual_sector_id"] == "technology"
        assert row["sector_source"] == "manual_override"

    @pytest.mark.asyncio
    async def test_clearing_manual_sector_restores_fmp(self, monkeypatch):
        # After the manual override is cleared, provider/FMP sector is used again.
        row = await self._build_rows(monkeypatch, {})
        assert row["sector_id"] == "financials"
        assert row["sector_source"] == "fmp"


class TestAtomicSectorWrite:
    def _capture_sqls(self, monkeypatch, csv_unused=None):
        import data.pg_storage as pgs

        captured: dict = {"sqls": [], "args": [], "rollbacks": 0, "commits": 0}

        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, params=None):
                captured["sqls"].append(str(sql))
                captured["args"].append(params)

        class _Conn:
            def cursor(self):
                return _Cursor()

            def commit(self):
                captured["commits"] += 1

            def rollback(self):
                captured["rollbacks"] += 1

        monkeypatch.setattr(pgs, "_get_conn", lambda caller="": _Conn())
        monkeypatch.setattr(pgs, "_put_conn", lambda conn: None)
        return captured

    def test_atomic_save_includes_sector_without_breaking_themes(self, monkeypatch):
        import data.pg_storage as pgs

        captured = self._capture_sqls(monkeypatch)
        result = pgs.atomic_taxonomy_write_db(
            ticker_overrides=[
                {"theme_id": "semiconductors", "symbol": "TSTX", "action": "add",
                 "source": "deepseek_auto_classify", "note": "primary"},
                {"theme_id": "ai_accelerators", "symbol": "TSTX", "action": "add",
                 "source": "deepseek_auto_classify", "note": "subtheme"},
                {"theme_id": "cloud_software", "symbol": "TSTX", "action": "add",
                 "source": "deepseek_auto_classify", "note": "additional"},
            ],
            primary_operation={
                "action": "set", "user_id": "default", "ticker": "TSTX",
                "category": "Semiconductors", "source": "deepseek_auto_classify",
                "reason": "auto",
            },
            sector_override={
                "action": "set", "user_id": "default", "ticker": "TSTX",
                "sector_id": "technology", "source": "manual_sector_override",
            },
        )

        assert result["ok"] is True, result
        assert result["succeeded"] == 3, "theme/subtheme/additional writes must all succeed"

        theme_upserts = [s for s in captured["sqls"] if "theme_ticker_overrides" in s]
        assert len(theme_upserts) == 3

        sector_sqls = [s for s in captured["sqls"]
                       if "watchlist_category_overrides" in s and "sector_id" in s]
        assert sector_sqls, "sector override must be written in the same transaction"
        sector_args = [a for s, a in zip(captured["sqls"], captured["args"])
                       if "watchlist_category_overrides" in s and "sector_id" in s]
        assert any("technology" in (a or ()) for a in sector_args), (
            "sector_id must be persisted with the theme writes"
        )
        assert captured["commits"] == 1 and captured["rollbacks"] == 0

    def test_sector_clear_sets_null_and_restores_provider_behavior(self, monkeypatch):
        import data.pg_storage as pgs

        captured = self._capture_sqls(monkeypatch)
        result = pgs.atomic_taxonomy_write_db(
            ticker_overrides=[],
            sector_override={"action": "clear", "user_id": "default", "ticker": "CG"},
        )
        assert result["ok"] is True, result
        clear_sqls = [s for s in captured["sqls"]
                      if "sector_id = NULL" in s and "watchlist_category_overrides" in s]
        assert clear_sqls, "clear must set sector_id = NULL (restore provider sector)"

    def test_theme_clear_preserves_manual_sector(self, monkeypatch):
        import data.pg_storage as pgs

        captured = self._capture_sqls(monkeypatch)
        result = pgs.atomic_taxonomy_write_db(
            ticker_overrides=[],
            primary_operation={"action": "clear", "user_id": "default", "ticker": "CG"},
        )
        assert result["ok"] is True, result
        # DELETE still present (row without sector override)
        assert any("DELETE FROM public.watchlist_category_overrides" in s for s in captured["sqls"])
        # And the sector-preserving blank-out UPDATE is issued
        assert any(
            "UPDATE public.watchlist_category_overrides" in s and "category = ''" in s
            for s in captured["sqls"]
        )

    def test_failed_sector_write_rolls_back_theme_writes(self, monkeypatch):
        import data.pg_storage as pgs

        class _BadConn:
            def cursor(self):
                raise Exception("db down")

            def rollback(self):
                pass

        monkeypatch.setattr(pgs, "_get_conn", lambda caller="": _BadConn())
        result = pgs.atomic_taxonomy_write_db(
            ticker_overrides=[{"theme_id": "semiconductors", "symbol": "TSTX", "action": "add"}],
            sector_override={"action": "set", "user_id": "default", "ticker": "TSTX",
                             "sector_id": "technology"},
        )
        assert result["ok"] is False, "DB failure must surface as ok=False (no fake success)"


# ══════════════════════════════════════════════════════════════════════════════
# C — Taxonomy PUT route sector override + cache/BULK-LKG invalidation
# ══════════════════════════════════════════════════════════════════════════════

class TestTaxonomyPutSectorOverride:
    def _run_put(self, monkeypatch, body_dict: dict, txn_ok: bool = True):
        import asyncio
        import routes.themes as rth
        from unittest.mock import MagicMock
        import fastapi

        atomic_calls: list[dict] = []
        invalidate_overrides_calls: list = []
        lkg_inval_calls: list = []

        def fake_atomic(ticker_overrides, primary_operation=None, category_override=None,
                        sector_override=None):
            atomic_calls.append({
                "ticker_overrides": ticker_overrides,
                "primary_operation": primary_operation,
                "sector_override": sector_override,
            })
            if not txn_ok:
                return {"ok": False, "succeeded": 0, "failed": 1, "error": "injected"}
            return {"ok": True, "succeeded": len(ticker_overrides), "failed": 0, "error": None}

        fake_uni = {
            "technology": {"display_name": "Technology", "classification": "sector",
                           "assignable": False, "proxy_symbols": []},
            "semiconductors": {"display_name": "Semiconductors", "classification": "theme",
                               "assignable": True, "parent_theme_id": "technology",
                               "proxy_symbols": []},
            "cloud_software": {"display_name": "Cloud Software", "classification": "theme",
                               "assignable": True, "parent_theme_id": "technology",
                               "proxy_symbols": []},
        }

        def fake_memberships(ticker):
            return {
                "ticker": ticker,
                "primary_theme": {"theme_id": None, "theme_name": None, "source": "no_mapping"},
                "theme_memberships": [],
                "additional_theme_memberships": [],
            }

        def fake_invalidate_overrides(uid="default"):
            invalidate_overrides_calls.append(uid)

        def fake_lkg_inval(ticker):
            lkg_inval_calls.append(ticker)

        monkeypatch.setattr("data.pg_storage.atomic_taxonomy_write_db", fake_atomic)
        monkeypatch.setattr(rth, "_get_ticker_theme_memberships", fake_memberships)
        monkeypatch.setattr(rth, "_check_admin", lambda req, key: None)
        monkeypatch.setattr(rth, "_log", MagicMock())
        monkeypatch.setattr("services.theme_merge_layer.ENRICHED_THEME_RS_UNIVERSE", fake_uni)
        monkeypatch.setattr("services.theme_rs_universe.THEME_RS_UNIVERSE", fake_uni)
        monkeypatch.setattr("services.category_overrides.invalidate_overrides_cache",
                            fake_invalidate_overrides)
        monkeypatch.setattr("services.watchlist_router.invalidate_bulk_lkg_for_ticker",
                            fake_lkg_inval)
        monkeypatch.setattr(rth, "_REFRESH_GEN_LOCK", MagicMock())
        monkeypatch.setattr(rth, "_REFRESH_LOCK", MagicMock())

        class _FakeRequest:
            headers = {}
            state = MagicMock()

        body = rth.TickerTaxonomyBody(**body_dict)
        background_tasks = fastapi.BackgroundTasks()

        async def _run():
            return await rth.admin_put_ticker_taxonomy(
                ticker="CG",
                request=_FakeRequest(),
                body=body,
                background_tasks=background_tasks,
                x_api_key="test",
            )

        resp = asyncio.run(_run())
        return resp, atomic_calls, invalidate_overrides_calls, lkg_inval_calls

    def test_put_sector_set_persists_and_invalidates(self, monkeypatch):
        resp, atomic_calls, inv_ov, lkg = self._run_put(
            monkeypatch,
            {"primary_theme_id": None, "additional_theme_ids": [], "sector_id": "technology"},
        )
        assert resp["ok"] is True
        assert resp["sector_id"] == "technology"
        assert resp["sector_changed"] is True
        assert atomic_calls[0]["sector_override"] == {
            "action": "set", "user_id": "default", "ticker": "CG",
            "sector_id": "technology", "source": "manual_sector_override",
            "reason": None,
        }
        assert inv_ov == ["default"], "category overrides cache must be invalidated"
        assert lkg == ["CG"], "BULK LKG must be invalidated for the ticker"

    def test_put_sector_clear_persists_clear_op(self, monkeypatch):
        resp, atomic_calls, inv_ov, lkg = self._run_put(
            monkeypatch,
            {"primary_theme_id": None, "additional_theme_ids": [], "sector_id": None},
        )
        assert resp["ok"] is True
        assert resp["sector_id"] is None
        assert resp["sector_changed"] is True
        assert atomic_calls[0]["sector_override"] == {
            "action": "clear", "user_id": "default", "ticker": "CG",
        }

    def test_put_invalid_sector_rejected(self, monkeypatch):
        import fastapi
        exc_holder = []

        import routes.themes as rth
        from unittest.mock import MagicMock
        monkeypatch.setattr(rth, "_check_admin", lambda req, key: None)
        monkeypatch.setattr("services.theme_merge_layer.ENRICHED_THEME_RS_UNIVERSE", {
            "technology": {"display_name": "Technology", "classification": "sector",
                           "assignable": False, "proxy_symbols": []},
        })
        monkeypatch.setattr("services.theme_rs_universe.THEME_RS_UNIVERSE", {
            "technology": {"display_name": "Technology", "classification": "sector",
                           "assignable": False, "proxy_symbols": []},
            "semiconductors": {"display_name": "Semiconductors", "classification": "theme",
                               "assignable": True, "proxy_symbols": []},
        })

        class _FakeRequest:
            headers = {}
            state = MagicMock()

        async def _run():
            try:
                await rth.admin_put_ticker_taxonomy(
                    ticker="CG",
                    request=_FakeRequest(),
                    body=rth.TickerTaxonomyBody(sector_id="not_a_sector"),
                    background_tasks=fastapi.BackgroundTasks(),
                    x_api_key="test",
                )
            except fastapi.HTTPException as e:
                exc_holder.append(e)

        asyncio.run(_run())
        assert exc_holder and exc_holder[0].status_code == 422, (
            "non-canonical sector_id must be rejected with 422"
        )

    def test_put_sector_plus_themes_goes_through_one_atomic_call(self, monkeypatch):
        resp, atomic_calls, inv_ov, lkg = self._run_put(
            monkeypatch,
            {"primary_theme_id": "semiconductors",
             "additional_theme_ids": ["cloud_software"],
             "sector_id": "technology"},
        )
        assert resp["ok"] is True
        assert resp["primary_theme_id"] == "semiconductors"
        assert resp["additional_theme_ids"] == ["cloud_software"]
        assert resp["sector_id"] == "technology"
        assert len(atomic_calls) == 1, "exactly one atomic write for themes + sector"
        assert atomic_calls[0]["primary_operation"]["action"] == "set"
        assert atomic_calls[0]["sector_override"]["action"] == "set"


# ══════════════════════════════════════════════════════════════════════════════
# A — _persist_classification invalidation contract
# ══════════════════════════════════════════════════════════════════════════════

class TestPersistClassificationInvalidation:
    @pytest.mark.asyncio
    async def test_persist_invalidates_overrides_cache_and_bulk_lkg(self, monkeypatch):
        inv_ov: list = []
        lkg: list = []

        def fake_atomic(ticker_overrides, primary_operation=None, category_override=None,
                        sector_override=None):
            return {"ok": True, "succeeded": len(ticker_overrides), "failed": 0, "error": None}

        monkeypatch.setattr("data.pg_storage.atomic_taxonomy_write_db", fake_atomic)
        monkeypatch.setattr("services.category_overrides.invalidate_overrides_cache",
                            lambda uid="default": inv_ov.append(uid))
        monkeypatch.setattr("services.watchlist_router.invalidate_bulk_lkg_for_ticker",
                            lambda ticker: lkg.append(ticker))

        wc = _get_classifier()
        ok = await wc._persist_classification({
            "ticker": "TSTX",
            "primary_theme_id": "semiconductors",
            "primary_subtheme_id": None,
            "additional_theme_ids": ["ai_accelerators"],
            "confidence": 0.9,
            "rationale": "test",
            "no_valid_theme": False,
        })
        assert ok is True
        assert inv_ov == ["default"], "category overrides cache must be invalidated"
        assert lkg == ["TSTX"], "BULK LKG must be invalidated for the ticker"
