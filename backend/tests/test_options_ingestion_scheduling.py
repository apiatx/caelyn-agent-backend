import asyncio
import time
from datetime import datetime, timedelta, timezone

from data import options_ingestion as ingestion


def test_classify_ingestion_work_preserves_status_semantics(monkeypatch):
    monkeypatch.setattr(
        ingestion,
        "OPTIONS_WATCHLIST",
        ["NEW", "PENDING", "ERROR", "FRESH", "STALE", "BAD_DATE"],
    )
    now = datetime.now(timezone.utc)
    states = {
        "NEW": None,
        "PENDING": {"status": "pending"},
        "ERROR": {"status": "error"},
        "FRESH": {"status": "complete", "updated_at": now.isoformat()},
        "STALE": {
            "status": "complete",
            "updated_at": (now - timedelta(hours=7)).isoformat(),
        },
        "BAD_DATE": {"status": "complete", "updated_at": "not-a-date"},
    }

    pending, stale = ingestion._classify_ingestion_work(states.get)

    assert pending == ["NEW", "PENDING", "ERROR"]
    assert stale == ["STALE", "BAD_DATE"]


def test_progress_reads_do_not_block_event_loop(monkeypatch):
    monkeypatch.setattr(ingestion, "OPTIONS_WATCHLIST", ["A", "B"])

    def slow_getter(_ticker):
        time.sleep(0.05)
        return None

    async def run():
        task = asyncio.create_task(
            ingestion._load_ingestion_work_queue(slow_getter)
        )
        tick_started = time.monotonic()
        await asyncio.sleep(0.01)
        tick_elapsed = time.monotonic() - tick_started
        result = await task
        return tick_elapsed, result

    tick_elapsed, result = asyncio.run(run())

    assert tick_elapsed < 0.04
    assert result == (["A", "B"], [])