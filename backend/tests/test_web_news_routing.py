from pathlib import Path


def test_smart_orchestrator_schema_has_web_news_fields():
    text = Path('agent/prompts.py').read_text()
    assert '"web_news": false' in text
    assert '"needs_citations": false' in text
    assert '"news_query": "optional cleaned search query or null"' in text
    assert '"min_citations": 3' in text


def test_backend_enforcement_has_news_unavailable_error_code():
    text = Path('agent/claude_agent.py').read_text()
    assert 'NEWS_SOURCES_UNAVAILABLE' in text
    assert '[CITATION REQUIREMENT]' in text
    assert '_fetch_web_news_context' in text


def test_web_search_provider_tracks_provider_used():
    text = Path('data/web_search_provider.py').read_text()
    assert 'provider_used' in text
    assert '[WebNews]' in text
