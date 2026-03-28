import assert from 'node:assert/strict';
import test from 'node:test';

import { normalizeHistory, normalizeHistoryBuckets } from '../history';

test('normalizeHistory parses bucketed API shape into flat entries', () => {
  const data = {
    'terminal::freeform_query': {
      category: 'terminal',
      intent: 'freeform_query',
      entries: [
        { id: '1', timestamp: 10, content: 'a', display_type: 'chat' },
      ],
    },
  };

  const flat = normalizeHistory(data);
  assert.equal(flat.length, 1);
  assert.equal(flat[0].category, 'terminal');
  assert.equal(flat[0].intent, 'freeform_query');
  assert.equal(flat[0].id, '1');
});

test('normalizeHistory includes display_type=chat entries', () => {
  const data = {
    'terminal::freeform_query': {
      category: 'terminal',
      intent: 'freeform_query',
      entries: [
        { id: 'chat-1', timestamp: 101, content: 'chat payload', display_type: 'chat' },
      ],
    },
  };

  const flat = normalizeHistory(data);
  assert.equal(flat.length, 1);
  assert.equal(flat[0].display_type, 'chat');
});

test('normalizeHistory sorts by timestamp desc and latest10 is slice(0,10)', () => {
  const entries = Array.from({ length: 12 }, (_, i) => ({
    id: `${i + 1}`,
    timestamp: i + 1,
    content: `c${i + 1}`,
    display_type: 'chat',
  }));

  const data = {
    'terminal::freeform_query': {
      category: 'terminal',
      intent: 'freeform_query',
      entries,
    },
  };

  const flat = normalizeHistory(data);
  assert.equal(flat[0].timestamp, 12);
  assert.equal(flat[11].timestamp, 1);

  const latest10 = flat.slice(0, 10);
  assert.equal(latest10.length, 10);
  assert.equal(latest10[0].timestamp, 12);
  assert.equal(latest10[9].timestamp, 3);
});

test('normalizeHistoryBuckets supports post-query refresh update shape', () => {
  const initial = normalizeHistoryBuckets({
    'overview::daily_briefing': {
      category: 'overview',
      intent: 'daily_briefing',
      entries: [{ id: 'old', timestamp: 100, content: 'old', display_type: 'report' }],
    },
  });
  assert.equal(initial['overview::daily_briefing'].entries.length, 1);

  const refreshed = normalizeHistoryBuckets({
    'overview::daily_briefing': {
      category: 'overview',
      intent: 'daily_briefing',
      entries: [
        { id: 'new', timestamp: 200, content: 'new', display_type: 'report' },
        { id: 'old', timestamp: 100, content: 'old', display_type: 'report' },
      ],
    },
  });

  assert.equal(refreshed['overview::daily_briefing'].entries.length, 2);
  assert.equal(refreshed['overview::daily_briefing'].entries[0].id, 'new');
});
