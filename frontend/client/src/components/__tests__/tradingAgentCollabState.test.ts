import assert from 'node:assert/strict';
import test from 'node:test';

import {
  applyPresetState,
  buildCollabPayload,
  DEFAULT_COLLAB_STATE,
} from '../tradingAgentCollabState';

test('initial load default preset maps state correctly', () => {
  const state = applyPresetState({
    id: 'default',
    reasoning_model: 'agent_collab',
    primary: 'claude',
    agents: ['grok', 'perplexity'],
    lock_agents: true,
    lock_reasoning: false,
  });

  assert.equal(state.selectedPresetId, 'default');
  assert.equal(state.reasoningModelRequest, 'agent_collab');
  assert.equal(state.primaryModel, 'claude');
  assert.equal(state.reasoningModelUI, 'claude');
  assert.deepEqual(state.collabAgents, ['grok', 'perplexity']);
  assert.equal(state.lockAgents, true);
  assert.equal(state.lockReasoning, false);
});

test('explicit click default after another preset restores collaborators', () => {
  const full = applyPresetState({
    id: 'full_collab',
    reasoning_model: 'all_agents',
    primary: 'claude',
    agents: ['claude', 'grok', 'gpt-4o', 'gemini', 'perplexity'],
    lock_agents: true,
    lock_reasoning: false,
  });
  assert.equal(full.selectedPresetId, 'full_collab');

  const backToDefault = applyPresetState({
    id: 'default',
    reasoning_model: 'agent_collab',
    primary: 'claude',
    agents: ['grok', 'perplexity'],
    lock_agents: true,
    lock_reasoning: false,
  });

  assert.deepEqual(backToDefault.collabAgents, ['grok', 'perplexity']);
  assert.equal(backToDefault.reasoningModelUI, 'claude');
});

test('payload builder sends canonical default payload', () => {
  const payload = buildCollabPayload(DEFAULT_COLLAB_STATE, 'grok');

  assert.deepEqual(payload, {
    reasoning_model: 'agent_collab',
    primary_model: 'claude',
    collab_agents: ['grok', 'perplexity'],
  });
});

test('full to default roundtrip payload remains stable', () => {
  const fullPayload = buildCollabPayload(
    applyPresetState({
      id: 'full_collab',
      reasoning_model: 'all_agents',
      primary: 'claude',
      agents: ['claude', 'grok', 'gpt-4o', 'gemini', 'perplexity'],
      lock_agents: true,
      lock_reasoning: false,
    }),
    'claude',
  );

  assert.equal(fullPayload.reasoning_model, 'all_agents');

  const defaultPayload = buildCollabPayload(
    applyPresetState({
      id: 'default',
      reasoning_model: 'agent_collab',
      primary: 'claude',
      agents: ['grok', 'perplexity'],
      lock_agents: true,
      lock_reasoning: false,
    }),
    'claude',
  );

  assert.deepEqual(defaultPayload.collab_agents, ['grok', 'perplexity']);
  assert.equal(defaultPayload.reasoning_model, 'agent_collab');
  assert.equal(defaultPayload.primary_model, 'claude');
});
