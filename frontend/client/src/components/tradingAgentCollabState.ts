export interface CollabPreset {
  id: string;
  label?: string;
  name?: string;
  reasoning_model?: string;
  primary?: string;
  agents?: string[];
  lock_agents?: boolean;
  lock_reasoning?: boolean;
}

export interface CollabState {
  selectedPresetId: string;
  reasoningModelRequest: string;
  primaryModel: string;
  reasoningModelUI: string;
  collabAgents: string[];
  lockAgents: boolean;
  lockReasoning: boolean;
}

export const DEFAULT_COLLAB_STATE: CollabState = {
  selectedPresetId: 'default',
  reasoningModelRequest: 'agent_collab',
  primaryModel: 'claude',
  reasoningModelUI: 'claude',
  collabAgents: ['grok', 'perplexity'],
  lockAgents: true,
  lockReasoning: false,
};

export function applyPresetState(preset: CollabPreset): CollabState {
  const primaryModel = preset.primary ?? 'claude';
  return {
    selectedPresetId: preset.id,
    reasoningModelRequest: preset.reasoning_model ?? 'agent_collab',
    primaryModel,
    reasoningModelUI: primaryModel,
    collabAgents: [...(preset.agents ?? [])],
    lockAgents: !!preset.lock_agents,
    lockReasoning: !!preset.lock_reasoning,
  };
}

export function buildCollabPayload(collabState: CollabState | null, selectedModel: string) {
  if (!collabState) {
    // Direct model mode — single model, no collab
    return { reasoning_model: selectedModel };
  }

  if (collabState.selectedPresetId === 'default') {
    // Caelyn / Auto mode — let backend handle all routing
    return {
      reasoning_model: 'agent_collab',
      primary_model: null,
      collab_agents: [],
    };
  }

  if (collabState.selectedPresetId === 'full_collab') {
    // Full collab — all selected agents reason independently
    return {
      reasoning_model: 'all_agents',
      primary_model: collabState.primaryModel || 'claude',
      collab_agents: collabState.collabAgents,
    };
  }

  // Custom collab — explicit collaborator selections
  return {
    reasoning_model: collabState.reasoningModelRequest || 'agent_collab',
    primary_model: collabState.primaryModel || 'claude',
    collab_agents: collabState.collabAgents,
  };
}

export function shouldKeepCollaboratorsOnReasoningChange(collabState: CollabState | null) {
  return !!collabState?.lockAgents;
}
