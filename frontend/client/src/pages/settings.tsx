import { useState, useEffect, useCallback } from "react";
import { GlassCard } from "@/components/ui/glass-card";
import { Button } from "@/components/ui/button";
import { Settings, Save, Plus, RotateCcw, ChevronDown, ChevronUp, Check, Loader2, X } from "lucide-react";

const AGENT_BACKEND_URL = "https://fast-api-server-trading-agent-aidanpilon.replit.app";
const AGENT_API_KEY = "hippo_ak_7f3x9k2m4p8q1w5t";

function getToken(): string | null {
  return localStorage.getItem('caelyn_token') || sessionStorage.getItem('caelyn_token');
}

function authHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const h: Record<string, string> = { 'Content-Type': 'application/json', 'X-API-Key': AGENT_API_KEY, ...extra };
  const t = getToken();
  if (t) h['Authorization'] = `Bearer ${t}`;
  return h;
}

// ─── Preset Options ─────────────────────────────────────────────

const SECTOR_OPTIONS = [
  "Technology", "Energy", "Defense/Aerospace", "Healthcare", "Financials",
  "Materials", "Consumer", "Industrial", "Real Estate", "Utilities", "Communications",
];

const MARKET_CAP_OPTIONS = ["", "Micro (<$300M)", "Small ($300M-$2B)", "Mid ($2B-$10B)", "Large ($10B+)"];
const RISK_STANCE_OPTIONS = ["", "Conservative", "Moderate", "Aggressive", "Very Aggressive"];
const CONVICTION_OPTIONS = ["", "Show All", "Medium+", "High Only"];
const ANALYSIS_DEPTH_OPTIONS = ["", "Quick Summary", "Standard", "Deep Dive"];

const CAPITAL_RANGE_OPTIONS = ["", "<$10K", "$10K-$50K", "$50K-$200K", "$200K-$500K", "$500K-$1M", "$1M+"];
const RISK_TOLERANCE_OPTIONS = ["", "Conservative", "Moderate", "Aggressive"];
const MAX_POSITIONS_OPTIONS = ["", "5", "10", "15", "20", "30", "50"];
const POSITION_SIZING_OPTIONS = ["", "1-2%", "2-5%", "5-10%", "10-20%", "Flexible"];
const HOLDING_PERIOD_OPTIONS = ["", "Day Trade", "Swing (1-5 days)", "Position (1-4 weeks)", "Long-term (1+ months)", "Mixed"];
const STRATEGY_OPTIONS = [
  "Momentum", "Value", "Growth", "Contrarian", "Mean Reversion",
  "Breakout", "Earnings Play", "Sector Rotation",
];

// ─── Types ──────────────────────────────────────────────────────

interface Template {
  name: string;
  content: string;
  created_at: string;
  updated_at: string;
}

interface InstructionPresets {
  focus_sectors: string[];
  market_cap: string;
  risk_stance: string;
  conviction_minimum: string;
  analysis_depth: string;
}

interface ProfilePresets {
  capital_range: string;
  risk_tolerance: string;
  max_positions: string;
  position_sizing: string;
  holding_period: string;
  strategy_types: string[];
  preferred_sectors: string[];
}

interface SettingsData {
  standing_instructions: string;
  personal_profile: string;
  instruction_presets: InstructionPresets;
  profile_presets: ProfilePresets;
  instruction_templates: Template[];
  profile_templates: Template[];
  active_instruction_template: string | null;
  active_profile_template: string | null;
  default_personal_profile: string;
  core_quant_dna: string;
}

const EMPTY_INSTR_PRESETS: InstructionPresets = {
  focus_sectors: [], market_cap: "", risk_stance: "", conviction_minimum: "", analysis_depth: "",
};

const EMPTY_PROFILE_PRESETS: ProfilePresets = {
  capital_range: "", risk_tolerance: "", max_positions: "", position_sizing: "",
  holding_period: "", strategy_types: [], preferred_sectors: [],
};

// ─── API Helpers ────────────────────────────────────────────────

async function fetchSettings(): Promise<SettingsData | null> {
  try {
    const res = await fetch(`${AGENT_BACKEND_URL}/api/settings`, { headers: authHeaders() });
    if (res.ok) return res.json();
  } catch {}
  return null;
}

async function updateSettings(data: Record<string, unknown>): Promise<SettingsData | null> {
  try {
    const res = await fetch(`${AGENT_BACKEND_URL}/api/settings`, {
      method: "PUT",
      headers: authHeaders(),
      body: JSON.stringify(data),
    });
    if (res.ok) return res.json();
  } catch {}
  return null;
}

async function saveTemplateApi(type: string, name: string, content: string): Promise<SettingsData | null> {
  try {
    const res = await fetch(`${AGENT_BACKEND_URL}/api/settings/templates`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify({ type, name, content }),
    });
    if (res.ok) return res.json();
  } catch {}
  return null;
}

async function deleteTemplateApi(type: string, name: string): Promise<SettingsData | null> {
  try {
    const res = await fetch(
      `${AGENT_BACKEND_URL}/api/settings/templates?template_type=${encodeURIComponent(type)}&name=${encodeURIComponent(name)}`,
      { method: "DELETE", headers: authHeaders({ 'Content-Type': '' }) },
    );
    if (res.ok) return res.json();
  } catch {}
  return null;
}

// ─── Reusable UI Components ────────────────────────────────────

function FieldSelect({ label, value, options, onChange }: {
  label: string; value: string; options: string[]; onChange: (v: string) => void;
}) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-[9px] text-white/30 uppercase tracking-wider font-semibold">{label}</label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="bg-white/[0.04] border border-white/[0.1] rounded-lg px-2.5 py-1.5 text-[11px] text-white appearance-none cursor-pointer focus:outline-none focus:border-white/20 transition-all"
        style={{ backgroundImage: "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6' viewBox='0 0 10 6'%3E%3Cpath d='M1 1l4 4 4-4' stroke='rgba(255,255,255,0.3)' fill='none' stroke-width='1.5'/%3E%3C/svg%3E\")", backgroundRepeat: "no-repeat", backgroundPosition: "right 8px center" }}
      >
        {options.map((o) => (
          <option key={o} value={o} className="bg-[#1a1a1f] text-white">
            {o || "— Not set —"}
          </option>
        ))}
      </select>
    </div>
  );
}

function ChipMultiSelect({ label, options, selected, onChange }: {
  label: string; options: string[]; selected: string[]; onChange: (v: string[]) => void;
}) {
  const toggle = (opt: string) => {
    onChange(selected.includes(opt) ? selected.filter((s) => s !== opt) : [...selected, opt]);
  };
  return (
    <div className="flex flex-col gap-1.5">
      <label className="text-[9px] text-white/30 uppercase tracking-wider font-semibold">{label}</label>
      <div className="flex flex-wrap gap-1.5">
        {options.map((o) => {
          const active = selected.includes(o);
          return (
            <button
              key={o}
              onClick={() => toggle(o)}
              className={`text-[10px] px-2.5 py-1 rounded-md border transition-all ${
                active
                  ? "bg-blue-500/15 border-blue-500/30 text-blue-400 font-semibold"
                  : "bg-white/[0.03] border-white/[0.08] text-white/40 hover:bg-white/[0.06] hover:text-white/60"
              }`}
            >
              {active && <Check className="w-2.5 h-2.5 inline mr-1" />}
              {o}
            </button>
          );
        })}
      </div>
    </div>
  );
}

function TemplateBar({ templates, activeTemplate, onLoad, onDelete, onSave, label }: {
  templates: Template[]; activeTemplate: string | null;
  onLoad: (t: Template) => void; onDelete: (name: string) => void; onSave: () => void; label: string;
}) {
  return (
    <div className="flex items-center gap-2 flex-wrap mb-3">
      <span className="text-[9px] text-white/25 uppercase tracking-wider font-semibold">{label}:</span>
      {templates.map((t) => (
        <div key={t.name} className="flex items-center gap-0">
          <button
            onClick={() => onLoad(t)}
            className={`text-[10px] px-2.5 py-1 rounded-l-md border transition-all ${
              activeTemplate === t.name
                ? "bg-blue-500/15 border-blue-500/30 text-blue-400 font-semibold"
                : "bg-white/[0.03] border-white/[0.08] text-white/50 hover:bg-white/[0.06] hover:text-white/70"
            }`}
          >
            {activeTemplate === t.name && <Check className="w-2.5 h-2.5 inline mr-1" />}
            {t.name}
          </button>
          <button
            onClick={() => onDelete(t.name)}
            className="text-[10px] px-1.5 py-1 rounded-r-md border border-l-0 border-white/[0.08] bg-white/[0.02] text-white/20 hover:text-red-400 hover:bg-red-500/10 transition-all"
          >
            <X className="w-2.5 h-2.5" />
          </button>
        </div>
      ))}
      <button
        onClick={onSave}
        className="text-[10px] px-2 py-1 rounded-md border border-dashed border-white/[0.1] text-white/30 hover:text-white/50 hover:border-white/20 transition-all flex items-center gap-1"
      >
        <Plus className="w-2.5 h-2.5" /> Save Template
      </button>
    </div>
  );
}

function SaveTemplateModal({ onSave, onClose }: { onSave: (name: string) => void; onClose: () => void }) {
  const [name, setName] = useState("");
  return (
    <div className="fixed inset-0 z-[70] flex items-center justify-center p-4" onClick={onClose}>
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />
      <div className="relative bg-[#060709] border border-white/10 rounded-xl p-5 w-full max-w-sm" onClick={(e) => e.stopPropagation()}>
        <h3 className="text-sm font-bold text-white mb-3">Save as Template</h3>
        <input
          type="text" value={name} onChange={(e) => setName(e.target.value)}
          placeholder="Template name (e.g. Small Cap Aggressive)"
          maxLength={60} autoFocus
          className="w-full bg-white/[0.04] border border-white/[0.1] rounded-lg px-3 py-2 text-xs text-white placeholder-white/25 focus:outline-none focus:border-white/20 mb-3"
          onKeyDown={(e) => { if (e.key === "Enter" && name.trim()) onSave(name.trim()); }}
        />
        <div className="flex justify-end gap-2">
          <button onClick={onClose} className="text-xs text-white/40 px-3 py-1.5 hover:text-white/60 transition-colors">Cancel</button>
          <Button
            onClick={() => name.trim() && onSave(name.trim())}
            disabled={!name.trim()}
            className="text-xs text-white px-3 py-1.5 rounded-lg disabled:opacity-30"
            style={{ background: "linear-gradient(135deg, #2090d0, #5cc8f0)" }}
          >Save</Button>
        </div>
      </div>
    </div>
  );
}

// ─── Settings Modal ─────────────────────────────────────────────

export function SettingsModal({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
  const [settings, setSettings] = useState<SettingsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [toast, setToast] = useState<{ type: "success" | "error"; msg: string } | null>(null);
  const [showDNA, setShowDNA] = useState(false);

  // Form state
  const [instructions, setInstructions] = useState("");
  const [profile, setProfile] = useState("");
  const [instrPresets, setInstrPresets] = useState<InstructionPresets>({ ...EMPTY_INSTR_PRESETS });
  const [profPresets, setProfPresets] = useState<ProfilePresets>({ ...EMPTY_PROFILE_PRESETS });
  const [activeInstrTemplate, setActiveInstrTemplate] = useState<string | null>(null);
  const [activeProfileTemplate, setActiveProfileTemplate] = useState<string | null>(null);

  // Template save modals
  const [saveInstrModal, setSaveInstrModal] = useState(false);
  const [saveProfileModal, setSaveProfileModal] = useState(false);

  const showToast = useCallback((type: "success" | "error", msg: string) => {
    setToast({ type, msg });
    setTimeout(() => setToast(null), 3000);
  }, []);

  // Load settings when modal opens
  useEffect(() => {
    if (!isOpen) return;
    setLoading(true);
    fetchSettings().then((data) => {
      if (data) {
        setSettings(data);
        setInstructions(data.standing_instructions || "");
        setProfile(data.personal_profile || "");
        setInstrPresets({ ...EMPTY_INSTR_PRESETS, ...data.instruction_presets });
        setProfPresets({ ...EMPTY_PROFILE_PRESETS, ...data.profile_presets });
        setActiveInstrTemplate(data.active_instruction_template);
        setActiveProfileTemplate(data.active_profile_template);
      }
      setLoading(false);
    });
  }, [isOpen]);

  // Lock body scroll when open
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => { document.body.style.overflow = ""; };
  }, [isOpen]);

  const handleSave = async () => {
    setSaving(true);
    const result = await updateSettings({
      standing_instructions: instructions,
      personal_profile: profile,
      instruction_presets: instrPresets,
      profile_presets: profPresets,
      active_instruction_template: activeInstrTemplate,
      active_profile_template: activeProfileTemplate,
    });
    if (result) {
      setSettings((prev) => prev ? { ...prev, ...result } : prev);
      showToast("success", "Settings saved — takes effect on next query");
    } else {
      showToast("error", "Failed to save settings");
    }
    setSaving(false);
  };

  const handleSaveInstrTemplate = async (name: string) => {
    const result = await saveTemplateApi("instruction", name, instructions);
    if (result) {
      setSettings((prev) => prev ? { ...prev, instruction_templates: result.instruction_templates } : prev);
      setActiveInstrTemplate(name);
      showToast("success", `Template "${name}" saved`);
    }
    setSaveInstrModal(false);
  };

  const handleSaveProfileTemplate = async (name: string) => {
    const result = await saveTemplateApi("profile", name, profile);
    if (result) {
      setSettings((prev) => prev ? { ...prev, profile_templates: result.profile_templates } : prev);
      setActiveProfileTemplate(name);
      showToast("success", `Template "${name}" saved`);
    }
    setSaveProfileModal(false);
  };

  const handleDeleteInstrTemplate = async (name: string) => {
    const result = await deleteTemplateApi("instruction", name);
    if (result) {
      setSettings((prev) => prev ? { ...prev, instruction_templates: result.instruction_templates } : prev);
      if (activeInstrTemplate === name) setActiveInstrTemplate(null);
      showToast("success", `Template "${name}" deleted`);
    }
  };

  const handleDeleteProfileTemplate = async (name: string) => {
    const result = await deleteTemplateApi("profile", name);
    if (result) {
      setSettings((prev) => prev ? { ...prev, profile_templates: result.profile_templates } : prev);
      if (activeProfileTemplate === name) setActiveProfileTemplate(null);
      showToast("success", `Template "${name}" deleted`);
    }
  };

  const handleLoadInstrTemplate = (t: Template) => {
    setInstructions(t.content);
    setActiveInstrTemplate(t.name);
  };

  const handleLoadProfileTemplate = (t: Template) => {
    setProfile(t.content);
    setActiveProfileTemplate(t.name);
  };

  const handleClearInstructions = () => {
    setInstructions("");
    setInstrPresets({ ...EMPTY_INSTR_PRESETS });
    setActiveInstrTemplate(null);
  };

  const handleResetProfile = () => {
    setProfile("");
    setProfPresets({ ...EMPTY_PROFILE_PRESETS });
    setActiveProfileTemplate(null);
  };

  const handleLoadDefaultProfile = () => {
    if (settings?.default_personal_profile) {
      setProfile(settings.default_personal_profile);
      setActiveProfileTemplate(null);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center p-4" onClick={onClose}>
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/70 backdrop-blur-md" />

      {/* Modal */}
      <div
        className="relative bg-[#060709] border border-white/[0.08] rounded-2xl w-full max-w-[900px] max-h-[90vh] flex flex-col shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-white/[0.06] flex-shrink-0">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 bg-gradient-to-br from-[#2090d0] to-[#5cc8f0] rounded-xl flex items-center justify-center">
              <Settings className="w-4.5 h-4.5 text-white" />
            </div>
            <div>
              <h1 className="text-base font-bold text-white">Settings</h1>
              <p className="text-[10px] text-white/35">Customize Caelyn with structured presets and/or free-form instructions</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="w-8 h-8 rounded-lg bg-white/[0.04] border border-white/[0.08] flex items-center justify-center text-white/40 hover:text-white hover:bg-white/[0.08] transition-all"
          >
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Toast */}
        {toast && (
          <div className={`mx-6 mt-3 px-4 py-2 rounded-lg text-xs font-semibold border ${
            toast.type === "success"
              ? "bg-emerald-500/10 border-emerald-500/20 text-emerald-400"
              : "bg-red-500/10 border-red-500/20 text-red-400"
          }`}>
            {toast.msg}
          </div>
        )}

        {/* Scrollable body */}
        <div className="flex-1 overflow-y-auto px-6 py-5 space-y-5 scrollbar-thin scrollbar-thumb-white/10 scrollbar-track-transparent">
          {loading ? (
            <div className="flex items-center justify-center py-20">
              <Loader2 className="w-6 h-6 animate-spin text-blue-400" />
            </div>
          ) : (
            <>
              {/* ═══ Standing Instructions ═══ */}
              <GlassCard className="p-5">
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h2 className="text-sm font-bold text-white">Standing Instructions</h2>
                    <p className="text-[10px] text-white/30 mt-0.5">
                      Persistent directives applied to every Caelyn query. Use presets, free-form, or both.
                    </p>
                  </div>
                  <button
                    onClick={handleClearInstructions}
                    className="text-[10px] px-2.5 py-1.5 rounded-lg border border-white/[0.08] text-white/30 hover:text-white/60 hover:border-white/15 transition-all flex items-center gap-1"
                  >
                    <RotateCcw className="w-3 h-3" /> Clear All
                  </button>
                </div>

                {/* Preset fields */}
                <div className="mb-4 p-4 bg-white/[0.02] border border-white/[0.05] rounded-xl space-y-3.5">
                  <p className="text-[9px] text-white/25 uppercase tracking-wider font-semibold flex items-center gap-1.5">
                    Structured Presets
                    <span className="text-white/15 font-normal normal-case tracking-normal">— quick-select common preferences</span>
                  </p>

                  <ChipMultiSelect
                    label="Focus Sectors"
                    options={SECTOR_OPTIONS}
                    selected={instrPresets.focus_sectors}
                    onChange={(v) => setInstrPresets((p) => ({ ...p, focus_sectors: v }))}
                  />

                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                    <FieldSelect label="Market Cap" value={instrPresets.market_cap} options={MARKET_CAP_OPTIONS}
                      onChange={(v) => setInstrPresets((p) => ({ ...p, market_cap: v }))} />
                    <FieldSelect label="Risk Stance" value={instrPresets.risk_stance} options={RISK_STANCE_OPTIONS}
                      onChange={(v) => setInstrPresets((p) => ({ ...p, risk_stance: v }))} />
                    <FieldSelect label="Conviction Minimum" value={instrPresets.conviction_minimum} options={CONVICTION_OPTIONS}
                      onChange={(v) => setInstrPresets((p) => ({ ...p, conviction_minimum: v }))} />
                    <FieldSelect label="Analysis Depth" value={instrPresets.analysis_depth} options={ANALYSIS_DEPTH_OPTIONS}
                      onChange={(v) => setInstrPresets((p) => ({ ...p, analysis_depth: v }))} />
                  </div>
                </div>

                {/* Free-form */}
                <div className="mb-2">
                  <p className="text-[9px] text-white/25 uppercase tracking-wider font-semibold mb-2 flex items-center gap-1.5">
                    Free-form Instructions
                    <span className="text-white/15 font-normal normal-case tracking-normal">— anything presets don't cover</span>
                  </p>

                  <TemplateBar
                    templates={settings?.instruction_templates || []}
                    activeTemplate={activeInstrTemplate}
                    onLoad={handleLoadInstrTemplate}
                    onDelete={handleDeleteInstrTemplate}
                    onSave={() => setSaveInstrModal(true)}
                    label="Templates"
                  />

                  <textarea
                    value={instructions}
                    onChange={(e) => { setInstructions(e.target.value); setActiveInstrTemplate(null); }}
                    placeholder={"e.g. \"Only show me stocks under $500M market cap. I'm looking for pre-earnings accumulation plays in energy and defense. Be more aggressive with conviction ratings.\""}
                    rows={3}
                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-lg px-3 py-2.5 text-xs text-white placeholder-white/20 resize-y focus:outline-none focus:border-white/20 focus:ring-1 focus:ring-white/10 transition-all"
                  />
                  <div className="flex justify-between mt-1">
                    <span className="text-[9px] text-white/20">{instructions.length}/5000</span>
                    {activeInstrTemplate && <span className="text-[9px] text-blue-400/60">Active: {activeInstrTemplate}</span>}
                  </div>
                </div>
              </GlassCard>

              {/* ═══ Investment Profile ═══ */}
              <GlassCard className="p-5">
                <div className="flex items-start justify-between mb-4">
                  <div>
                    <h2 className="text-sm font-bold text-white">Investment Profile</h2>
                    <p className="text-[10px] text-white/30 mt-0.5">
                      Personal calibration layered on Core Quant DNA. Use presets, free-form, or both.
                    </p>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <button
                      onClick={handleLoadDefaultProfile}
                      className="text-[10px] px-2.5 py-1.5 rounded-lg border border-white/[0.08] text-white/30 hover:text-white/60 hover:border-white/15 transition-all"
                    >
                      Load Default
                    </button>
                    <button
                      onClick={handleResetProfile}
                      className="text-[10px] px-2.5 py-1.5 rounded-lg border border-white/[0.08] text-white/30 hover:text-white/60 hover:border-white/15 transition-all flex items-center gap-1"
                    >
                      <RotateCcw className="w-3 h-3" /> Reset All
                    </button>
                  </div>
                </div>

                {/* Preset fields */}
                <div className="mb-4 p-4 bg-white/[0.02] border border-white/[0.05] rounded-xl space-y-3.5">
                  <p className="text-[9px] text-white/25 uppercase tracking-wider font-semibold flex items-center gap-1.5">
                    Structured Presets
                    <span className="text-white/15 font-normal normal-case tracking-normal">— define your portfolio parameters</span>
                  </p>

                  <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                    <FieldSelect label="Capital Range" value={profPresets.capital_range} options={CAPITAL_RANGE_OPTIONS}
                      onChange={(v) => setProfPresets((p) => ({ ...p, capital_range: v }))} />
                    <FieldSelect label="Risk Tolerance" value={profPresets.risk_tolerance} options={RISK_TOLERANCE_OPTIONS}
                      onChange={(v) => setProfPresets((p) => ({ ...p, risk_tolerance: v }))} />
                    <FieldSelect label="Max Positions" value={profPresets.max_positions} options={MAX_POSITIONS_OPTIONS}
                      onChange={(v) => setProfPresets((p) => ({ ...p, max_positions: v }))} />
                    <FieldSelect label="Position Sizing" value={profPresets.position_sizing} options={POSITION_SIZING_OPTIONS}
                      onChange={(v) => setProfPresets((p) => ({ ...p, position_sizing: v }))} />
                    <FieldSelect label="Holding Period" value={profPresets.holding_period} options={HOLDING_PERIOD_OPTIONS}
                      onChange={(v) => setProfPresets((p) => ({ ...p, holding_period: v }))} />
                  </div>

                  <ChipMultiSelect
                    label="Strategy Types"
                    options={STRATEGY_OPTIONS}
                    selected={profPresets.strategy_types}
                    onChange={(v) => setProfPresets((p) => ({ ...p, strategy_types: v }))}
                  />

                  <ChipMultiSelect
                    label="Preferred Sectors"
                    options={SECTOR_OPTIONS}
                    selected={profPresets.preferred_sectors}
                    onChange={(v) => setProfPresets((p) => ({ ...p, preferred_sectors: v }))}
                  />
                </div>

                {/* Free-form */}
                <div className="mb-2">
                  <p className="text-[9px] text-white/25 uppercase tracking-wider font-semibold mb-2 flex items-center gap-1.5">
                    Free-form Profile
                    <span className="text-white/15 font-normal normal-case tracking-normal">— detailed personal context beyond presets</span>
                  </p>

                  <TemplateBar
                    templates={settings?.profile_templates || []}
                    activeTemplate={activeProfileTemplate}
                    onLoad={handleLoadProfileTemplate}
                    onDelete={handleDeleteProfileTemplate}
                    onSave={() => setSaveProfileModal(true)}
                    label="Templates"
                  />

                  <textarea
                    value={profile}
                    onChange={(e) => { setProfile(e.target.value); setActiveProfileTemplate(null); }}
                    placeholder="Detailed investment profile — capital range, risk tolerance, max positions, sectors, holding periods. Or click 'Load Default' to start from the built-in profile."
                    rows={8}
                    className="w-full bg-white/[0.03] border border-white/[0.08] rounded-lg px-3 py-2.5 text-xs text-white placeholder-white/20 resize-y focus:outline-none focus:border-white/20 focus:ring-1 focus:ring-white/10 transition-all font-mono leading-relaxed"
                  />
                  <div className="flex justify-between mt-1">
                    <span className="text-[9px] text-white/20">{profile.length}/10000</span>
                    <div className="flex items-center gap-3">
                      {!profile.trim() && profPresets.capital_range === "" && profPresets.strategy_types.length === 0 && (
                        <span className="text-[9px] text-yellow-400/50">No profile active — using Core Quant DNA only</span>
                      )}
                      {activeProfileTemplate && <span className="text-[9px] text-blue-400/60">Active: {activeProfileTemplate}</span>}
                    </div>
                  </div>
                </div>
              </GlassCard>

              {/* ═══ Core Quant DNA (read-only) ═══ */}
              <GlassCard className="p-5">
                <button onClick={() => setShowDNA(!showDNA)} className="flex items-center justify-between w-full text-left">
                  <div>
                    <h2 className="text-sm font-bold text-white/60">Core Quant DNA</h2>
                    <p className="text-[10px] text-white/25 mt-0.5">
                      Bottleneck Thesis &middot; EBITDA Turn &middot; Weinstein Stages &middot; Power Law &middot; Asymmetric Setups &middot; Sell Discipline
                    </p>
                  </div>
                  {showDNA ? <ChevronUp className="w-4 h-4 text-white/30" /> : <ChevronDown className="w-4 h-4 text-white/30" />}
                </button>
                {showDNA && (
                  <div className="mt-4 p-4 bg-white/[0.02] border border-white/[0.06] rounded-lg">
                    <p className="text-[9px] text-white/20 uppercase tracking-wider font-semibold mb-3">Read-only — these frameworks are always active</p>
                    <pre className="text-[10px] text-white/40 whitespace-pre-wrap font-mono leading-relaxed">
                      {settings?.core_quant_dna || "Loading..."}
                    </pre>
                  </div>
                )}
              </GlassCard>
            </>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between px-6 py-4 border-t border-white/[0.06] flex-shrink-0">
          <p className="text-[10px] text-white/20">Changes take effect on the next Caelyn query</p>
          <Button
            onClick={handleSave}
            disabled={saving || loading}
            className="text-white text-sm px-5 py-2.5 rounded-lg transition-all disabled:opacity-40 flex items-center gap-2"
            style={{ background: "linear-gradient(135deg, #2090d0, #5cc8f0)" }}
          >
            {saving ? <Loader2 className="w-4 h-4 animate-spin" /> : <Save className="w-4 h-4" />}
            Save Settings
          </Button>
        </div>
      </div>

      {/* Template save modals */}
      {saveInstrModal && <SaveTemplateModal onSave={handleSaveInstrTemplate} onClose={() => setSaveInstrModal(false)} />}
      {saveProfileModal && <SaveTemplateModal onSave={handleSaveProfileTemplate} onClose={() => setSaveProfileModal(false)} />}
    </div>
  );
}

// Keep default export for backward compatibility during transition
export default function SettingsPage() {
  return (
    <div className="min-h-screen text-white flex items-center justify-center" style={{ background: '#050608' }}>
      <p className="text-white/40 text-sm">Settings have moved — use the gear icon in the sidebar.</p>
    </div>
  );
}
