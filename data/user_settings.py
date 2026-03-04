import json
from pathlib import Path
from datetime import datetime

SETTINGS_FILE = Path("data/user_settings.json")

MAX_STANDING_INSTRUCTIONS = 5000
MAX_PERSONAL_PROFILE = 10000
MAX_TEMPLATE_NAME = 60
MAX_TEMPLATES_PER_TYPE = 20

DEFAULT_INSTRUCTION_PRESETS = {
    "focus_sectors": [],
    "market_cap": "",
    "risk_stance": "",
    "conviction_minimum": "",
    "analysis_depth": "",
}

DEFAULT_PROFILE_PRESETS = {
    "capital_range": "",
    "risk_tolerance": "",
    "max_positions": "",
    "position_sizing": "",
    "holding_period": "",
    "strategy_types": [],
    "preferred_sectors": [],
}


def _ensure_file():
    SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not SETTINGS_FILE.exists():
        default = {
            "standing_instructions": "",
            "personal_profile": "",
            "instruction_presets": DEFAULT_INSTRUCTION_PRESETS.copy(),
            "profile_presets": DEFAULT_PROFILE_PRESETS.copy(),
            "instruction_templates": [],
            "profile_templates": [],
            "active_instruction_template": None,
            "active_profile_template": None,
        }
        SETTINGS_FILE.write_text(json.dumps(default, indent=2))


def get_settings() -> dict:
    _ensure_file()
    try:
        data = json.loads(SETTINGS_FILE.read_text())
    except (json.JSONDecodeError, FileNotFoundError):
        data = {}
    return {
        "standing_instructions": data.get("standing_instructions", ""),
        "personal_profile": data.get("personal_profile", ""),
        "instruction_presets": {**DEFAULT_INSTRUCTION_PRESETS, **data.get("instruction_presets", {})},
        "profile_presets": {**DEFAULT_PROFILE_PRESETS, **data.get("profile_presets", {})},
        "instruction_templates": data.get("instruction_templates", []),
        "profile_templates": data.get("profile_templates", []),
        "active_instruction_template": data.get("active_instruction_template"),
        "active_profile_template": data.get("active_profile_template"),
    }


def save_settings(
    standing_instructions: str = None,
    personal_profile: str = None,
    instruction_presets: dict = None,
    profile_presets: dict = None,
    active_instruction_template: str = None,
    active_profile_template: str = None,
) -> dict:
    settings = get_settings()
    if standing_instructions is not None:
        settings["standing_instructions"] = standing_instructions[:MAX_STANDING_INSTRUCTIONS]
    if personal_profile is not None:
        settings["personal_profile"] = personal_profile[:MAX_PERSONAL_PROFILE]
    if instruction_presets is not None:
        settings["instruction_presets"] = {**DEFAULT_INSTRUCTION_PRESETS, **instruction_presets}
    if profile_presets is not None:
        settings["profile_presets"] = {**DEFAULT_PROFILE_PRESETS, **profile_presets}
    if active_instruction_template is not None:
        settings["active_instruction_template"] = active_instruction_template or None
    if active_profile_template is not None:
        settings["active_profile_template"] = active_profile_template or None
    _write(settings)
    return settings


def format_instruction_presets(presets: dict) -> str:
    """Format instruction presets into natural language for the system prompt."""
    parts = []
    if presets.get("focus_sectors"):
        parts.append(f"Focus sectors: {', '.join(presets['focus_sectors'])}")
    if presets.get("market_cap"):
        parts.append(f"Market cap preference: {presets['market_cap']}")
    if presets.get("risk_stance"):
        parts.append(f"Risk stance: {presets['risk_stance']}")
    if presets.get("conviction_minimum"):
        parts.append(f"Conviction minimum: {presets['conviction_minimum']}")
    if presets.get("analysis_depth"):
        parts.append(f"Analysis depth: {presets['analysis_depth']}")
    return "\n".join(parts)


def format_profile_presets(presets: dict) -> str:
    """Format profile presets into natural language for the system prompt."""
    parts = []
    if presets.get("capital_range"):
        parts.append(f"Portfolio capital: {presets['capital_range']}")
    if presets.get("risk_tolerance"):
        parts.append(f"Risk tolerance: {presets['risk_tolerance']}")
    if presets.get("max_positions"):
        parts.append(f"Max concurrent positions: {presets['max_positions']}")
    if presets.get("position_sizing"):
        parts.append(f"Default position sizing: {presets['position_sizing']}")
    if presets.get("holding_period"):
        parts.append(f"Preferred holding period: {presets['holding_period']}")
    if presets.get("strategy_types"):
        parts.append(f"Strategy types: {', '.join(presets['strategy_types'])}")
    if presets.get("preferred_sectors"):
        parts.append(f"Preferred sectors: {', '.join(presets['preferred_sectors'])}")
    return "\n".join(parts)


def save_template(template_type: str, name: str, content: str) -> dict:
    """Save a named template. template_type is 'instruction' or 'profile'."""
    settings = get_settings()
    key = f"{template_type}_templates"
    templates = settings.get(key, [])

    name = name.strip()[:MAX_TEMPLATE_NAME]
    if not name:
        raise ValueError("Template name is required")

    # Update existing or add new
    found = False
    for t in templates:
        if t["name"] == name:
            t["content"] = content
            t["updated_at"] = datetime.now().isoformat()
            found = True
            break
    if not found:
        if len(templates) >= MAX_TEMPLATES_PER_TYPE:
            raise ValueError(f"Maximum {MAX_TEMPLATES_PER_TYPE} templates allowed")
        templates.append({
            "name": name,
            "content": content,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
        })

    settings[key] = templates
    _write(settings)
    return settings


def delete_template(template_type: str, name: str) -> dict:
    settings = get_settings()
    key = f"{template_type}_templates"
    templates = settings.get(key, [])
    settings[key] = [t for t in templates if t["name"] != name]
    # Clear active if it was the deleted one
    active_key = f"active_{template_type}_template"
    if settings.get(active_key) == name:
        settings[active_key] = None
    _write(settings)
    return settings


def _write(settings: dict):
    _ensure_file()
    SETTINGS_FILE.write_text(json.dumps(settings, indent=2))
