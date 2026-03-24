import re
import datetime
from config import ALIASES

def to_display_name(canonical_name):
    return ALIASES.get(canonical_name, canonical_name)

def normalize_department_name(raw_name):
    cleaned = (raw_name or "").strip().lower()
    mapping = {
        "01": "Ain", "1": "Ain", "ain": "Ain", "03": "Allier", "3": "Allier", "allier": "Allier",
        "07": "Ardeche", "7": "Ardeche", "ardeche": "Ardeche", "ardèche": "Ardeche",
        "15": "Cantal", "cantal": "Cantal", "26": "Drome", "drome": "Drome", "drôme": "Drome",
        "43": "Haute-Loire", "haute-loire": "Haute-Loire", "haute loire": "Haute-Loire",
        "74": "Haute-Savoie", "haute-savoie": "Haute-Savoie", "haute savoie": "Haute-Savoie",
        "38": "Isere", "isere": "Isere", "isère": "Isere", "42": "Loire", "loire": "Loire",
        "63": "Puy-de-Dome", "puy-de-dome": "Puy-de-Dome", "puy de dome": "Puy-de-Dome", "puy-de-dôme": "Puy-de-Dome",
        "69": "Rhone", "rhone": "Rhone", "rhône": "Rhone", "73": "Savoie", "savoie": "Savoie",
    }
    return mapping.get(cleaned)

def read_ci(row_dict, key):
    key_lower = key.lower()
    for existing_key, value in row_dict.items():
        if existing_key.lower() == key_lower:
            return value
    return None

def to_int(value, default=0):
    if value is None: return default
    return int(round(float(value)))

def parse_float(value):
    if value is None: return None
    text = str(value).strip().replace(",", ".")
    if not text: return None
    try: return float(text)
    except ValueError: return None

def read_first_non_empty(data, candidates):
    for key in candidates:
        value = read_ci(data, key)
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
        return value
    return None

def parse_weeks_param(raw_value):
    if not raw_value: return []
    weeks = []
    for token in raw_value.split(","):
        candidate = token.strip().upper()
        if candidate and re.match(r"^S\d{1,2}$", candidate):
            weeks.append(candidate)
    return list(dict.fromkeys(weeks)) # Preserve order, remove duplicates

def parse_week_number(week_value):
    text = str(week_value or "").upper().strip()
    if text.startswith("S") and text[1:].isdigit(): return int(text[1:])
    if text.isdigit(): return int(text)
    return None

def week_label(week_value):
    number = parse_week_number(week_value)
    return f"S{number}" if number is not None else str(week_value or "").upper().strip()

def season_week_sort(week_value):
    number = parse_week_number(week_value)
    if number is None: return 999
    if number == 51: return 0
    if number == 52: return 1
    if 1 <= number <= 15: return number + 1
    return number + 200

def week_sort_key(week_value):
    number = parse_week_number(week_value)
    return number if number is not None else 999

def week_label_from_date(value):
    if value is None: return None
    if isinstance(value, (datetime.datetime, datetime.date)):
        return f"S{value.isocalendar().week}"
    text = str(value).strip()
    if not text: return None
    try:
        date_value = datetime.datetime.fromisoformat(text.replace("Z", "")).date()
        return f"S{date_value.isocalendar().week}"
    except ValueError:
        return None

def normalize_activities_value(value):
    if not value: return []
    if isinstance(value, list):
        raw_items = value
    else:
        raw_text = str(value).strip()
        if not raw_text: return []
        if raw_text.startswith("[") and raw_text.endswith("]"):
            content = raw_text[1:-1]
            raw_items = [part.strip().strip('"').strip("'") for part in content.split(",")]
        else:
            raw_items = [part.strip() for part in raw_text.split(",")]

    cleaned, seen = [], set()
    for item in raw_items:
        text = str(item or "").strip()
        if text and text.lower() not in seen:
            seen.add(text.lower())
            cleaned.append(text)
    return cleaned

def extract_activities_from_array(array_value):
    return normalize_activities_value(array_value)