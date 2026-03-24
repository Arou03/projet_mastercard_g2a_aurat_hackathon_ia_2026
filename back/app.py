from flask import Flask, jsonify, request
from flask_cors import CORS
import base64
import datetime
import os
import re
import time

import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

app = Flask(__name__)
CORS(app)  # Autorise les requêtes du front

# Donnees de demo pour hackathon.
# Ces valeurs pourront ensuite venir de Snowflake.
DEPARTMENT_KPIS = {
    "Ain": {"total_aura": 72, "rural": 22, "urbain": 30, "stations_montagne": 10, "villages_montagne": 10, "frequentation": 72},
    "Allier": {"total_aura": 54, "rural": 16, "urbain": 28, "stations_montagne": 5, "villages_montagne": 5, "frequentation": 54},
    "Ardeche": {"total_aura": 66, "rural": 24, "urbain": 25, "stations_montagne": 8, "villages_montagne": 9, "frequentation": 66},
    "Cantal": {"total_aura": 58, "rural": 26, "urbain": 18, "stations_montagne": 6, "villages_montagne": 8, "frequentation": 58},
    "Drome": {"total_aura": 61, "rural": 20, "urbain": 27, "stations_montagne": 6, "villages_montagne": 8, "frequentation": 61},
    "Haute-Loire": {"total_aura": 57, "rural": 23, "urbain": 20, "stations_montagne": 6, "villages_montagne": 8, "frequentation": 57},
    "Haute-Savoie": {"total_aura": 95, "rural": 20, "urbain": 30, "stations_montagne": 27, "villages_montagne": 18, "frequentation": 95},
    "Isere": {"total_aura": 88, "rural": 18, "urbain": 36, "stations_montagne": 18, "villages_montagne": 16, "frequentation": 88},
    "Loire": {"total_aura": 62, "rural": 18, "urbain": 32, "stations_montagne": 6, "villages_montagne": 6, "frequentation": 62},
    "Puy-de-Dome": {"total_aura": 69, "rural": 21, "urbain": 30, "stations_montagne": 9, "villages_montagne": 9, "frequentation": 69},
    "Rhone": {"total_aura": 91, "rural": 10, "urbain": 62, "stations_montagne": 10, "villages_montagne": 9, "frequentation": 91},
    "Savoie": {"total_aura": 92, "rural": 15, "urbain": 25, "stations_montagne": 30, "villages_montagne": 22, "frequentation": 92},
}

ALIASES = {
    "Ardeche": "Ardèche",
    "Drome": "Drôme",
    "Isere": "Isère",
    "Puy-de-Dome": "Puy-de-Dôme",
    "Rhone": "Rhône",
}

DEPARTMENT_TO_CODE = {
    "Ain": "01",
    "Allier": "03",
    "Ardeche": "07",
    "Cantal": "15",
    "Drome": "26",
    "Haute-Loire": "43",
    "Haute-Savoie": "74",
    "Isere": "38",
    "Loire": "42",
    "Puy-de-Dome": "63",
    "Rhone": "69",
    "Savoie": "73",
}

VALID_KPIS = {"total_aura", "rural", "urbain", "stations_montagne", "villages_montagne"}
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "300"))
SNOWFLAKE_LOGIN_TIMEOUT_SECONDS = int(os.getenv("SNOWFLAKE_LOGIN_TIMEOUT_SECONDS", "10"))
SNOWFLAKE_NETWORK_TIMEOUT_SECONDS = int(os.getenv("SNOWFLAKE_NETWORK_TIMEOUT_SECONDS", "20"))
SNOWFLAKE_SOCKET_TIMEOUT_SECONDS = int(os.getenv("SNOWFLAKE_SOCKET_TIMEOUT_SECONDS", "20"))
SNOWFLAKE_QUERY_TIMEOUT_SECONDS = int(os.getenv("SNOWFLAKE_QUERY_TIMEOUT_SECONDS", "20"))

# Schémas globaux (recommandés)
# - SNOWFLAKE_SCHEMA_PUBLIC: schéma par défaut (PUBLIC)
# - SNOWFLAKE_SCHEMA_FACT: schéma des tables de faits (ex: CLEANED)
# - SNOWFLAKE_SCHEMA_DIM: schéma des dimensions (ex: DIMENSION)
# - SNOWFLAKE_SCHEMA_REF: schéma des référentiels (ex: REF)
# Compat rétroactive conservée avec SNOWFLAKE_FACT_SCHEMA / SNOWFLAKE_DIM_SCHEMA.
SNOWFLAKE_SCHEMA_PUBLIC = os.getenv("SNOWFLAKE_SCHEMA_PUBLIC", os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")).strip() or "PUBLIC"
SNOWFLAKE_SCHEMA_FACT = os.getenv("SNOWFLAKE_SCHEMA_FACT", os.getenv("SNOWFLAKE_FACT_SCHEMA", "CLEANED")).strip() or "CLEANED"
SNOWFLAKE_SCHEMA_DIM = os.getenv("SNOWFLAKE_SCHEMA_DIM", os.getenv("SNOWFLAKE_DIM_SCHEMA", "DIMENSION")).strip() or "DIMENSION"
SNOWFLAKE_SCHEMA_REF = os.getenv("SNOWFLAKE_SCHEMA_REF", "REF").strip() or "REF"

# Alias utilises dans le code existant
SNOWFLAKE_FACT_SCHEMA = SNOWFLAKE_SCHEMA_FACT
SNOWFLAKE_DIM_SCHEMA = SNOWFLAKE_SCHEMA_DIM

# Cache en memoire process-local.
# Sur Render, ce cache vit par instance et se reconstruit au redemarrage/scale.
api_cache = {}
last_snowflake_error = None


def quote_ident(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'


def fq_table(schema_name, table_name):
    database_name = (os.getenv("SNOWFLAKE_DATABASE") or "").strip()
    schema_part = quote_ident(schema_name)
    table_part = quote_ident(table_name)
    if database_name:
        return f"{quote_ident(database_name)}.{schema_part}.{table_part}"
    return f"{schema_part}.{table_part}"


def to_display_name(canonical_name):
    return ALIASES.get(canonical_name, canonical_name)


def normalize_department_name(raw_name):
    cleaned = (raw_name or "").strip().lower()

    mapping = {
        "01": "Ain",
        "1": "Ain",
        "ain": "Ain",
        "03": "Allier",
        "3": "Allier",
        "allier": "Allier",
        "07": "Ardeche",
        "7": "Ardeche",
        "ardeche": "Ardeche",
        "ardèche": "Ardeche",
        "15": "Cantal",
        "cantal": "Cantal",
        "26": "Drome",
        "drome": "Drome",
        "drôme": "Drome",
        "43": "Haute-Loire",
        "haute-loire": "Haute-Loire",
        "haute loire": "Haute-Loire",
        "74": "Haute-Savoie",
        "haute-savoie": "Haute-Savoie",
        "haute savoie": "Haute-Savoie",
        "38": "Isere",
        "isere": "Isere",
        "isère": "Isere",
        "42": "Loire",
        "loire": "Loire",
        "63": "Puy-de-Dome",
        "puy-de-dome": "Puy-de-Dome",
        "puy de dome": "Puy-de-Dome",
        "puy-de-dôme": "Puy-de-Dome",
        "69": "Rhone",
        "rhone": "Rhone",
        "rhône": "Rhone",
        "73": "Savoie",
        "savoie": "Savoie",
    }
    return mapping.get(cleaned)


def is_snowflake_configured():
    return len(get_missing_required_env_vars()) == 0


def get_required_env_vars():
    # Les schemas ont des valeurs par défaut via SNOWFLAKE_SCHEMA_PUBLIC/FACT/DIM/REF,
    # donc ils ne sont plus bloquants pour la connexion.
    return [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_PRIVATE_KEY_B64",
    ]


def get_missing_required_env_vars():
    return [name for name in get_required_env_vars() if not os.getenv(name)]


def load_private_key_der_bytes():
    key_b64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_B64", "").strip()
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip()

    if not key_b64:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_B64 is missing")

    private_key_pem = base64.b64decode(key_b64)
    private_key = serialization.load_pem_private_key(
        private_key_pem,
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def read_ci(row_dict, key):
    key_lower = key.lower()
    for existing_key, value in row_dict.items():
        if existing_key.lower() == key_lower:
            return value
    return None


def to_int(value, default=0):
    if value is None:
        return default
    return int(round(float(value)))


def parse_weeks_param(raw_value):
    if not raw_value:
        return []

    weeks = []
    for token in raw_value.split(","):
        candidate = token.strip().upper()
        if not candidate:
            continue
        if re.match(r"^S\d{1,2}$", candidate):
            weeks.append(candidate)

    # Preserve order and remove duplicates.
    unique = []
    seen = set()
    for week in weeks:
        if week in seen:
            continue
        seen.add(week)
        unique.append(week)
    return unique


def parse_activities_param(raw_value):
    if not raw_value:
        return []

    items = []
    seen = set()
    for token in raw_value.split(","):
        activity = token.strip()
        if not activity:
            continue
        normalized = activity.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        items.append(activity)
    return items


def parse_week_number(week_value):
    text = str(week_value or "").upper().strip()
    if text.startswith("S") and text[1:].isdigit():
        return int(text[1:])
    if text.isdigit():
        return int(text)
    return None


def week_label(week_value):
    number = parse_week_number(week_value)
    if number is None:
        return str(week_value or "").upper().strip()
    return f"S{number}"


def season_week_sort(week_value):
    number = parse_week_number(week_value)
    if number is None:
        return 999
    if number == 51:
        return 0
    if number == 52:
        return 1
    if 1 <= number <= 15:
        return number + 1
    return number + 200


def week_label_from_date(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        iso_week = value.isocalendar().week
        return f"S{iso_week}"
    if isinstance(value, datetime.date):
        iso_week = value.isocalendar().week
        return f"S{iso_week}"

    text = str(value).strip()
    if not text:
        return None
    try:
        date_value = datetime.datetime.fromisoformat(text.replace("Z", "")).date()
        return f"S{date_value.isocalendar().week}"
    except ValueError:
        return None


def is_winter_week(week_label):
    """Check if a week is in winter period (S51, S52, S1-S15)."""
    num = parse_week_number(week_label)
    if num is None:
        return False
    return num == 51 or num == 52 or (1 <= num <= 15)


def filter_winter_holidays(holidays):
    """Filter holidays to only include those in winter weeks."""
    winter_holidays = []
    for h in holidays:
        week_start = h.get("week_start")
        week_end = h.get("week_end")
        # Include holiday if it spans winter weeks (check if start or end is in winter)
        if is_winter_week(week_start) or is_winter_week(week_end):
            winter_holidays.append(h)
    return winter_holidays


def build_mock_department_timeline(canonical_name, year=2024):
    """Build mock timeline data for a department and year.
    For 2024: use base KPIS. For 2025+: apply growth factor (5% per year).
    """
    base = DEPARTMENT_KPIS.get(canonical_name, DEPARTMENT_KPIS["Savoie"])
    weeks = ["S51", "S52"] + [f"S{i}" for i in range(1, 16)]
    
    # Growth factor for future years (5% annual growth)
    year = max(2024, int(year))
    growth_factor = 1.0 + (0.05 * (year - 2024))

    observed = []
    rural = []
    urbain = []
    stations = []
    villages = []
    for index, _ in enumerate(weeks):
        variation = 1 + (0.08 * ((index % 5) - 2))
        observed.append(to_int(base["total_aura"] * variation * growth_factor))
        rural.append(to_int(base["rural"] * variation * growth_factor))
        urbain.append(to_int(base["urbain"] * variation * growth_factor))
        stations.append(to_int(base["stations_montagne"] * variation * growth_factor))
        villages.append(to_int(base["villages_montagne"] * variation * growth_factor))

    # Generate realistic winter holidays
    holidays = []
    
    # Vacances de Noël (S51-S1, typically Dec 23 - Jan 8)
    holidays.append({
        "country_code": "FR",
        "country_name": "France",
        "season": "HIVER",
        "holiday_type": "VACANCES_NOEL",
        "week_start": "S51",
        "week_end": "S1",
        "date_start": f"{year-1}-12-23",
        "date_end": f"{year}-01-08",
    })
    
    # Vacances de ski (S6-S9, typically Feb 10 - Mar 3)
    holidays.append({
        "country_code": "FR",
        "country_name": "France",
        "season": "HIVER",
        "holiday_type": "VACANCES_SKI",
        "week_start": "S6",
        "week_end": "S9",
        "date_start": f"{year}-02-10",
        "date_end": f"{year}-03-03",
    })

    return {
        "weeks": weeks,
        "series": [
            {"id": "observed", "label": "Frequentation observee", "values": observed, "color": "#086cb2"},
            {"id": "prediction", "label": "Prediction" if year > 2024 else "", "values": [None for _ in weeks] if year == 2024 else observed, "color": "#d14247"},
            {"id": "rural", "label": "Feature rural", "values": rural, "color": "#1a7251"},
            {"id": "urbain", "label": "Feature urbain", "values": urbain, "color": "#90437d"},
            {"id": "stations", "label": "Feature stations montagne", "values": stations, "color": "#d4a434"},
            {"id": "villages", "label": "Feature villages montagne", "values": villages, "color": "#00a0df"},
        ],
        "holidays": filter_winter_holidays(holidays),
        "countries": ["France"],
    }


def build_mock_global_holidays(year=2024):
    """Build global holidays (winter period) for the homepage, grouped by countries."""
    year = max(2024, int(year))
    weeks = ["S51", "S52"] + [f"S{i}" for i in range(1, 16)]

    holidays = [
        {
            "country_code": "FR",
            "country_name": "France",
            "season": "HIVER",
            "holiday_type": "VACANCES_NOEL",
            "week_start": "S51",
            "week_end": "S1",
            "date_start": f"{year-1}-12-23",
            "date_end": f"{year}-01-08",
        },
        {
            "country_code": "FR",
            "country_name": "France",
            "season": "HIVER",
            "holiday_type": "VACANCES_HIVER",
            "week_start": "S6",
            "week_end": "S9",
            "date_start": f"{year}-02-10",
            "date_end": f"{year}-03-03",
        },
        {
            "country_code": "BE",
            "country_name": "Belgique",
            "season": "HIVER",
            "holiday_type": "VACANCES_CARNAVAL",
            "week_start": "S8",
            "week_end": "S9",
            "date_start": f"{year}-02-17",
            "date_end": f"{year}-03-01",
        },
        {
            "country_code": "DE",
            "country_name": "Allemagne",
            "season": "HIVER",
            "holiday_type": "FERIEN_HIVER",
            "week_start": "S7",
            "week_end": "S8",
            "date_start": f"{year}-02-10",
            "date_end": f"{year}-02-24",
        },
        {
            "country_code": "NL",
            "country_name": "Pays-Bas",
            "season": "HIVER",
            "holiday_type": "VOORJAARSVAKANTIE",
            "week_start": "S8",
            "week_end": "S9",
            "date_start": f"{year}-02-17",
            "date_end": f"{year}-03-01",
        },
        {
            "country_code": "GB",
            "country_name": "Royaume-Uni",
            "season": "HIVER",
            "holiday_type": "HALF_TERM",
            "week_start": "S7",
            "week_end": "S8",
            "date_start": f"{year}-02-10",
            "date_end": f"{year}-02-23",
        },
    ]

    winter_holidays = filter_winter_holidays(holidays)
    countries = sorted({item.get("country_name") for item in winter_holidays if item.get("country_name")})

    return {
        "weeks": weeks,
        "holidays": winter_holidays,
        "countries": countries,
    }


def fetch_global_holidays_from_snowflake(year):
    """Fetch global holidays for a selected winter season from CLEANED.CALENDAR_SCHOOL_HOLIDAYS."""
    holidays_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "CALENDAR_SCHOOL_HOLIDAYS")
    countries_table = fq_table(SNOWFLAKE_DIM_SCHEMA, "DIM_COUNTRIES")
    season_code = f"H{str(int(year))[-2:]}".upper()

    private_key = load_private_key_der_bytes()
    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=SNOWFLAKE_SCHEMA_PUBLIC,
        private_key=private_key,
        login_timeout=SNOWFLAKE_LOGIN_TIMEOUT_SECONDS,
        network_timeout=SNOWFLAKE_NETWORK_TIMEOUT_SECONDS,
        socket_timeout=SNOWFLAKE_SOCKET_TIMEOUT_SECONDS,
        ocsp_fail_open=True,
        session_parameters={"QUERY_TAG": "aura_global_holidays"},
    )

    try:
        with connection.cursor(DictCursor) as cursor:
            query = (
                "SELECT "
                "h.CODE_PAYS AS code_pays, "
                "COALESCE(c.NOM, h.CODE_PAYS) AS country_name, "
                "h.DEBUT AS debut, "
                "h.FIN AS fin, "
                "h.SAISON AS saison, "
                "h.TYPE_VACANCES AS type_vacances "
                f"FROM {holidays_table} h "
                f"LEFT JOIN {countries_table} c ON c.CODE_PAYS = h.CODE_PAYS "
                "WHERE h.DEBUT IS NOT NULL "
                "AND h.FIN IS NOT NULL "
                "AND UPPER(h.SAISON) = %s"
            )
            cursor.execute(query, (season_code,), timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            rows = cursor.fetchall()
    finally:
        connection.close()

    holidays = []
    countries = set()
    weeks = set()

    for row in rows:
        week_start = week_label_from_date(read_ci(row, "debut"))
        week_end = week_label_from_date(read_ci(row, "fin"))
        if not week_start or not week_end:
            continue

        code = str(read_ci(row, "code_pays") or "").strip().upper()
        country_name = str(read_ci(row, "country_name") or code or "Inconnu").strip()

        debut_value = read_ci(row, "debut")
        fin_value = read_ci(row, "fin")
        if isinstance(debut_value, datetime.datetime):
            date_start = debut_value.date().isoformat()
        elif isinstance(debut_value, datetime.date):
            date_start = debut_value.isoformat()
        else:
            date_start = str(debut_value or "")

        if isinstance(fin_value, datetime.datetime):
            date_end = fin_value.date().isoformat()
        elif isinstance(fin_value, datetime.date):
            date_end = fin_value.isoformat()
        else:
            date_end = str(fin_value or "")

        holidays.append(
            {
                "country_code": code,
                "country_name": country_name,
                "season": str(read_ci(row, "saison") or "").strip(),
                "holiday_type": str(read_ci(row, "type_vacances") or "").strip(),
                "week_start": week_start,
                "week_end": week_end,
                "date_start": date_start,
                "date_end": date_end,
            }
        )
        countries.add(country_name)
        weeks.add(week_start)
        weeks.add(week_end)

    winter_holidays = filter_winter_holidays(holidays)
    winter_weeks = sorted(
        {
            w
            for item in winter_holidays
            for w in (item.get("week_start"), item.get("week_end"))
            if w
        },
        key=season_week_sort,
    )

    winter_countries = sorted(
        {
            item.get("country_name")
            for item in winter_holidays
            if item.get("country_name")
        }
    )

    return {
        "year": int(year),
        "season": season_code,
        "weeks": winter_weeks,
        "holidays": winter_holidays,
        "countries": winter_countries,
    }


def fetch_department_timeline_from_snowflake(canonical_name):
    fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
    holidays_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "CALENDAR_SCHOOL_HOLIDAYS")
    countries_table = fq_table(SNOWFLAKE_DIM_SCHEMA, "DIM_COUNTRIES")
    department_code = DEPARTMENT_TO_CODE.get(canonical_name)

    private_key = load_private_key_der_bytes()
    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=SNOWFLAKE_SCHEMA_PUBLIC,
        private_key=private_key,
        login_timeout=SNOWFLAKE_LOGIN_TIMEOUT_SECONDS,
        network_timeout=SNOWFLAKE_NETWORK_TIMEOUT_SECONDS,
        socket_timeout=SNOWFLAKE_SOCKET_TIMEOUT_SECONDS,
        ocsp_fail_open=True,
        session_parameters={"QUERY_TAG": "aura_department_timeline"},
    )

    try:
        with connection.cursor(DictCursor) as cursor:
            if department_code:
                weekly_query = (
                    "SELECT "
                    "WEEK AS week, "
                    "SUM(TOTAL_AURA) AS total_aura, "
                    "SUM(RURAL) AS rural, "
                    "SUM(URBAIN) AS urbain, "
                    "SUM(STATIONS_MONTAGNE) AS stations_montagne, "
                    "SUM(VILLAGES_MONTAGNE) AS villages_montagne "
                    f"FROM {fact_table} "
                    "WHERE CODE_DEPARTEMENT = %s "
                    "GROUP BY WEEK"
                )
                cursor.execute(weekly_query, (department_code,), timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            else:
                weekly_query = (
                    "SELECT "
                    "WEEK AS week, "
                    "SUM(TOTAL_AURA) AS total_aura, "
                    "SUM(RURAL) AS rural, "
                    "SUM(URBAIN) AS urbain, "
                    "SUM(STATIONS_MONTAGNE) AS stations_montagne, "
                    "SUM(VILLAGES_MONTAGNE) AS villages_montagne "
                    f"FROM {fact_table} "
                    "GROUP BY WEEK"
                )
                cursor.execute(weekly_query, timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            weekly_rows = cursor.fetchall()

        week_values = {}
        for row in weekly_rows:
            week = week_label(read_ci(row, "week"))
            week_values[week] = {
                "total_aura": to_int(read_ci(row, "total_aura")),
                "rural": to_int(read_ci(row, "rural")),
                "urbain": to_int(read_ci(row, "urbain")),
                "stations_montagne": to_int(read_ci(row, "stations_montagne")),
                "villages_montagne": to_int(read_ci(row, "villages_montagne")),
            }

        ordered_weeks = sorted(week_values.keys(), key=season_week_sort)

        with connection.cursor(DictCursor) as cursor:
            holidays_query = (
                "SELECT "
                "h.CODE_PAYS AS code_pays, "
                "COALESCE(c.NOM, h.CODE_PAYS) AS country_name, "
                "h.DEBUT AS debut, "
                "h.FIN AS fin, "
                "h.SAISON AS saison, "
                "h.TYPE_VACANCES AS type_vacances "
                f"FROM {holidays_table} h "
                f"LEFT JOIN {countries_table} c ON c.CODE_PAYS = h.CODE_PAYS "
                "WHERE h.DEBUT IS NOT NULL AND h.FIN IS NOT NULL"
            )
            cursor.execute(holidays_query, timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            holiday_rows = cursor.fetchall()
    finally:
        connection.close()

    holidays = []
    countries = set()
    for row in holiday_rows:
        week_start = week_label_from_date(read_ci(row, "debut"))
        week_end = week_label_from_date(read_ci(row, "fin"))
        if not week_start or not week_end:
            continue

        country_name = str(read_ci(row, "country_name") or read_ci(row, "code_pays") or "").strip()
        if country_name:
            countries.add(country_name)

        holidays.append(
            {
                "country_code": str(read_ci(row, "code_pays") or "").strip(),
                "country_name": country_name,
                "season": str(read_ci(row, "saison") or "").strip(),
                "holiday_type": str(read_ci(row, "type_vacances") or "").strip(),
                "week_start": week_start,
                "week_end": week_end,
            }
        )

    series = [
        {
            "id": "observed",
            "label": "Frequentation observee",
            "values": [week_values[week]["total_aura"] for week in ordered_weeks],
            "color": "#086cb2",
        },
        {
            "id": "prediction",
            "label": "Prediction (placeholder)",
            "values": [None for _ in ordered_weeks],
            "color": "#d14247",
        },
        {
            "id": "rural",
            "label": "Feature rural",
            "values": [week_values[week]["rural"] for week in ordered_weeks],
            "color": "#1a7251",
        },
        {
            "id": "urbain",
            "label": "Feature urbain",
            "values": [week_values[week]["urbain"] for week in ordered_weeks],
            "color": "#90437d",
        },
        {
            "id": "stations",
            "label": "Feature stations montagne",
            "values": [week_values[week]["stations_montagne"] for week in ordered_weeks],
            "color": "#d4a434",
        },
        {
            "id": "villages",
            "label": "Feature villages montagne",
            "values": [week_values[week]["villages_montagne"] for week in ordered_weeks],
            "color": "#00a0df",
        },
    ]

    return {
        "weeks": ordered_weeks,
        "series": series,
        "holidays": holidays,
        "countries": sorted(list(countries)),
    }


def week_sort_key(week_value):
    text = str(week_value or "").upper()
    if text.startswith("S") and text[1:].isdigit():
        return int(text[1:])
    return 999


def get_snowflake_query():
    fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
    dim_table = fq_table(SNOWFLAKE_DIM_SCHEMA, "DIM_DEPARTEMENTS")

    custom_query = os.getenv("SNOWFLAKE_KPI_QUERY", "").strip()
    if custom_query:
        return custom_query

    # Lire la table fact et joindre la dimension pour recuperer le nom de departement.
    return (
        "SELECT "
        "f.CODE_DEPARTEMENT as department_code, "
        "COALESCE(d.NOM_DEPARTEMENT, f.CODE_DEPARTEMENT) as department_name, "
        "f.WEEK as week, "
        "CAST(f.TOTAL_AURA as INTEGER) as frequentation "
        f"FROM {fact_table} f "
        f"LEFT JOIN {dim_table} d ON d.CODE_DEPARTEMENT = f.CODE_DEPARTEMENT "
        "ORDER BY f.CODE_DEPARTEMENT"
    )


def fetch_available_weeks_from_snowflake(connection):
    fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
    with connection.cursor(DictCursor) as cursor:
        cursor.execute(
            f"SELECT DISTINCT WEEK FROM {fact_table} WHERE WEEK IS NOT NULL",
            timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS,
        )
        rows = cursor.fetchall()

    weeks = []
    for row in rows:
        value = read_ci(row, "WEEK")
        if value is not None:
            weeks.append(str(value).upper())

    weeks = sorted(set(weeks), key=week_sort_key)
    return weeks


def fetch_dataset_from_snowflake(selected_weeks=None):
    selected_weeks = selected_weeks or []
    fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
    dim_table = fq_table(SNOWFLAKE_DIM_SCHEMA, "DIM_DEPARTEMENTS")

    private_key = load_private_key_der_bytes()

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=SNOWFLAKE_SCHEMA_PUBLIC,
        private_key=private_key,
        login_timeout=SNOWFLAKE_LOGIN_TIMEOUT_SECONDS,
        network_timeout=SNOWFLAKE_NETWORK_TIMEOUT_SECONDS,
        socket_timeout=SNOWFLAKE_SOCKET_TIMEOUT_SECONDS,
        ocsp_fail_open=True,
        session_parameters={"QUERY_TAG": "aura_dashboard_backend"},
    )

    try:
        available_weeks = fetch_available_weeks_from_snowflake(connection)

        where_clause = ""
        if selected_weeks:
            week_list = ", ".join([f"'{week}'" for week in selected_weeks])
            where_clause = f"WHERE WEEK IN ({week_list})"

        query = (
            "SELECT "
            "f.CODE_DEPARTEMENT as department_code, "
            "COALESCE(d.NOM_DEPARTEMENT, f.CODE_DEPARTEMENT) as department_name, "
            "SUM(f.TOTAL_AURA) as total_aura, "
            "SUM(f.RURAL) as rural, "
            "SUM(f.URBAIN) as urbain, "
            "SUM(f.STATIONS_MONTAGNE) as stations_montagne, "
            "SUM(f.VILLAGES_MONTAGNE) as villages_montagne "
            f"FROM {fact_table} f "
            f"LEFT JOIN {dim_table} d ON d.CODE_DEPARTEMENT = f.CODE_DEPARTEMENT "
            f"{where_clause} "
            "GROUP BY f.CODE_DEPARTEMENT, d.NOM_DEPARTEMENT "
            "ORDER BY f.CODE_DEPARTEMENT"
        )

        with connection.cursor(DictCursor) as cursor:
            cursor.execute(query, timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            rows = cursor.fetchall()
    finally:
        connection.close()

    dataset = {}
    for row in rows:
        raw_name = read_ci(row, "department_name")
        raw_code = read_ci(row, "department_code")
        canonical_name = normalize_department_name(str(raw_name or ""))
        if not canonical_name and raw_code is not None:
            canonical_name = normalize_department_name(str(raw_code))
        if not canonical_name:
            continue

        dataset[canonical_name] = {
            "total_aura": to_int(read_ci(row, "total_aura")),
            "rural": to_int(read_ci(row, "rural")),
            "urbain": to_int(read_ci(row, "urbain")),
            "stations_montagne": to_int(read_ci(row, "stations_montagne")),
            "villages_montagne": to_int(read_ci(row, "villages_montagne")),
        }
        dataset[canonical_name]["frequentation"] = dataset[canonical_name]["total_aura"]

    return dataset, query, available_weeks


def normalize_activities_value(value):
    if value is None:
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        raw_text = str(value).strip()
        if not raw_text:
            return []
        # Some Snowflake connectors return ARRAY as JSON-like string.
        if raw_text.startswith("[") and raw_text.endswith("]"):
            content = raw_text[1:-1]
            raw_items = [part.strip().strip('"').strip("'") for part in content.split(",")]
        else:
            raw_items = [part.strip() for part in raw_text.split(",")]

    cleaned = []
    seen = set()
    for item in raw_items:
        text = str(item or "").strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        cleaned.append(text)
    return cleaned


def read_first_non_empty(data, candidates):
    for key in candidates:
        value = read_ci(data, key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def parse_float(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    return float(text)


def fetch_stations_from_snowflake(selected_activities=None):
    selected_activities = selected_activities or []
    ref_table = fq_table(SNOWFLAKE_SCHEMA_REF, "REF_STATIONS")

    private_key = load_private_key_der_bytes()
    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=SNOWFLAKE_SCHEMA_PUBLIC,
        private_key=private_key,
        login_timeout=SNOWFLAKE_LOGIN_TIMEOUT_SECONDS,
        network_timeout=SNOWFLAKE_NETWORK_TIMEOUT_SECONDS,
        socket_timeout=SNOWFLAKE_SOCKET_TIMEOUT_SECONDS,
        ocsp_fail_open=True,
        session_parameters={"QUERY_TAG": "aura_dashboard_stations"},
    )

    try:
        query = (
            "SELECT OBJECT_CONSTRUCT_KEEP_NULL(*) AS row_obj "
            f"FROM {ref_table}"
        )

        with connection.cursor(DictCursor) as cursor:
            cursor.execute(query, timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            rows = cursor.fetchall()
    finally:
        connection.close()

    selected_activities_set = {item.lower() for item in selected_activities}
    points = []
    activities_set = set()
    for row in rows:
        row_obj = read_ci(row, "row_obj") or {}
        if not isinstance(row_obj, dict):
            continue

        lat_raw = read_first_non_empty(row_obj, ["LATITUDE", "LAT", "Y", "COORD_Y"])
        lon_raw = read_first_non_empty(row_obj, ["LONGITUDE", "LON", "LNG", "X", "COORD_X"])

        lat = None
        lon = None
        try:
            lat = parse_float(lat_raw)
            lon = parse_float(lon_raw)
        except (TypeError, ValueError):
            lat = None
            lon = None

        if lat is None or lon is None:
            continue

        activities_raw = read_first_non_empty(row_obj, ["ACTIVITES_LISTE", "ACTIVITES", "ACTIVITES_LIST", "ACTIVITY_LIST"])
        activities = normalize_activities_value(activities_raw)
        for activity in activities:
            activities_set.add(activity)

        if selected_activities_set and not any(activity.lower() in selected_activities_set for activity in activities):
            continue

        station_name = read_first_non_empty(row_obj, ["NOM_INSTALLATION", "NOM_INSTALLATION_SPORTIVE", "NOM_STATION", "STATION_NAME", "NOM"])
        department_code = read_first_non_empty(row_obj, ["CODE_DEPARTEMENT", "DEPARTEMENT_CODE", "DEP_CODE"])
        department_name = read_first_non_empty(row_obj, ["NOM_DEPARTEMENT", "DEPARTEMENT_NOM", "DEP_NOM", "DEPARTEMENT"])
        equipment_type = read_first_non_empty(row_obj, ["TYPE_EQUIPEMENT", "TYPE_D_EQUIPEMENT_SPORTIF", "TYPE"])

        points.append(
            {
                "name": str(station_name or "Installation sans nom").strip(),
                "department_code": str(department_code or "").strip(),
                "department_name": str(department_name or "").strip(),
                "equipment_type": str(equipment_type or "").strip(),
                "lat": lat,
                "lon": lon,
                "activities": activities,
            }
        )

    available_activities = sorted(list(activities_set), key=lambda item: str(item).lower())
    return points, available_activities


def get_station_points(selected_activities=None):
    global last_snowflake_error

    selected_activities = selected_activities or []
    activity_key = "|".join([item.lower() for item in selected_activities]) if selected_activities else "ALL"
    cache_key = f"stations:{activity_key}"

    cached_value, hit = get_cache(cache_key)
    if hit:
        return cached_value["points"], cached_value["activities_available"], True, cached_value["data_source"]

    if is_snowflake_configured():
        try:
            points, activities_available = fetch_stations_from_snowflake(selected_activities)
            payload = {
                "points": points,
                "activities_available": activities_available,
                "data_source": "snowflake",
            }
            set_cache(cache_key, payload)
            return points, activities_available, False, "snowflake"
        except Exception as exc:
            last_snowflake_error = str(exc)

    return [], [], False, "mock"


def build_departments_payload(selected_kpi, dataset):
    items = []
    for canonical_name, values in dataset.items():
        # Defensive defaults avoid runtime crashes if a source row misses a KPI key.
        frequentation = values.get("frequentation", values.get("total_aura", 0))
        score = values.get(selected_kpi, 0)
        items.append(
            {
                "name": to_display_name(canonical_name),
                "frequentation": frequentation,
                "score": score,
            }
        )
    return items


def get_cache(cache_key):
    cached = api_cache.get(cache_key)
    if not cached:
        return None, False

    if cached["expires_at"] < time.time():
        api_cache.pop(cache_key, None)
        return None, False

    return cached["value"], True


def set_cache(cache_key, value):
    api_cache[cache_key] = {
        "value": value,
        "expires_at": time.time() + CACHE_TTL_SECONDS,
    }


def get_department_dataset(selected_weeks=None):
    global last_snowflake_error

    selected_weeks = selected_weeks or []
    weeks_key = ",".join(selected_weeks) if selected_weeks else "ALL"

    dataset, hit = get_cache(f"dataset:{weeks_key}")
    if hit:
        available_weeks, _ = get_cache("available_weeks")
        return dataset, True, "cache", available_weeks or []

    if is_snowflake_configured():
        try:
            dataset, query_used, available_weeks = fetch_dataset_from_snowflake(selected_weeks)
            if dataset:
                set_cache(f"dataset:{weeks_key}", dataset)
                set_cache("available_weeks", available_weeks)
                last_snowflake_error = None
                return dataset, False, "snowflake", available_weeks
            last_snowflake_error = "Snowflake query returned no usable rows"
        except Exception as exc:
            last_snowflake_error = str(exc)

    # TODO Snowflake: remplacer par un SELECT et mapper les lignes vers ce format.
    dataset = DEPARTMENT_KPIS
    set_cache(f"dataset:{weeks_key}", dataset)
    return dataset, False, "mock", []

@app.route("/")
def home():
    return "Flask API is running"

@app.route("/api/hello")
def hello():
    return jsonify({
        "message": "Hello from Flask"
    })


@app.route("/api/data")
def data():
    global last_snowflake_error

    try:
        selected_kpi = request.args.get("kpi", "total_aura").lower().strip()
        selected_weeks = parse_weeks_param(request.args.get("weeks", ""))
        if selected_kpi not in VALID_KPIS:
            return jsonify({"error": "Invalid KPI", "valid_kpis": sorted(VALID_KPIS)}), 400

        dataset, dataset_cache_hit, data_source, available_weeks = get_department_dataset(selected_weeks)
        weeks_key = ",".join(selected_weeks) if selected_weeks else "ALL"
        cache_key = f"api_data:{selected_kpi}:{weeks_key}"
        cached_payload, payload_cache_hit = get_cache(cache_key)

        if payload_cache_hit:
            payload = dict(cached_payload)
        else:
            departments = build_departments_payload(selected_kpi, dataset)
            scores = [item["score"] for item in departments]
            if scores:
                ranges = {
                    "min": min(scores),
                    "max": max(scores),
                    "avg": round(sum(scores) / len(scores), 1),
                }
            else:
                ranges = {"min": 0, "max": 0, "avg": 0}

            payload = {
                "selected_kpi": selected_kpi,
                "kpis": sorted(VALID_KPIS),
                "weeks_selected": selected_weeks,
                "weeks_available": available_weeks,
                "departments": departments,
                "ranges": ranges,
            }
            set_cache(cache_key, payload)

        payload["cache"] = {
            "dataset_hit": dataset_cache_hit,
            "payload_hit": payload_cache_hit,
            "ttl_seconds": CACHE_TTL_SECONDS,
        }
        payload["data_source"] = data_source
        payload["snowflake_error"] = last_snowflake_error
        return jsonify(payload)
    except Exception as exc:
        # Emergency fallback: keep frontend functional instead of returning 500.
        last_snowflake_error = f"api/data runtime error: {exc}"
        departments = build_departments_payload("total_aura", DEPARTMENT_KPIS)
        scores = [item["score"] for item in departments]
        ranges = {
            "min": min(scores) if scores else 0,
            "max": max(scores) if scores else 0,
            "avg": round(sum(scores) / len(scores), 1) if scores else 0,
        }
        return jsonify(
            {
                "selected_kpi": "total_aura",
                "kpis": sorted(VALID_KPIS),
                "weeks_selected": [],
                "weeks_available": [],
                "departments": departments,
                "ranges": ranges,
                "cache": {
                    "dataset_hit": False,
                    "payload_hit": False,
                    "ttl_seconds": CACHE_TTL_SECONDS,
                },
                "data_source": "mock",
                "snowflake_error": last_snowflake_error,
            }
        )


@app.route("/api/department/<dep_name>")
def department_data(dep_name):
    global last_snowflake_error

    try:
        canonical_name = normalize_department_name(dep_name)
        dataset, dataset_cache_hit, data_source, _ = get_department_dataset()

        if not canonical_name or canonical_name not in dataset:
            return jsonify({"error": f"Unknown department: {dep_name}"}), 404

        cache_key = f"api_department:{canonical_name}"
        cached_payload, payload_cache_hit = get_cache(cache_key)

        if payload_cache_hit:
            payload = dict(cached_payload)
        else:
            values = dataset[canonical_name]
            payload = {
                "name": to_display_name(canonical_name),
                "kpis": values,
                "insights": [
                    "Pic de frequentation pendant la saison hivernale.",
                    "Niveau global de frequentation observe sur la periode selectionnee.",
                ],
            }
            set_cache(cache_key, payload)

        payload["cache"] = {
            "dataset_hit": dataset_cache_hit,
            "payload_hit": payload_cache_hit,
            "ttl_seconds": CACHE_TTL_SECONDS,
        }
        payload["data_source"] = data_source
        payload["snowflake_error"] = last_snowflake_error
        return jsonify(payload)
    except Exception as exc:
        last_snowflake_error = f"api/department runtime error: {exc}"
        return jsonify({"error": last_snowflake_error}), 500


@app.route("/api/global/holidays")
def global_holidays_data():
    global last_snowflake_error

    year_param = request.args.get("year", "2024").strip()
    try:
        year = int(year_param)
        year = max(2024, year)
    except ValueError:
        year = 2024

    cache_key = f"api_global_holidays:{year}"
    cached_payload, payload_cache_hit = get_cache(cache_key)
    if payload_cache_hit:
        payload = dict(cached_payload)
        payload["cache"] = {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}
        return jsonify(payload)

    data_source = "mock"
    try:
        if is_snowflake_configured():
            snowflake_data = fetch_global_holidays_from_snowflake(year)
            payload = {
                "year": year,
                "season": snowflake_data.get("season"),
                "weeks": snowflake_data.get("weeks", []),
                "holidays": snowflake_data.get("holidays", []),
                "countries": snowflake_data.get("countries", []),
                "data_source": "snowflake",
                "snowflake_error": last_snowflake_error,
            }
            data_source = "snowflake"
        else:
            mock_data = build_mock_global_holidays(year)
            payload = {
                "year": year,
                "weeks": mock_data["weeks"],
                "holidays": mock_data["holidays"],
                "countries": mock_data["countries"],
                "data_source": "mock",
                "snowflake_error": last_snowflake_error,
            }
    except Exception as exc:
        last_snowflake_error = str(exc)
        mock_data = build_mock_global_holidays(year)
        payload = {
            "year": year,
            "weeks": mock_data["weeks"],
            "holidays": mock_data["holidays"],
            "countries": mock_data["countries"],
            "data_source": "mock",
            "snowflake_error": last_snowflake_error,
        }

    set_cache(cache_key, payload)
    payload["cache"] = {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}
    payload["data_source"] = data_source
    return jsonify(payload)


@app.route("/api/department/<dep_name>/timeline")
def department_timeline(dep_name):
    global last_snowflake_error

    canonical_name = normalize_department_name(dep_name)
    if not canonical_name:
        return jsonify({"error": f"Unknown department: {dep_name}"}), 404

    # Parse year parameter (2024 = observed, 2025+ = ML predictions)
    year_param = request.args.get("year", "2024").strip()
    try:
        year = int(year_param)
        year = max(2024, year)  # Minimum 2024
    except ValueError:
        year = 2024

    cache_key = f"api_department_timeline:{canonical_name}:{year}"
    cached_payload, payload_cache_hit = get_cache(cache_key)
    if payload_cache_hit:
        payload = dict(cached_payload)
        payload["cache"] = {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}
        return jsonify(payload)

    data_source = "mock"
    try:
        if year == 2024 and is_snowflake_configured():
            # For 2024, try to fetch from Snowflake FREQ_GLOBAL_PER_DEPT
            timeline = fetch_department_timeline_from_snowflake(canonical_name)
            # Filter holidays to winter only
            timeline["holidays"] = filter_winter_holidays(timeline.get("holidays", []))
            data_source = "snowflake"
        else:
            # For 2024 without Snowflake or 2025+: use mock with growth factor
            timeline = build_mock_department_timeline(canonical_name, year=year)
    except Exception as exc:
        last_snowflake_error = str(exc)
        timeline = build_mock_department_timeline(canonical_name, year=year)

    payload = {
        "name": to_display_name(canonical_name),
        "year": year,
        "timeline": timeline,
        "data_source": data_source,
        "snowflake_error": last_snowflake_error,
    }
    set_cache(cache_key, payload)
    payload["cache"] = {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}
    return jsonify(payload)


def extract_activities_from_array(array_value):
    """
    Parse Snowflake ARRAY ACTIVITES_LISTE.
    Peut être: list, string "[...]", ou autre format.
    Retourne une liste de strings nettoyées.
    """
    if not array_value:
        return []
    
    items = []
    
    # Si c'est déjà une liste Python
    if isinstance(array_value, list):
        for item in array_value:
            text = str(item or "").strip()
            if text:
                items.append(text)
        return items
    
    # Si c'est un string JSON-like "[...]"
    text = str(array_value or "").strip()
    if text.startswith("[") and text.endswith("]"):
        content = text[1:-1]
        for part in content.split(","):
            item_text = part.strip().strip('"').strip("'")
            if item_text:
                items.append(item_text)
        return items
    
    # Si c'est un string direct séparé par des virgules
    if "," in text:
        for part in text.split(","):
            item_text = part.strip()
            if item_text:
                items.append(item_text)
        return items
    
    # Un seul item
    if text:
        items.append(text)
    
    return items


def fetch_stations_from_snowflake():
    """
    Récupère tous les points d'installation depuis REF.REF_STATIONS.
    Retourne:
        - station_points: list de dicts avec {name, lat, lon, department_code, department_name, equipment_type, activities}
        - activities_available: list d'activités distinctes trouvées
    """
    ref_table = fq_table(SNOWFLAKE_SCHEMA_REF, "REF_STATIONS")
    
    private_key = load_private_key_der_bytes()
    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=SNOWFLAKE_SCHEMA_PUBLIC,
        private_key=private_key,
        login_timeout=SNOWFLAKE_LOGIN_TIMEOUT_SECONDS,
        network_timeout=SNOWFLAKE_NETWORK_TIMEOUT_SECONDS,
        socket_timeout=SNOWFLAKE_SOCKET_TIMEOUT_SECONDS,
        ocsp_fail_open=True,
        session_parameters={"QUERY_TAG": "aura_dashboard_stations"},
    )
    
    try:
        with connection.cursor(DictCursor) as cursor:
            cursor.execute(
                f"SELECT * FROM {ref_table}",
                timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS,
            )
            rows = cursor.fetchall()
    finally:
        connection.close()
    
    station_points = []
    activities_set = set()
    
    for row in rows:
        # Récupère les colonnes (case insensitive)
        lat = read_ci(row, "LATITUDE")
        lon = read_ci(row, "LONGITUDE")
        
        # Convertir en float
        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            continue  # Skip si pas de coordonnées valides
        
        # Extraire activités
        activities_raw = read_ci(row, "ACTIVITES_LISTE")
        activities = extract_activities_from_array(activities_raw)
        for activity in activities:
            activities_set.add(activity)
        
        # Construire le point
        point = {
            "name": str(read_ci(row, "NOM_INSTALLATION") or "Installation sans nom").strip(),
            "lat": lat,
            "lon": lon,
            "department_code": str(read_ci(row, "CODE_DEPARTEMENT") or "").strip(),
            "department_name": str(read_ci(row, "NOM_DEPARTEMENT") or "").strip(),
            "equipment_type": str(read_ci(row, "TYPE_EQUIPEMENT") or "").strip(),
            "activities": activities,
        }
        station_points.append(point)
    
    activities_available = sorted(list(activities_set), key=lambda x: str(x).lower())
    return station_points, activities_available


@app.route("/api/stations")
def stations_data():
    """
    Retourne tous les points d'installation avec les activités disponibles.
    Optionnel: filtrer par activités via ?activities=Activité1,Activité2
    """
    global last_snowflake_error
    
    try:
        # Récupérer les stations depuis Snowflake
        if not is_snowflake_configured():
            return jsonify({
                "activities_available": [],
                "points": [],
                "data_source": "error",
                "error": "Snowflake not configured",
            }), 400
        
        station_points, activities_available = fetch_stations_from_snowflake()
        
        # Filtrer par activités si demandé
        selected_activities_param = request.args.get("activities", "")
        selected_activities = [a.strip() for a in selected_activities_param.split(",") if a.strip()] if selected_activities_param else []
        
        filtered_points = station_points
        if selected_activities:
            activities_set = set(a.lower() for a in selected_activities)
            filtered_points = [
                p for p in station_points
                if any(activity.lower() in activities_set for activity in p.get("activities", []))
            ]
        
        return jsonify({
            "activities_available": activities_available,
            "activities_selected": selected_activities,
            "points": filtered_points,
            "data_source": "snowflake",
            "count": len(filtered_points),
        }), 200
    
    except Exception as exc:
        last_snowflake_error = str(exc)
        return jsonify({
            "activities_available": [],
            "points": [],
            "data_source": "error",
            "error": str(exc),
        }), 500


@app.route("/api/snowflake/status")
def snowflake_status():
    return jsonify(
        {
            "configured": is_snowflake_configured(),
            "missing_env_vars": get_missing_required_env_vars(),
            "using_query": get_snowflake_query(),
            "public_schema": SNOWFLAKE_SCHEMA_PUBLIC,
            "fact_schema": SNOWFLAKE_FACT_SCHEMA,
            "dim_schema": SNOWFLAKE_DIM_SCHEMA,
            "ref_schema": SNOWFLAKE_SCHEMA_REF,
            "last_error": last_snowflake_error,
        }
    )


@app.route("/api/snowflake/test/freq-globale")
def test_freq_globale():
    """
    Test de connexion Snowflake et lecture de la table FREQ_GLOBAL_PER_DEPT
    """
    if not is_snowflake_configured():
        return jsonify({
            "success": False,
            "error": "Snowflake non configure. Verifiez les variables d'environnement.",
            "required_env_vars": get_required_env_vars(),
            "missing_env_vars": get_missing_required_env_vars(),
            "schema_config": {
                "public_schema": SNOWFLAKE_SCHEMA_PUBLIC,
                "fact_schema": SNOWFLAKE_FACT_SCHEMA,
                "dim_schema": SNOWFLAKE_DIM_SCHEMA,
                "ref_schema": SNOWFLAKE_SCHEMA_REF,
            },
            "schema_env_vars_supported": [
                "SNOWFLAKE_SCHEMA_PUBLIC",
                "SNOWFLAKE_SCHEMA_FACT",
                "SNOWFLAKE_SCHEMA_DIM",
                "SNOWFLAKE_SCHEMA_REF",
            ],
        }), 400

    try:
        print("[DEBUG] Tentative de connexion Snowflake...")
        private_key = load_private_key_der_bytes()

        connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=SNOWFLAKE_SCHEMA_PUBLIC,
            private_key=private_key,
            login_timeout=SNOWFLAKE_LOGIN_TIMEOUT_SECONDS,
            network_timeout=SNOWFLAKE_NETWORK_TIMEOUT_SECONDS,
            socket_timeout=SNOWFLAKE_SOCKET_TIMEOUT_SECONDS,
            ocsp_fail_open=True,
            session_parameters={"QUERY_TAG": "aura_dashboard_test"},
        )

        print("[DEBUG] Connexion etablie avec succes!")

        # Test 1: Lister les tables disponibles
        print("[DEBUG] Recuperation des tables disponibles...")
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES;", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            tables = cursor.fetchall()
            table_names = [row[1] for row in tables]  # row[1] est le nom de la table

        print(f"[DEBUG] Tables trouvees: {table_names}")

        # Test 2: Interroger FREQ_GLOBAL_PER_DEPT
        fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
        print("[DEBUG] Interrogation de FREQ_GLOBAL_PER_DEPT...")
        with connection.cursor(DictCursor) as cursor:
            cursor.execute(
                f"SELECT * FROM {fact_table} LIMIT 100;",
                timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS,
            )
            rows = cursor.fetchall()

        print(f"[DEBUG] {len(rows)} lignes recuperees de FREQ_GLOBAL_PER_DEPT")

        # Convertir les resultats en format JSON-serializable
        data = []
        for row in rows:
            data.append(dict(row))

        connection.close()

        return jsonify({
            "success": True,
            "message": f"Connexion Snowflake reussie! {len(rows)} lignes trouvees dans FREQ_GLOBAL_PER_DEPT",
            "connection_details": {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": SNOWFLAKE_SCHEMA_PUBLIC,
            },
            "available_tables": table_names,
            "freq_globale_columns": list(data[0].keys()) if data else [],
            "freq_globale_row_count": len(rows),
            "freq_globale_sample": data[:10]  # Affiche les 10 premieres lignes
        }), 200

    except Exception as exc:
        print(f"[DEBUG] Erreur Snowflake: {str(exc)}")
        import traceback
        traceback.print_exc()

        return jsonify({
            "success": False,
            "error": str(exc),
            "error_type": type(exc).__name__,
            "traceback": traceback.format_exc()
        }), 500


@app.route("/api/snowflake/test/ref-stations")
def test_ref_stations():
    """
    Test de connexion Snowflake et inspection de la table REF_STATIONS.
    Retourne un echantillon brut + valeurs parsees pour faciliter le debug.
    """
    if not is_snowflake_configured():
        return jsonify({
            "success": False,
            "error": "Snowflake non configure. Verifiez les variables d'environnement.",
            "required_env_vars": get_required_env_vars(),
            "missing_env_vars": get_missing_required_env_vars(),
            "schema_config": {
                "public_schema": SNOWFLAKE_SCHEMA_PUBLIC,
                "fact_schema": SNOWFLAKE_FACT_SCHEMA,
                "dim_schema": SNOWFLAKE_DIM_SCHEMA,
                "ref_schema": SNOWFLAKE_SCHEMA_REF,
            },
        }), 400

    try:
        limit = request.args.get("limit", "20")
        try:
            limit_value = int(limit)
        except ValueError:
            limit_value = 20
        limit_value = max(1, min(limit_value, 100))

        private_key = load_private_key_der_bytes()
        connection = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=SNOWFLAKE_SCHEMA_PUBLIC,
            private_key=private_key,
            login_timeout=SNOWFLAKE_LOGIN_TIMEOUT_SECONDS,
            network_timeout=SNOWFLAKE_NETWORK_TIMEOUT_SECONDS,
            socket_timeout=SNOWFLAKE_SOCKET_TIMEOUT_SECONDS,
            ocsp_fail_open=True,
            session_parameters={"QUERY_TAG": "aura_dashboard_test_ref_stations"},
        )

        ref_table = fq_table(SNOWFLAKE_SCHEMA_REF, "REF_STATIONS")

        try:
            with connection.cursor(DictCursor) as cursor:
                cursor.execute(
                    f"SELECT * FROM {ref_table} LIMIT {limit_value}",
                    timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS,
                )
                raw_rows = cursor.fetchall()

            with connection.cursor(DictCursor) as cursor:
                cursor.execute(
                    (
                        "SELECT DISTINCT TRIM(f.value::string) AS activity "
                        f"FROM {ref_table} s, LATERAL FLATTEN(input => s.ACTIVITES_LISTE) f "
                        "WHERE f.value IS NOT NULL "
                        "ORDER BY activity LIMIT 200"
                    ),
                    timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS,
                )
                activity_rows = cursor.fetchall()

            parsed_preview = []
            invalid_coords = 0
            for row in raw_rows:
                activities = normalize_activities_value(read_ci(row, "ACTIVITES_LISTE"))
                try:
                    lat = parse_float(read_ci(row, "LATITUDE"))
                    lon = parse_float(read_ci(row, "LONGITUDE"))
                except (TypeError, ValueError):
                    lat = None
                    lon = None

                if lat is None or lon is None:
                    invalid_coords += 1

                parsed_preview.append(
                    {
                        "name": read_ci(row, "NOM_INSTALLATION"),
                        "department_code": read_ci(row, "CODE_DEPARTEMENT"),
                        "department_name": read_ci(row, "NOM_DEPARTEMENT"),
                        "equipment_type": read_ci(row, "TYPE_EQUIPEMENT"),
                        "lat": lat,
                        "lon": lon,
                        "activities_count": len(activities),
                        "activities_sample": activities[:8],
                    }
                )

            activities_available = []
            for item in activity_rows:
                value = read_ci(item, "activity")
                if value is None:
                    continue
                text = str(value).strip()
                if text:
                    activities_available.append(text)

            columns = sorted(list(raw_rows[0].keys())) if raw_rows else []

            return jsonify({
                "success": True,
                "table": f"{SNOWFLAKE_SCHEMA_REF}.REF_STATIONS",
                "row_count": len(raw_rows),
                "requested_limit": limit_value,
                "columns": columns,
                "activity_count": len(activities_available),
                "activities_available": activities_available,
                "invalid_coordinate_rows": invalid_coords,
                "raw_sample": raw_rows[:5],
                "parsed_sample": parsed_preview[:10],
            }), 200
        finally:
            connection.close()

    except Exception as exc:
        import traceback
        return jsonify({
            "success": False,
            "error": str(exc),
            "error_type": type(exc).__name__,
            "traceback": traceback.format_exc(),
            "table": f"{SNOWFLAKE_SCHEMA_REF}.REF_STATIONS",
        }), 500


if __name__ == "__main__":
    app.run(debug=True)