from flask import Flask, jsonify, request
from flask_cors import CORS
import base64
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

VALID_KPIS = {"total_aura", "rural", "urbain", "stations_montagne", "villages_montagne"}
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# Cache en memoire process-local.
# Sur Render, ce cache vit par instance et se reconstruit au redemarrage/scale.
api_cache = {}
last_snowflake_error = None


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
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_PRIVATE_KEY_B64",
    ]
    return all(os.getenv(name) for name in required)


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


def week_sort_key(week_value):
    text = str(week_value or "").upper()
    if text.startswith("S") and text[1:].isdigit():
        return int(text[1:])
    return 999


def get_snowflake_query():
    custom_query = os.getenv("SNOWFLAKE_KPI_QUERY", "").strip()
    if custom_query:
        return custom_query

    # Lire FREQ_GLOBAL_PER_DEPT et mapper les colonnes
    return (
        "SELECT CODE_DEPARTEMENT as department_name, "
        "WEEK as week, "
        "CAST(TOTAL_AURA as INTEGER) as frequentation "
        "FROM FREQ_GLOBAL_PER_DEPT "
        "ORDER BY CODE_DEPARTEMENT"
    )


def fetch_available_weeks_from_snowflake(connection):
    with connection.cursor(DictCursor) as cursor:
        cursor.execute("SELECT DISTINCT WEEK FROM FREQ_GLOBAL_PER_DEPT WHERE WEEK IS NOT NULL")
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

    private_key = load_private_key_der_bytes()

    connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        private_key=private_key,
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
            "CODE_DEPARTEMENT as department_name, "
            "SUM(TOTAL_AURA) as total_aura, "
            "SUM(RURAL) as rural, "
            "SUM(URBAIN) as urbain, "
            "SUM(STATIONS_MONTAGNE) as stations_montagne, "
            "SUM(VILLAGES_MONTAGNE) as villages_montagne "
            "FROM FREQ_GLOBAL_PER_DEPT "
            f"{where_clause} "
            "GROUP BY CODE_DEPARTEMENT "
            "ORDER BY CODE_DEPARTEMENT"
        )

        with connection.cursor(DictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    finally:
        connection.close()

    dataset = {}
    for row in rows:
        raw_name = read_ci(row, "department_name")
        canonical_name = normalize_department_name(str(raw_name or ""))
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


@app.route("/api/snowflake/status")
def snowflake_status():
    return jsonify(
        {
            "configured": is_snowflake_configured(),
            "using_query": get_snowflake_query(),
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
            "required_env_vars": [
                "SNOWFLAKE_ACCOUNT",
                "SNOWFLAKE_USER",
                "SNOWFLAKE_ROLE",
                "SNOWFLAKE_WAREHOUSE",
                "SNOWFLAKE_DATABASE",
                "SNOWFLAKE_SCHEMA",
                "SNOWFLAKE_PRIVATE_KEY_B64",
            ]
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
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            private_key=private_key,
            session_parameters={"QUERY_TAG": "aura_dashboard_test"},
        )

        print("[DEBUG] Connexion etablie avec succes!")

        # Test 1: Lister les tables disponibles
        print("[DEBUG] Recuperation des tables disponibles...")
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            table_names = [row[1] for row in tables]  # row[1] est le nom de la table

        print(f"[DEBUG] Tables trouvees: {table_names}")

        # Test 2: Interroger FREQ_GLOBAL_PER_DEPT
        print("[DEBUG] Interrogation de FREQ_GLOBAL_PER_DEPT...")
        with connection.cursor(DictCursor) as cursor:
            cursor.execute("SELECT * FROM FREQ_GLOBAL_PER_DEPT LIMIT 100;")
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
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
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


if __name__ == "__main__":
    app.run(debug=True)