from flask import Flask, jsonify, request
from flask_cors import CORS
import base64
import os
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
    "Ain": {"frequentation": 72, "meteo": 68, "securite": 81},
    "Allier": {"frequentation": 54, "meteo": 64, "securite": 84},
    "Ardeche": {"frequentation": 66, "meteo": 74, "securite": 79},
    "Cantal": {"frequentation": 58, "meteo": 70, "securite": 88},
    "Drome": {"frequentation": 61, "meteo": 72, "securite": 80},
    "Haute-Loire": {"frequentation": 57, "meteo": 67, "securite": 86},
    "Haute-Savoie": {"frequentation": 95, "meteo": 83, "securite": 77},
    "Isere": {"frequentation": 88, "meteo": 79, "securite": 76},
    "Loire": {"frequentation": 62, "meteo": 63, "securite": 82},
    "Puy-de-Dome": {"frequentation": 69, "meteo": 66, "securite": 83},
    "Rhone": {"frequentation": 91, "meteo": 62, "securite": 71},
    "Savoie": {"frequentation": 92, "meteo": 81, "securite": 78},
}

ALIASES = {
    "Ardeche": "Ardèche",
    "Drome": "Drôme",
    "Isere": "Isère",
    "Puy-de-Dome": "Puy-de-Dôme",
    "Rhone": "Rhône",
}

VALID_KPIS = {"frequentation", "meteo", "securite"}
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
        "ain": "Ain",
        "allier": "Allier",
        "ardeche": "Ardeche",
        "ardèche": "Ardeche",
        "cantal": "Cantal",
        "drome": "Drome",
        "drôme": "Drome",
        "haute-loire": "Haute-Loire",
        "haute loire": "Haute-Loire",
        "haute-savoie": "Haute-Savoie",
        "haute savoie": "Haute-Savoie",
        "isere": "Isere",
        "isère": "Isere",
        "loire": "Loire",
        "puy-de-dome": "Puy-de-Dome",
        "puy de dome": "Puy-de-Dome",
        "puy-de-dôme": "Puy-de-Dome",
        "rhone": "Rhone",
        "rhône": "Rhone",
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


def get_snowflake_query():
    custom_query = os.getenv("SNOWFLAKE_KPI_QUERY", "").strip()
    if custom_query:
        return custom_query

    table_name = os.getenv("SNOWFLAKE_KPI_TABLE", "DEPARTMENT_KPIS").strip()
    return (
        "SELECT department_name, frequentation, meteo, securite "
        f"FROM {table_name}"
    )


def fetch_dataset_from_snowflake():
    query = get_snowflake_query()
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
            "frequentation": to_int(read_ci(row, "frequentation")),
            "meteo": to_int(read_ci(row, "meteo")),
            "securite": to_int(read_ci(row, "securite")),
        }

    return dataset


def build_departments_payload(selected_kpi, dataset):
    items = []
    for canonical_name, values in dataset.items():
        items.append(
            {
                "name": to_display_name(canonical_name),
                "frequentation": values["frequentation"],
                "meteo": values["meteo"],
                "securite": values["securite"],
                "score": values[selected_kpi],
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


def get_department_dataset():
    global last_snowflake_error

    dataset, hit = get_cache("dataset")
    if hit:
        return dataset, True, "cache"

    if is_snowflake_configured():
        try:
            dataset = fetch_dataset_from_snowflake()
            if dataset:
                set_cache("dataset", dataset)
                last_snowflake_error = None
                return dataset, False, "snowflake"
            last_snowflake_error = "Snowflake query returned no usable rows"
        except Exception as exc:
            last_snowflake_error = str(exc)

    # TODO Snowflake: remplacer par un SELECT et mapper les lignes vers ce format.
    dataset = DEPARTMENT_KPIS
    set_cache("dataset", dataset)
    return dataset, False, "mock"

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
    selected_kpi = request.args.get("kpi", "frequentation").lower().strip()
    if selected_kpi not in VALID_KPIS:
        return jsonify({"error": "Invalid KPI", "valid_kpis": sorted(VALID_KPIS)}), 400

    dataset, dataset_cache_hit, data_source = get_department_dataset()
    cache_key = f"api_data:{selected_kpi}"
    cached_payload, payload_cache_hit = get_cache(cache_key)

    if payload_cache_hit:
        payload = dict(cached_payload)
    else:
        departments = build_departments_payload(selected_kpi, dataset)
        scores = [item["score"] for item in departments]
        payload = {
            "selected_kpi": selected_kpi,
            "kpis": sorted(VALID_KPIS),
            "departments": departments,
            "ranges": {
                "min": min(scores),
                "max": max(scores),
                "avg": round(sum(scores) / len(scores), 1),
            },
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


@app.route("/api/department/<dep_name>")
def department_data(dep_name):
    canonical_name = normalize_department_name(dep_name)
    dataset, dataset_cache_hit, data_source = get_department_dataset()

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
                "Meteo stable sur les 7 derniers jours.",
                "Niveau de securite conforme au seuil regional.",
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


@app.route("/api/snowflake/status")
def snowflake_status():
    return jsonify(
        {
            "configured": is_snowflake_configured(),
            "using_query": get_snowflake_query(),
            "last_error": last_snowflake_error,
        }
    )

if __name__ == "__main__":
    app.run(debug=True)