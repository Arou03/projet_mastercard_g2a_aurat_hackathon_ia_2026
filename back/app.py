from flask import Flask, jsonify, request
from flask_cors import CORS
import traceback

from config import VALID_KPIS, CACHE_TTL_SECONDS, DEPARTMENT_KPIS, SNOWFLAKE_SCHEMA_PUBLIC, SNOWFLAKE_FACT_SCHEMA, SNOWFLAKE_DIM_SCHEMA, SNOWFLAKE_SCHEMA_REF
from database import get_last_error, set_last_error, is_snowflake_configured, get_missing_required_env_vars, get_required_env_vars, get_connection, fq_table
from utils.cache import get_cache, set_cache
from utils.helpers import parse_weeks_param, normalize_department_name, to_display_name
from services import kpi_service, holiday_service, geo_service

app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return "Flask API is running"

@app.route("/api/hello")
def hello():
    return jsonify({"message": "Hello from Flask"})

@app.route("/api/data")
def data():
    try:
        selected_kpi = request.args.get("kpi", "total_aura").lower().strip()
        selected_weeks = parse_weeks_param(request.args.get("weeks", ""))
        if selected_kpi not in VALID_KPIS:
            return jsonify({"error": "Invalid KPI", "valid_kpis": sorted(VALID_KPIS)}), 400

        dataset, dataset_hit, data_source, available_weeks = kpi_service.get_department_dataset(selected_weeks)
        weeks_key = ",".join(selected_weeks) if selected_weeks else "ALL"
        cache_key = f"api_data:{selected_kpi}:{weeks_key}"
        cached_payload, payload_hit = get_cache(cache_key)

        if payload_hit:
            payload = dict(cached_payload)
        else:
            departments = kpi_service.build_departments_payload(selected_kpi, dataset)
            scores = [item["score"] for item in departments]
            ranges = {"min": min(scores), "max": max(scores), "avg": round(sum(scores) / len(scores), 1)} if scores else {"min": 0, "max": 0, "avg": 0}
            payload = {"selected_kpi": selected_kpi, "kpis": sorted(VALID_KPIS), "weeks_selected": selected_weeks, "weeks_available": available_weeks, "departments": departments, "ranges": ranges}
            set_cache(cache_key, payload)

        payload["cache"] = {"dataset_hit": dataset_hit, "payload_hit": payload_hit, "ttl_seconds": CACHE_TTL_SECONDS}
        payload["data_source"] = data_source
        payload["snowflake_error"] = get_last_error()
        return jsonify(payload)
    except Exception as exc:
        set_last_error(f"api/data runtime error: {exc}")
        return jsonify({"error": get_last_error(), "data_source": "mock", "departments": kpi_service.build_departments_payload("total_aura", DEPARTMENT_KPIS)})

@app.route("/api/department/<dep_name>")
def department_data(dep_name):
    try:
        canonical_name = normalize_department_name(dep_name)
        dataset, dataset_hit, data_source, _ = kpi_service.get_department_dataset()
        if not canonical_name or canonical_name not in dataset:
            return jsonify({"error": f"Unknown department: {dep_name}"}), 404

        cache_key = f"api_department:{canonical_name}"
        cached_payload, payload_hit = get_cache(cache_key)
        if payload_hit:
            payload = dict(cached_payload)
        else:
            payload = {"name": to_display_name(canonical_name), "kpis": dataset[canonical_name], "insights": ["Pic de frequentation..."]}
            set_cache(cache_key, payload)

        payload["cache"] = {"dataset_hit": dataset_hit, "payload_hit": payload_hit, "ttl_seconds": CACHE_TTL_SECONDS}
        payload["data_source"] = data_source
        payload["snowflake_error"] = get_last_error()
        return jsonify(payload)
    except Exception as exc:
        set_last_error(f"api/department runtime error: {exc}")
        return jsonify({"error": get_last_error()}), 500

@app.route("/api/global/holidays")
def global_holidays_data():
    year = max(2024, int(request.args.get("year", "2024").strip() if request.args.get("year", "2024").isdigit() else 2024))
    cache_key = f"api_global_holidays:{year}"
    cached_payload, hit = get_cache(cache_key)
    if hit: return jsonify({**cached_payload, "cache": {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}})

    data_source = "mock"
    try:
        if is_snowflake_configured():
            payload = holiday_service.fetch_global_holidays_from_snowflake(year)
            payload["data_source"] = data_source = "snowflake"
        else:
            payload = holiday_service.build_mock_global_holidays(year)
            payload["data_source"] = "mock"
    except Exception as exc:
        set_last_error(str(exc))
        payload = holiday_service.build_mock_global_holidays(year)
        payload["data_source"] = "mock"

    payload["snowflake_error"] = get_last_error()
    set_cache(cache_key, payload)
    return jsonify({**payload, "cache": {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}})

@app.route("/api/department/<dep_name>/timeline")
def department_timeline(dep_name):
    canonical_name = normalize_department_name(dep_name)
    if not canonical_name: return jsonify({"error": f"Unknown department"}), 404
    year = max(2024, int(request.args.get("year", "2024").strip() if request.args.get("year", "2024").isdigit() else 2024))
    
    cache_key = f"api_timeline:{canonical_name}:{year}"
    cached_payload, hit = get_cache(cache_key)
    if hit: return jsonify({**cached_payload, "cache": {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}})

    data_source = "mock"
    try:
        if year == 2024 and is_snowflake_configured():
            timeline = kpi_service.fetch_department_timeline_from_snowflake(canonical_name)
            timeline["holidays"] = holiday_service.filter_winter_holidays(timeline.get("holidays", []))
            data_source = "snowflake"
        else:
            timeline = kpi_service.build_mock_department_timeline(canonical_name, year)
    except Exception as exc:
        set_last_error(str(exc))
        timeline = kpi_service.build_mock_department_timeline(canonical_name, year)

    payload = {"name": to_display_name(canonical_name), "year": year, "timeline": timeline, "data_source": data_source, "snowflake_error": get_last_error()}
    set_cache(cache_key, payload)
    return jsonify({**payload, "cache": {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}})

@app.route("/api/stations")
def stations_data():
    try:
        if not is_snowflake_configured():
            return jsonify({"error": "Snowflake not configured", "data_source": "error"}), 400
            
        selected_activities = [a.strip() for a in request.args.get("activities", "").split(",") if a.strip()]
        points, activities, _, source = geo_service.get_station_points(selected_activities)
        return jsonify({"activities_available": activities, "activities_selected": selected_activities, "points": points, "data_source": source, "count": len(points)})
    except Exception as exc:
        set_last_error(str(exc))
        return jsonify({"error": str(exc), "data_source": "error"}), 500

@app.route("/api/snowflake/status")
def snowflake_status():
    return jsonify({
        "configured": is_snowflake_configured(),
        "missing_env_vars": get_missing_required_env_vars(),
        "using_query": kpi_service.get_snowflake_query(),
        "schemas": {"public": SNOWFLAKE_SCHEMA_PUBLIC, "fact": SNOWFLAKE_FACT_SCHEMA, "dim": SNOWFLAKE_DIM_SCHEMA, "ref": SNOWFLAKE_SCHEMA_REF},
        "last_error": get_last_error()
    })

# /api/snowflake/test/freq-globale et /api/snowflake/test/ref-stations
# Restent très similaires à ton ancien code, appelant simplement get_connection() 
# et retournant jsonify(...). Pour garder la briéveté, tu peux les intégrer ici.

if __name__ == "__main__":
    app.run(debug=True)