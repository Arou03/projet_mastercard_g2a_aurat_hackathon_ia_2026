from flask import Flask, jsonify, request
from flask_cors import CORS
import traceback
import os

# Imports de notre nouvelle architecture modulaire
from config import VALID_KPIS, CACHE_TTL_SECONDS, DEPARTMENT_KPIS, SNOWFLAKE_SCHEMA_PUBLIC, SNOWFLAKE_FACT_SCHEMA, SNOWFLAKE_DIM_SCHEMA, SNOWFLAKE_SCHEMA_REF, SNOWFLAKE_QUERY_TIMEOUT_SECONDS
from database import get_last_error, set_last_error, is_snowflake_configured, get_missing_required_env_vars, get_required_env_vars, get_connection, fq_table
from snowflake.connector import DictCursor
from utils.cache import get_cache, set_cache
from utils.helpers import parse_weeks_param, normalize_department_name, to_display_name

# Imports de nos services métiers
from services import kpi_service, holiday_service, geo_service, ml_service

app = Flask(__name__)
CORS(app)  # Autorise les requêtes du front

@app.route("/")
def home():
    return "Flask API is running"

@app.route("/api/hello")
def hello():
    return jsonify({"message": "Hello from Flask"})

# --- ROUTES KPI ET DÉPARTEMENTS ---

@app.route("/api/data")
def data():
    try:
        selected_kpi = request.args.get("kpi", "total_aura").lower().strip()
        selected_weeks = parse_weeks_param(request.args.get("weeks", ""))
        selected_year = max(2024, int(request.args.get("year", "2024").strip() if request.args.get("year", "2024").isdigit() else 2024))
        
        if selected_kpi not in VALID_KPIS:
            return jsonify({"error": "Invalid KPI", "valid_kpis": sorted(VALID_KPIS)}), 400

        dataset, dataset_hit, data_source, available_weeks = kpi_service.get_department_dataset(selected_weeks, selected_year)
        
        weeks_key = ",".join(selected_weeks) if selected_weeks else "ALL"
        cache_key = f"api_data:{selected_kpi}:{selected_year}:{weeks_key}"
        cached_payload, payload_hit = get_cache(cache_key)

        if payload_hit:
            payload = dict(cached_payload)
        else:
            departments = kpi_service.build_departments_payload(selected_kpi, dataset)
            scores = [item["score"] for item in departments]
            ranges = {"min": min(scores), "max": max(scores), "avg": round(sum(scores) / len(scores), 1)} if scores else {"min": 0, "max": 0, "avg": 0}
            
            payload = {
                "selected_kpi": selected_kpi, 
                "selected_year": selected_year,
                "kpis": sorted(VALID_KPIS), 
                "weeks_selected": selected_weeks, 
                "weeks_available": available_weeks, 
                "departments": departments, 
                "ranges": ranges
            }
            set_cache(cache_key, payload)

        payload["cache"] = {"dataset_hit": dataset_hit, "payload_hit": payload_hit, "ttl_seconds": CACHE_TTL_SECONDS}
        payload["data_source"] = data_source
        payload["snowflake_error"] = get_last_error()
        
        return jsonify(payload)
        
    except Exception as exc:
        set_last_error(f"api/data runtime error: {exc}")
        return jsonify({
            "error": get_last_error(), 
            "data_source": "mock", 
            "departments": kpi_service.build_departments_payload("total_aura", DEPARTMENT_KPIS)
        })

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
            payload = {
                "name": to_display_name(canonical_name), 
                "kpis": dataset[canonical_name], 
                "insights": ["Pic de frequentation pendant la saison hivernale.", "Niveau global de frequentation observe sur la periode selectionnee."]
            }
            set_cache(cache_key, payload)

        payload["cache"] = {"dataset_hit": dataset_hit, "payload_hit": payload_hit, "ttl_seconds": CACHE_TTL_SECONDS}
        payload["data_source"] = data_source
        payload["snowflake_error"] = get_last_error()
        
        return jsonify(payload)
        
    except Exception as exc:
        set_last_error(f"api/department runtime error: {exc}")
        return jsonify({"error": get_last_error()}), 500

@app.route("/api/department/<dep_name>/timeline")
def department_timeline(dep_name):
    canonical_name = normalize_department_name(dep_name)
    if not canonical_name: 
        return jsonify({"error": f"Unknown department: {dep_name}"}), 404
        
    year = max(2024, int(request.args.get("year", "2024").strip() if request.args.get("year", "2024").isdigit() else 2024))
    
    cache_key = f"api_timeline:{canonical_name}:{year}"
    cached_payload, hit = get_cache(cache_key)
    if hit: 
        return jsonify({**cached_payload, "cache": {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}})

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

    # Apply growth factor for prediction years
    if year > 2024 and "frequentation" in timeline:
        growth_factor = 1.0 + (0.05 * (year - 2024))
        freq_data = timeline["frequentation"]
        if freq_data.get("values_observed"):
            freq_data["values_predicted"] = [
                int(v * growth_factor) if v is not None else None 
                for v in freq_data["values_observed"]
            ]
        freq_data["year"] = year

    payload = {
        "name": to_display_name(canonical_name), 
        "year": year, 
        "timeline": timeline, 
        "data_source": data_source, 
        "snowflake_error": get_last_error()
    }
    set_cache(cache_key, payload)
    
    return jsonify({**payload, "cache": {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}})

# --- ROUTES VACANCES ET STATIONS ---

@app.route("/api/global/holidays")
def global_holidays_data():
    year = max(2024, int(request.args.get("year", "2024").strip() if request.args.get("year", "2024").isdigit() else 2024))
    
    cache_key = f"api_global_holidays:{year}"
    cached_payload, hit = get_cache(cache_key)
    if hit: 
        return jsonify({**cached_payload, "cache": {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}})

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


@app.route("/api/global/frequentation")
def global_frequentation_data():
    year = max(2024, int(request.args.get("year", "2024").strip() if request.args.get("year", "2024").isdigit() else 2024))
    years_param = request.args.get("years", "").strip()
    requested_years = []
    if years_param:
        for token in years_param.split(","):
            candidate = token.strip()
            if candidate.isdigit():
                requested_years.append(max(2024, int(candidate)))
    if not requested_years:
        requested_years = [year]

    ml_params = {}
    for key, value in request.args.items():
        if key.startswith("ml_param_"):
            param_name = key[len("ml_param_"):].strip()
            if param_name:
                ml_params[param_name] = value

    selected_weeks = parse_weeks_param(request.args.get("weeks", ""))
    years_key = ",".join(str(y) for y in sorted(set(requested_years)))
    weeks_key = ",".join(selected_weeks) if selected_weeks else "ALL"
    params_key = ";".join(f"{k}={ml_params[k]}" for k in sorted(ml_params.keys())) if ml_params else "NONE"

    cache_key = f"api_global_frequentation:{years_key}:{weeks_key}:{params_key}"
    cached_payload, hit = get_cache(cache_key)
    if hit:
        return jsonify({**cached_payload, "cache": {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}})

    if not is_snowflake_configured():
        return jsonify({
            "error": "Snowflake non configure: impossible de fournir des donnees reelles pour /api/global/frequentation",
            "data_source": "error",
            "missing_env_vars": get_missing_required_env_vars(),
        }), 503

    try:
        base_data = kpi_service.fetch_global_frequentation_from_snowflake(2024, selected_weeks)
        weeks = base_data.get("weeks", [])
        values = base_data.get("values", [])
        if not weeks or not values:
            return jsonify({
                "error": "Aucune donnee reelle disponible depuis Snowflake pour /api/global/frequentation",
                "data_source": "error",
            }), 503

        payload = kpi_service.build_global_frequentation_multi_from_base(
            weeks,
            values,
            requested_years,
            ml_params,
        )
        data_source = "snowflake" if all(y == 2024 for y in requested_years) else "ml_prediction"
    except Exception as exc:
        set_last_error(str(exc))
        return jsonify({
            "error": "Echec lecture Snowflake pour /api/global/frequentation",
            "details": str(exc),
            "data_source": "error",
        }), 503

    payload["selected_year"] = year
    payload["data_source"] = data_source
    payload["snowflake_error"] = get_last_error()
    set_cache(cache_key, payload)

    return jsonify({**payload, "cache": {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}})

@app.route("/api/stations")
def stations_data():
    try:
        if not is_snowflake_configured():
            return jsonify({"error": "Snowflake not configured", "data_source": "error"}), 400
            
        selected_activities = [a.strip() for a in request.args.get("activities", "").split(",") if a.strip()]
        points, activities, _, source = geo_service.get_station_points(selected_activities)
        
        return jsonify({
            "activities_available": activities, 
            "activities_selected": selected_activities, 
            "points": points, 
            "data_source": source, 
            "count": len(points)
        })
    except Exception as exc:
        set_last_error(str(exc))
        return jsonify({"error": str(exc), "data_source": "error"}), 500

# --- ROUTE MACHINE LEARNING ---

@app.route("/api/predict", methods=["POST"])
def predict_endpoint():
    """
    Endpoint POST pour l'inférence du modèle Huber.
    Attendu : un JSON contenant les 22 features requises.
    """
    try:
        input_data = request.get_json()
        
        if not input_data:
            return jsonify({
                "success": False, 
                "error": "Aucune donnée JSON fournie dans le corps de la requête"
            }), 400
            
        # Délégation du calcul au service ML
        predicted_value = ml_service.predict(input_data)
        
        return jsonify({
            "success": True,
            "prediction": predicted_value
        }), 200
        
    except FileNotFoundError as exc:
        return jsonify({
            "success": False, 
            "error": "Le modèle ML est introuvable sur le serveur.",
            "details": str(exc)
        }), 500
        
    except ValueError as exc:
        return jsonify({
            "success": False, 
            "error": "Erreur de format de données ou de features manquantes.",
            "details": str(exc)
        }), 400
        
    except Exception as exc:
        return jsonify({
            "success": False,
            "error": "Une erreur inattendue est survenue lors de la prédiction.",
            "details": str(exc),
            "traceback": traceback.format_exc()
        }), 500

@app.route("/api/predict/expenses", methods=["POST"])
def predict_expenses_endpoint():
    """
    Endpoint POST pour l'inférence du modèle de dépenses internationales.
    Attendu : un JSON contenant les 10 features requises.
    """
    try:
        input_data = request.get_json()

        if not input_data:
            return jsonify({
                "success": False,
                "error": "Aucune donnée JSON fournie dans le corps de la requête"
            }), 400

        predicted_value = ml_service.predict_expenses(input_data)

        return jsonify({
            "success": True,
            "prediction": predicted_value
        }), 200

    except FileNotFoundError as exc:
        return jsonify({
            "success": False,
            "error": "Le modèle ML est introuvable sur le serveur.",
            "details": str(exc)
        }), 500

    except ValueError as exc:
        return jsonify({
            "success": False,
            "error": "Erreur de format de données ou de features manquantes.",
            "details": str(exc)
        }), 400

    except Exception as exc:
        return jsonify({
            "success": False,
            "error": "Une erreur inattendue est survenue lors de la prédiction.",
            "details": str(exc),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/api/predict/expenses/context", methods=["GET"])
def predict_expenses_context_endpoint():
    """
    Endpoint GET pour prédire les dépenses à partir du contexte métier.
    Entrées attendues (query params):
      - country / pays / code_pays
      - week / week_of_year
      - season (optionnel)
      - month (optionnel)
      - feat_<FEATURE_NAME> (optionnel) pour override des features du modèle
    """
    try:
        country_code = (
            request.args.get("country")
            or request.args.get("pays")
            or request.args.get("code_pays")
            or ""
        ).strip().upper()

        week_value = (
            request.args.get("week")
            or request.args.get("week_of_year")
            or ""
        ).strip()

        season = (request.args.get("season") or request.args.get("saison") or "").strip()
        month_value = (request.args.get("month") or request.args.get("mois") or "").strip()

        if not country_code:
            return jsonify({
                "success": False,
                "error": "Paramètre manquant: country (ou pays / code_pays)"
            }), 400

        if not week_value or not week_value.isdigit():
            return jsonify({
                "success": False,
                "error": "Paramètre invalide: week (ou week_of_year) doit être un entier 1..52"
            }), 400

        week_of_year = int(week_value)
        if week_of_year < 1 or week_of_year > 52:
            return jsonify({
                "success": False,
                "error": "week_of_year doit être compris entre 1 et 52"
            }), 400

        month = int(month_value) if month_value.isdigit() else None

        overrides = {}
        for key, value in request.args.items():
            if key.startswith("feat_"):
                feature_name = key[len("feat_"):].strip().upper()
                if feature_name:
                    overrides[feature_name] = value

        cache_key = f"api_predict_expenses_ctx:{country_code}:{week_of_year}:{season or 'NONE'}:{month if month is not None else 'NONE'}:{';'.join([f'{k}={overrides[k]}' for k in sorted(overrides.keys())]) if overrides else 'NO_OVERRIDES'}"
        cached_payload, hit = get_cache(cache_key)
        if hit:
            return jsonify({**cached_payload, "cache": {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}})

        result = ml_service.predict_expenses_from_context(
            country_code=country_code,
            week_of_year=week_of_year,
            season=season or None,
            month=month,
            overrides=overrides,
        )

        payload = {
            "success": True,
            "context": {
                "country": country_code,
                "week_of_year": week_of_year,
                "season": season or None,
                "month": month,
            },
            "prediction": result["prediction"],
            "features_used": result["features_used"],
            "feature_source": result["feature_source"],
            "overrides": overrides,
            "data_source": "snowflake",
        }
        set_cache(cache_key, payload)

        return jsonify({**payload, "cache": {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}}), 200

    except FileNotFoundError as exc:
        return jsonify({
            "success": False,
            "error": "Le modèle ML des dépenses est introuvable.",
            "details": str(exc)
        }), 500
    except LookupError as exc:
        return jsonify({
            "success": False,
            "error": "Aucune ligne de features trouvée pour ce contexte.",
            "details": str(exc)
        }), 404
    except ValueError as exc:
        return jsonify({
            "success": False,
            "error": "Contexte ou features invalides.",
            "details": str(exc)
        }), 400
    except Exception as exc:
        return jsonify({
            "success": False,
            "error": "Erreur inattendue lors de la prédiction contextuelle des dépenses.",
            "details": str(exc),
            "traceback": traceback.format_exc()
        }), 500


@app.route("/api/expenses/countries", methods=["GET"])
def expenses_countries_endpoint():
    cache_key = "api_expenses_countries"
    cached_payload, hit = get_cache(cache_key)
    if hit:
        return jsonify({**cached_payload, "cache": {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}})

    try:
        result = ml_service.list_expenses_countries()
        countries = result.get("countries") or []
        payload = {
            "success": True,
            "countries": countries,
            "source_table": result.get("source_table"),
            "data_source": result.get("data_source", "snowflake"),
        }
        set_cache(cache_key, payload)
        return jsonify({**payload, "cache": {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}}), 200
    except Exception as exc:
        return jsonify({
            "success": False,
            "error": "Impossible de récupérer la liste des pays.",
            "details": str(exc),
        }), 500


@app.route("/api/predict/expenses/series", methods=["GET"])
def predict_expenses_series_endpoint():
    try:
        country_code = (
            request.args.get("country")
            or request.args.get("pays")
            or request.args.get("code_pays")
            or ""
        ).strip().upper()
        if not country_code:
            return jsonify({"success": False, "error": "Paramètre manquant: country"}), 400

        season = (request.args.get("season") or request.args.get("saison") or "").strip()
        year_value = (request.args.get("year") or "").strip()
        year = int(year_value) if year_value.isdigit() else None
        selected_weeks = parse_weeks_param(request.args.get("weeks", ""))

        overrides = {}
        for key, value in request.args.items():
            if key.startswith("feat_"):
                feature_name = key[len("feat_"):].strip().upper()
                if feature_name:
                    overrides[feature_name] = value

        cache_key = (
            f"api_predict_expenses_series:{country_code}:{season or 'NONE'}:{year if year is not None else 'NONE'}:"
            f"{','.join(selected_weeks) if selected_weeks else 'ALL'}:"
            f"{';'.join([f'{k}={overrides[k]}' for k in sorted(overrides.keys())]) if overrides else 'NO_OVERRIDES'}"
        )
        cached_payload, hit = get_cache(cache_key)
        if hit:
            return jsonify({**cached_payload, "cache": {"payload_hit": True, "ttl_seconds": CACHE_TTL_SECONDS}}), 200

        result = ml_service.predict_expenses_series(
            country_code=country_code,
            season=season or None,
            year=year,
            selected_weeks=selected_weeks,
            overrides=overrides,
        )

        payload = {
            "success": True,
            "country": country_code,
            "season": season or None,
            "year": year,
            "weeks": result.get("weeks", []),
            "predictions": result.get("predictions", []),
            "points": result.get("points", []),
            "source_table": result.get("source_table"),
            "overrides": overrides,
            "data_source": result.get("data_source", "ml_prediction"),
        }
        set_cache(cache_key, payload)
        return jsonify({**payload, "cache": {"payload_hit": False, "ttl_seconds": CACHE_TTL_SECONDS}}), 200

    except LookupError as exc:
        return jsonify({
            "success": False,
            "error": "Aucune série disponible pour ce contexte.",
            "details": str(exc),
        }), 404
    except Exception as exc:
        return jsonify({
            "success": False,
            "error": "Erreur inattendue lors de la prédiction des dépenses par série.",
            "details": str(exc),
            "traceback": traceback.format_exc(),
        }), 500

# --- ROUTES DE DIAGNOSTIC SNOWFLAKE ---

@app.route("/api/snowflake/status")
def snowflake_status():
    return jsonify({
        "configured": is_snowflake_configured(),
        "missing_env_vars": get_missing_required_env_vars(),
        "using_query": kpi_service.get_snowflake_query(),
        "schemas": {
            "public": SNOWFLAKE_SCHEMA_PUBLIC, 
            "fact": SNOWFLAKE_FACT_SCHEMA, 
            "dim": SNOWFLAKE_DIM_SCHEMA, 
            "ref": SNOWFLAKE_SCHEMA_REF
        },
        "last_error": get_last_error()
    })

@app.route("/api/snowflake/test/freq-globale")
def snowflake_test_freq_globale():
    """Test endpoint pour valider la connexion Snowflake et la table FREQ_GLOBAL_PER_DEPT"""
    missing_env = get_missing_required_env_vars()
    if missing_env:
        return jsonify({
            "success": False,
            "error": "Snowflake non configure",
            "error_type": "CONFIGURATION_ERROR",
            "missing_env_vars": missing_env
        }), 400
    
    connection = None
    try:
        connection = get_connection("aura_snowflake_test")
        database_name = (os.getenv("SNOWFLAKE_DATABASE") or "").strip()
        schema_name = SNOWFLAKE_FACT_SCHEMA
        expected_table = "FREQ_GLOBAL_PER_DEPT"
        
        with connection.cursor(DictCursor) as cursor:
            escaped_database = database_name.replace('"', '""')
            info_schema_table = f'"{escaped_database}".INFORMATION_SCHEMA.TABLES' if escaped_database else "INFORMATION_SCHEMA.TABLES"
            cursor.execute(
                f"SELECT TABLE_NAME FROM {info_schema_table} WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME",
                (schema_name.upper(),),
                timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS,
            )
            tables_result = cursor.fetchall() or []
            available_tables = [str(t.get("TABLE_NAME") or t.get("table_name") or "") for t in tables_result]

            if expected_table not in {name.upper() for name in available_tables}:
                return jsonify({
                    "success": False,
                    "error": f"Table introuvable: {expected_table}",
                    "error_type": "OBJECT_NOT_FOUND",
                    "diagnostic": {
                        "database": database_name,
                        "schema": schema_name,
                        "expected_table": expected_table,
                        "available_tables": available_tables,
                    },
                }), 404

            fact_table = fq_table(schema_name, expected_table)

            cursor.execute(f"SELECT COUNT(*) as row_count FROM {fact_table}", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            row_count_result = cursor.fetchone()
            freq_globale_row_count = row_count_result.get("ROW_COUNT", 0) if row_count_result else 0

            cursor.execute(f"SELECT * FROM {fact_table} LIMIT 1", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            freq_globale_columns = [desc[0] for desc in cursor.description] if cursor.description else []

            cursor.execute(f"SELECT * FROM {fact_table} LIMIT 5", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            freq_globale_sample = cursor.fetchall()

        set_last_error(None)
        
        return jsonify({
            "success": True,
            "connection_details": {
                "account": os.getenv("SNOWFLAKE_ACCOUNT", "N/A"),
                "user": os.getenv("SNOWFLAKE_USER", "N/A"),
                "database": os.getenv("SNOWFLAKE_DATABASE", "N/A"),
                "schema": schema_name
            },
            "freq_globale_row_count": freq_globale_row_count,
            "freq_globale_columns": freq_globale_columns,
            "freq_globale_sample": freq_globale_sample,
            "available_tables": available_tables
        })
    
    except Exception as exc:
        message = str(exc)
        set_last_error(message)

        error_type = "UNKNOWN_ERROR"
        if "Object does not exist" in message:
            error_type = "OBJECT_NOT_FOUND"
        elif "authentication" in message.lower() or "private key" in message.lower():
            error_type = "AUTHENTICATION_ERROR"
        elif "Database" in message or "Schema" in message:
            error_type = "CONFIGURATION_ERROR"

        return jsonify({
            "success": False,
            "error": message,
            "error_type": error_type,
            "snowflake_error": get_last_error(),
            "missing_env_vars": get_missing_required_env_vars(),
            "diagnostic": {
                "database": os.getenv("SNOWFLAKE_DATABASE", ""),
                "schema": SNOWFLAKE_FACT_SCHEMA,
                "expected_table": "FREQ_GLOBAL_PER_DEPT",
            },
        }), 500
    finally:
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass

if __name__ == "__main__":
    app.run(debug=True)