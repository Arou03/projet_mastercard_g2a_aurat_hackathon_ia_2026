import os
import joblib
import numpy as np
from snowflake.connector import DictCursor

from config import SNOWFLAKE_FACT_SCHEMA, SNOWFLAKE_QUERY_TIMEOUT_SECONDS
from database import get_connection, fq_table
from utils.helpers import read_ci

# Chemin absolu vers le modèle pour éviter les soucis de chemins relatifs
# Assure-toi que le dossier 'models' est à la racine de ton projet
MODEL_PATH_FREQ = os.path.join(os.path.dirname(__file__), '..', 'models', 'huber_model_freq.joblib')
MODEL_PATH_EXPENSES = os.path.join(os.path.dirname(__file__), '..', 'models', 'expenses_intl_bundle.joblib')

# Variables globales pour cacher les artefacts ML en mémoire
_ml_artifacts_freq = None
_ml_artifacts_expenses = None

# La liste stricte des features extraite de ton fichier joblib
# L'ordre DOIT être respecté scrupuleusement pour que Numpy fonctionne bien.
EXPECTED_FEATURES_FREQ = [
    "AVG_PRECIPITATIONS_MM",
    "AVG_PRECIPITATIONS_DUREE_MN",
    "AVG_TEMP_MIN",
    "AVG_TEMP_MAX",
    "AVG_TEMP_MOYENNE",
    "AVG_TEMP_AMPLITUDE",
    "AVG_VENT_MOYEN",
    "AVG_VENT_RAFALE_MAX",
    "week_of_year",
    "month",
    "lag_1",
    "lag_2",
    "rolling_mean_3",
    "pct_change",
    "DIST_HOLIDAY_BEL",
    "DIST_HOLIDAY_DEU",
    "DIST_HOLIDAY_FRA",
    "DIST_HOLIDAY_GBR",
    "DIST_HOLIDAY_NLD",
    "DIST_HOLIDAY_POL",
    "DIST_HOLIDAY_SCAN",
    "DIST_HOLIDAY_USA"
]

EXPECTED_FEATURES_EXPENSES = [
    "WEEK_OF_YEAR",
    "MONTH",
    "JOURS_ANTICIPATION",
    "PAYS_AVG_DEPENSES",
    "PAYS_STD_DEPENSES",
    "HAS_HOLIDAY",
    "LAG_1",
    "LAG_2",
    "ROLLING_MEAN_3",
    "PCT_CHANGE"
]


def load_artifacts_freq():
    """Charge le dictionnaire contenant le scaler et le modèle de fréquentation."""
    global _ml_artifacts_freq
    if _ml_artifacts_freq is None:
        if not os.path.exists(MODEL_PATH_FREQ):
            raise FileNotFoundError(f"Fichier modèle introuvable au chemin : {MODEL_PATH_FREQ}")
        _ml_artifacts_freq = joblib.load(MODEL_PATH_FREQ)
    return _ml_artifacts_freq


def load_artifacts_expenses():
    """Charge le dictionnaire contenant le scaler et le modèle de dépenses internationales."""
    global _ml_artifacts_expenses
    if _ml_artifacts_expenses is None:
        if not os.path.exists(MODEL_PATH_EXPENSES):
            raise FileNotFoundError(f"Fichier modèle introuvable au chemin : {MODEL_PATH_EXPENSES}")
        _ml_artifacts_expenses = joblib.load(MODEL_PATH_EXPENSES)
    return _ml_artifacts_expenses


def _predict_with_model(artifacts, expected_features, input_data):
    """Fonction utilitaire : construit le vecteur de features, scale et prédit."""
    model = artifacts.get('model')
    scaler = artifacts.get('scaler')

    if not model or not scaler:
        raise ValueError("Le fichier joblib ne contient pas de clé 'model' ou 'scaler' valide.")

    feature_values = [float(input_data.get(f, 0.0)) for f in expected_features]
    X = np.array([feature_values])
    X_scaled = scaler.transform(X)
    prediction = model.predict(X_scaled)
    return float(prediction[0])


def predict(input_data):
    """
    Prend un dictionnaire de données en entrée (venant du front),
    le formate en numpy array, applique le scaler, et retourne la prédiction de fréquentation.
    """
    artifacts = load_artifacts_freq()
    return _predict_with_model(artifacts, EXPECTED_FEATURES_FREQ, input_data)


def predict_expenses(input_data):
    """
    Prend un dictionnaire de données en entrée (venant du front),
    le formate en numpy array, applique le scaler, et retourne la prédiction de dépenses internationales.
    """
    artifacts = load_artifacts_expenses()
    return _predict_with_model(artifacts, EXPECTED_FEATURES_EXPENSES, input_data)


def _find_column(column_names, candidates):
    upper_columns = {str(name).upper(): str(name) for name in (column_names or [])}
    for candidate in candidates:
        if candidate.upper() in upper_columns:
            return upper_columns[candidate.upper()]
    return None


def _to_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _to_int(value, default=0):
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return int(default)


def _guess_month_from_week(week_of_year):
    week = max(1, min(52, _to_int(week_of_year, 1)))
    month = ((week - 1) // 4) + 1
    return max(1, min(12, month))


def fetch_expenses_features(country_code, week_of_year, season=None, month=None):
    """
    Fetches a feature row for expenses prediction from ML_EXPENSES_FEATURES.
    Uses best-effort column discovery to handle naming differences.
    """
    week_of_year = _to_int(week_of_year, 1)
    country_code = str(country_code or "").strip().upper()
    season = str(season or "").strip()

    if not country_code:
        raise ValueError("country_code is required")

    if week_of_year < 1 or week_of_year > 52:
        raise ValueError("week_of_year must be between 1 and 52")

    tables_to_try = [
        fq_table(SNOWFLAKE_FACT_SCHEMA, "ML_EXPENSES_FEATURES"),
        fq_table("PUBLIC", "ML_EXPENSES_FEATURES"),
    ]

    try:
        connection = get_connection("aura_expenses_features")
    except Exception as conn_err:
        from database import set_last_error
        set_last_error(f"Expenses features connection failed: {str(conn_err)}")
        raise LookupError(f"Snowflake unavailable: {str(conn_err)}")

    try:
        with connection.cursor(DictCursor) as cursor:
            for table_name in tables_to_try:
                try:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
                    column_names = [desc[0] for desc in (cursor.description or [])]
                except Exception:
                    continue

                country_col = _find_column(column_names, ["CODE_PAYS", "COUNTRY_CODE", "PAYS", "PAYS_ORIGINE", "ORIGIN_COUNTRY", "COUNTRY"])
                week_col = _find_column(column_names, ["WEEK_OF_YEAR", "WEEK", "SEMAINE", "WEEK_NUM"])
                season_col = _find_column(column_names, ["SAISON", "SEASON"])
                month_col = _find_column(column_names, ["MONTH", "MOIS"])

                if not country_col or not week_col:
                    continue

                where_clauses = [f'"{country_col}" = %s', f'"{week_col}" = %s']
                params = [country_code, week_of_year]

                if season and season_col:
                    where_clauses.append(f'"{season_col}" = %s')
                    params.append(season)
                if month is not None and month_col:
                    where_clauses.append(f'"{month_col}" = %s')
                    params.append(_to_int(month))

                query = f"SELECT * FROM {table_name} WHERE {' AND '.join(where_clauses)} LIMIT 1"
                cursor.execute(query, tuple(params), timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
                row = cursor.fetchone()

                # Fallback without season/month if strict filter returns no row.
                if not row:
                    fallback_clauses = [f'"{country_col}" = %s', f'"{week_col}" = %s']
                    fallback_params = [country_code, week_of_year]
                    fallback_query = f"SELECT * FROM {table_name} WHERE {' AND '.join(fallback_clauses)} LIMIT 1"
                    cursor.execute(fallback_query, tuple(fallback_params), timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
                    row = cursor.fetchone()

                if not row:
                    continue

                resolved_month = _to_int(read_ci(row, "MONTH"), _guess_month_from_week(week_of_year))
                features = {
                    "WEEK_OF_YEAR": _to_int(read_ci(row, "WEEK_OF_YEAR"), week_of_year),
                    "MONTH": _to_int(read_ci(row, "MONTH"), resolved_month),
                    "JOURS_ANTICIPATION": _to_float(read_ci(row, "JOURS_ANTICIPATION"), 0.0),
                    "PAYS_AVG_DEPENSES": _to_float(read_ci(row, "PAYS_AVG_DEPENSES"), 0.0),
                    "PAYS_STD_DEPENSES": _to_float(read_ci(row, "PAYS_STD_DEPENSES"), 0.0),
                    "HAS_HOLIDAY": _to_int(read_ci(row, "HAS_HOLIDAY"), 0),
                    "LAG_1": _to_float(read_ci(row, "LAG_1"), 0.0),
                    "LAG_2": _to_float(read_ci(row, "LAG_2"), 0.0),
                    "ROLLING_MEAN_3": _to_float(read_ci(row, "ROLLING_MEAN_3"), 0.0),
                    "PCT_CHANGE": _to_float(read_ci(row, "PCT_CHANGE"), 0.0),
                }

                return {
                    "features": features,
                    "source_table": table_name,
                    "matched_country": country_code,
                    "matched_week": week_of_year,
                    "matched_season": season,
                }
    finally:
        connection.close()

    raise LookupError("No feature row found in ML_EXPENSES_FEATURES for the provided context")


def predict_expenses_from_context(country_code, week_of_year, season=None, month=None, overrides=None):
    context = fetch_expenses_features(country_code, week_of_year, season=season, month=month)
    features = dict(context["features"])

    for key, value in (overrides or {}).items():
        if key in EXPECTED_FEATURES_EXPENSES:
            features[key] = _to_float(value, features.get(key, 0.0))

    prediction = predict_expenses(features)

    return {
        "prediction": prediction,
        "features_used": features,
        "feature_source": {
            "table": context.get("source_table"),
            "country": context.get("matched_country"),
            "week_of_year": context.get("matched_week"),
            "season": context.get("matched_season"),
        },
    }