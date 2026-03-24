import os
import joblib
import numpy as np

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