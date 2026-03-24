import os
import joblib
import numpy as np

# Chemin absolu vers le modèle pour éviter les soucis de chemins relatifs
# Assure-toi que le dossier 'models' est à la racine de ton projet
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'models', 'huber_model_freq.joblib')

# Variable globale pour cacher les artefacts ML en mémoire
_ml_artifacts = None

# La liste stricte des features extraite de ton fichier joblib
# L'ordre DOIT être respecté scrupuleusement pour que Numpy fonctionne bien.
EXPECTED_FEATURES = [
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

def load_artifacts():
    """Charge le dictionnaire contenant le scaler et le modèle en mémoire."""
    global _ml_artifacts
    if _ml_artifacts is None:
        if not os.path.exists(MODEL_PATH):
            raise FileNotFoundError(f"Fichier modèle introuvable au chemin : {MODEL_PATH}")
        
        # Le fichier joblib contient un dictionnaire avec 'model', 'scaler', 'features'
        _ml_artifacts = joblib.load(MODEL_PATH)
        
    return _ml_artifacts

def predict(input_data):
    """
    Prend un dictionnaire de données en entrée (venant du front), 
    le formate en numpy array, applique le scaler, et retourne la prédiction.
    """
    artifacts = load_artifacts()
    
    model = artifacts.get('model')
    scaler = artifacts.get('scaler')
    
    if not model or not scaler:
         raise ValueError("Le fichier joblib ne contient pas de clé 'model' ou 'scaler' valide.")

    # 1. Construction du tableau de features dans l'ordre strict
    feature_values = []
    for feature in EXPECTED_FEATURES:
        # On récupère la valeur depuis le JSON du front. 
        # On met 0.0 par défaut si le front oublie d'envoyer une feature.
        val = float(input_data.get(feature, 0.0))
        feature_values.append(val)
        
    # 2. Conversion en numpy array 2D (1 ligne, 22 colonnes)
    # Scikit-learn attend toujours un tableau 2D pour les prédictions
    X = np.array([feature_values])
    
    # 3. Application du StandardScaler AVANT la prédiction (Très important !)
    X_scaled = scaler.transform(X)
        
    # 4. Inférence avec le modèle Huber
    prediction = model.predict(X_scaled)
    
    # prediction est un array numpy (ex: [12450.5]), on renvoie le float brut
    return float(prediction[0])