import os

# --- Configurations de l'application ---
VALID_KPIS = {"total_aura", "rural", "urbain", "stations_montagne", "villages_montagne"}
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "300"))

# --- Configurations Snowflake ---
SNOWFLAKE_LOGIN_TIMEOUT_SECONDS = int(os.getenv("SNOWFLAKE_LOGIN_TIMEOUT_SECONDS", "10"))
SNOWFLAKE_NETWORK_TIMEOUT_SECONDS = int(os.getenv("SNOWFLAKE_NETWORK_TIMEOUT_SECONDS", "20"))
SNOWFLAKE_SOCKET_TIMEOUT_SECONDS = int(os.getenv("SNOWFLAKE_SOCKET_TIMEOUT_SECONDS", "20"))
SNOWFLAKE_QUERY_TIMEOUT_SECONDS = int(os.getenv("SNOWFLAKE_QUERY_TIMEOUT_SECONDS", "20"))

SNOWFLAKE_SCHEMA_PUBLIC = os.getenv("SNOWFLAKE_SCHEMA_PUBLIC", os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")).strip() or "PUBLIC"
SNOWFLAKE_SCHEMA_FACT = os.getenv("SNOWFLAKE_SCHEMA_FACT", os.getenv("SNOWFLAKE_FACT_SCHEMA", "CLEANED")).strip() or "CLEANED"
SNOWFLAKE_SCHEMA_DIM = os.getenv("SNOWFLAKE_SCHEMA_DIM", os.getenv("SNOWFLAKE_DIM_SCHEMA", "DIMENSION")).strip() or "DIMENSION"
SNOWFLAKE_SCHEMA_REF = os.getenv("SNOWFLAKE_SCHEMA_REF", "REF").strip() or "REF"

# Alias utilisés dans le code existant
SNOWFLAKE_FACT_SCHEMA = SNOWFLAKE_SCHEMA_FACT
SNOWFLAKE_DIM_SCHEMA = SNOWFLAKE_SCHEMA_DIM

# --- Données statiques et Mappings ---
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
    "Ain": "01", "Allier": "03", "Ardeche": "07", "Cantal": "15",
    "Drome": "26", "Haute-Loire": "43", "Haute-Savoie": "74",
    "Isere": "38", "Loire": "42", "Puy-de-Dome": "63",
    "Rhone": "69", "Savoie": "73",
}