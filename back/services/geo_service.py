from snowflake.connector import DictCursor
from config import *
from database import get_connection, fq_table, is_snowflake_configured, set_last_error
from utils.cache import get_cache, set_cache
from utils.helpers import read_ci, extract_activities_from_array

def fetch_stations_from_snowflake():
    ref_table = fq_table(SNOWFLAKE_SCHEMA_REF, "REF_STATIONS")
    connection = get_connection("aura_dashboard_stations")
    
    try:
        with connection.cursor(DictCursor) as cursor:
            cursor.execute(f"SELECT * FROM {ref_table}", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            rows = cursor.fetchall()
    finally:
        connection.close()
    
    station_points = []
    activities_set = set()
    
    for row in rows:
        try:
            lat = float(read_ci(row, "LATITUDE") or read_ci(row, "LAT") or 0)
            lon = float(read_ci(row, "LONGITUDE") or read_ci(row, "LON") or 0)
            if lat == 0 and lon == 0: continue
        except (TypeError, ValueError):
            continue
        
        activities = extract_activities_from_array(read_ci(row, "ACTIVITES_LISTE"))
        activities_set.update(activities)
        
        station_points.append({
            "name": str(read_ci(row, "NOM_INSTALLATION") or read_ci(row, "NOM") or "Installation sans nom").strip(),
            "lat": lat, "lon": lon,
            "department_code": str(read_ci(row, "CODE_DEPARTEMENT") or "").strip(),
            "department_name": str(read_ci(row, "NOM_DEPARTEMENT") or "").strip(),
            "equipment_type": str(read_ci(row, "TYPE_EQUIPEMENT") or "").strip(),
            "activities": activities,
        })
    
    return station_points, sorted(list(activities_set), key=lambda x: str(x).lower())

def get_station_points(selected_activities=None):
    selected_activities = selected_activities or []
    activity_key = "|".join([item.lower() for item in selected_activities]) if selected_activities else "ALL"
    cache_key = f"stations:{activity_key}"

    cached_value, hit = get_cache(cache_key)
    if hit: return cached_value["points"], cached_value["activities_available"], True, cached_value["data_source"]

    if is_snowflake_configured():
        try:
            points, activities_available = fetch_stations_from_snowflake()
            
            # Filtre si nécessaire
            if selected_activities:
                act_set = {a.lower() for a in selected_activities}
                points = [p for p in points if any(act.lower() in act_set for act in p.get("activities", []))]

            payload = {"points": points, "activities_available": activities_available, "data_source": "snowflake"}
            set_cache(cache_key, payload)
            set_last_error(None)
            return points, activities_available, False, "snowflake"
        except Exception as exc:
            set_last_error(exc)

    return [], [], False, "mock"