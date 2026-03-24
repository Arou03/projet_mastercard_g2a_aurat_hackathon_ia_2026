import os
from snowflake.connector import DictCursor
from config import *
from database import get_connection, fq_table, set_last_error, is_snowflake_configured
from utils.cache import get_cache, set_cache
from utils.helpers import *
from services.holiday_service import filter_winter_holidays # Importé du service holiday

def get_snowflake_query():
    fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
    dim_table = fq_table(SNOWFLAKE_DIM_SCHEMA, "DIM_DEPARTEMENTS")
    custom_query = os.getenv("SNOWFLAKE_KPI_QUERY", "").strip()
    if custom_query: return custom_query

    return (
        f"SELECT f.CODE_DEPARTEMENT as department_code, "
        f"COALESCE(d.NOM_DEPARTEMENT, f.CODE_DEPARTEMENT) as department_name, "
        f"f.WEEK as week, CAST(f.TOTAL_AURA as INTEGER) as frequentation "
        f"FROM {fact_table} f LEFT JOIN {dim_table} d ON d.CODE_DEPARTEMENT = f.CODE_DEPARTEMENT "
        f"ORDER BY f.CODE_DEPARTEMENT"
    )

def fetch_available_weeks_from_snowflake(connection):
    fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
    with connection.cursor(DictCursor) as cursor:
        cursor.execute(f"SELECT DISTINCT WEEK FROM {fact_table} WHERE WEEK IS NOT NULL", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
        rows = cursor.fetchall()
    
    weeks = [str(read_ci(r, "WEEK")).upper() for r in rows if read_ci(r, "WEEK") is not None]
    return sorted(set(weeks), key=week_sort_key)

def fetch_dataset_from_snowflake(selected_weeks=None):
    selected_weeks = selected_weeks or []
    fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
    dim_table = fq_table(SNOWFLAKE_DIM_SCHEMA, "DIM_DEPARTEMENTS")

    connection = get_connection("aura_dashboard_backend")
    try:
        available_weeks = fetch_available_weeks_from_snowflake(connection)
        where_clause = f"WHERE WEEK IN ({', '.join([f'{w!r}' for w in selected_weeks])})" if selected_weeks else ""
        
        query = (
            f"SELECT f.CODE_DEPARTEMENT as department_code, COALESCE(d.NOM_DEPARTEMENT, f.CODE_DEPARTEMENT) as department_name, "
            f"SUM(f.TOTAL_AURA) as total_aura, SUM(f.RURAL) as rural, SUM(f.URBAIN) as urbain, "
            f"SUM(f.STATIONS_MONTAGNE) as stations_montagne, SUM(f.VILLAGES_MONTAGNE) as villages_montagne "
            f"FROM {fact_table} f LEFT JOIN {dim_table} d ON d.CODE_DEPARTEMENT = f.CODE_DEPARTEMENT "
            f"{where_clause} GROUP BY f.CODE_DEPARTEMENT, d.NOM_DEPARTEMENT ORDER BY f.CODE_DEPARTEMENT"
        )
        with connection.cursor(DictCursor) as cursor:
            cursor.execute(query, timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            rows = cursor.fetchall()
    finally:
        connection.close()

    dataset = {}
    for row in rows:
        canonical_name = normalize_department_name(str(read_ci(row, "department_name") or ""))
        if not canonical_name: canonical_name = normalize_department_name(str(read_ci(row, "department_code")))
        if not canonical_name: continue

        dataset[canonical_name] = {
            "total_aura": to_int(read_ci(row, "total_aura")),
            "rural": to_int(read_ci(row, "rural")),
            "urbain": to_int(read_ci(row, "urbain")),
            "stations_montagne": to_int(read_ci(row, "stations_montagne")),
            "villages_montagne": to_int(read_ci(row, "villages_montagne")),
        }
        dataset[canonical_name]["frequentation"] = dataset[canonical_name]["total_aura"]

    return dataset, query, available_weeks

def get_department_dataset(selected_weeks=None):
    selected_weeks = selected_weeks or []
    weeks_key = ",".join(selected_weeks) if selected_weeks else "ALL"
    dataset, hit = get_cache(f"dataset:{weeks_key}")
    
    if hit:
        available_weeks, _ = get_cache("available_weeks")
        return dataset, True, "cache", available_weeks or []

    if is_snowflake_configured():
        try:
            dataset, _, available_weeks = fetch_dataset_from_snowflake(selected_weeks)
            if dataset:
                set_cache(f"dataset:{weeks_key}", dataset)
                set_cache("available_weeks", available_weeks)
                set_last_error(None)
                return dataset, False, "snowflake", available_weeks
            set_last_error("Snowflake query returned no usable rows")
        except Exception as exc:
            set_last_error(exc)

    dataset = DEPARTMENT_KPIS
    set_cache(f"dataset:{weeks_key}", dataset)
    return dataset, False, "mock", []

def build_departments_payload(selected_kpi, dataset):
    items = []
    for canonical_name, values in dataset.items():
        frequentation = values.get("frequentation", values.get("total_aura", 0))
        score = values.get(selected_kpi, 0)
        items.append({"name": to_display_name(canonical_name), "frequentation": frequentation, "score": score})
    return items

def build_mock_department_timeline(canonical_name, year=2024):
    base = DEPARTMENT_KPIS.get(canonical_name, DEPARTMENT_KPIS["Savoie"])
    weeks = ["S51", "S52"] + [f"S{i}" for i in range(1, 16)]
    year = max(2024, int(year))
    growth_factor = 1.0 + (0.05 * (year - 2024))

    observed, rural, urbain, stations, villages = [], [], [], [], []
    for index, _ in enumerate(weeks):
        variation = 1 + (0.08 * ((index % 5) - 2))
        observed.append(to_int(base["total_aura"] * variation * growth_factor))
        rural.append(to_int(base["rural"] * variation * growth_factor))
        urbain.append(to_int(base["urbain"] * variation * growth_factor))
        stations.append(to_int(base["stations_montagne"] * variation * growth_factor))
        villages.append(to_int(base["villages_montagne"] * variation * growth_factor))

    holidays = [
        {"country_code": "FR", "country_name": "France", "season": "HIVER", "holiday_type": "VACANCES_NOEL", "week_start": "S51", "week_end": "S1", "date_start": f"{year-1}-12-23", "date_end": f"{year}-01-08"},
        {"country_code": "FR", "country_name": "France", "season": "HIVER", "holiday_type": "VACANCES_SKI", "week_start": "S6", "week_end": "S9", "date_start": f"{year}-02-10", "date_end": f"{year}-03-03"},
    ]

    return {
        "weeks": weeks,
        "series": [
            {"id": "observed", "label": "Frequentation observee", "values": observed, "color": "#086cb2"},
            {"id": "prediction", "label": "Prediction" if year > 2024 else "", "values": [None]*len(weeks) if year == 2024 else observed, "color": "#d14247"},
            {"id": "rural", "label": "Feature rural", "values": rural, "color": "#1a7251"},
            {"id": "urbain", "label": "Feature urbain", "values": urbain, "color": "#90437d"},
            {"id": "stations", "label": "Feature stations montagne", "values": stations, "color": "#d4a434"},
            {"id": "villages", "label": "Feature villages montagne", "values": villages, "color": "#00a0df"},
        ],
        "holidays": filter_winter_holidays(holidays),
        "countries": ["France"],
        "frequentation": {
            "weeks": weeks,
            "values_observed": observed if year == 2024 else [None]*len(weeks),
            "values_predicted": [None]*len(weeks) if year == 2024 else observed,
            "year": year
        }
    }

def fetch_department_timeline_from_snowflake(canonical_name):
    fact_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "FREQ_GLOBAL_PER_DEPT")
    holidays_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "CALENDAR_SCHOOL_HOLIDAYS")
    countries_table = fq_table(SNOWFLAKE_DIM_SCHEMA, "DIM_COUNTRIES")
    department_code = DEPARTMENT_TO_CODE.get(canonical_name)

    connection = get_connection("aura_department_timeline")
    try:
        with connection.cursor(DictCursor) as cursor:
            if department_code:
                cursor.execute(f"SELECT WEEK AS week, SUM(TOTAL_AURA) AS total_aura, SUM(RURAL) AS rural, SUM(URBAIN) AS urbain, SUM(STATIONS_MONTAGNE) AS stations_montagne, SUM(VILLAGES_MONTAGNE) AS villages_montagne FROM {fact_table} WHERE CODE_DEPARTEMENT = %s GROUP BY WEEK", (department_code,), timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            else:
                cursor.execute(f"SELECT WEEK AS week, SUM(TOTAL_AURA) AS total_aura, SUM(RURAL) AS rural, SUM(URBAIN) AS urbain, SUM(STATIONS_MONTAGNE) AS stations_montagne, SUM(VILLAGES_MONTAGNE) AS villages_montagne FROM {fact_table} GROUP BY WEEK", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            weekly_rows = cursor.fetchall()

        week_values = {}
        for row in weekly_rows:
            week = week_label(read_ci(row, "week"))
            week_values[week] = {"total_aura": to_int(read_ci(row, "total_aura")), "rural": to_int(read_ci(row, "rural")), "urbain": to_int(read_ci(row, "urbain")), "stations_montagne": to_int(read_ci(row, "stations_montagne")), "villages_montagne": to_int(read_ci(row, "villages_montagne"))}

        ordered_weeks = sorted(week_values.keys(), key=season_week_sort)

        with connection.cursor(DictCursor) as cursor:
            cursor.execute(f"SELECT h.CODE_PAYS AS code_pays, COALESCE(c.NOM, h.CODE_PAYS) AS country_name, h.DEBUT AS debut, h.FIN AS fin, h.SAISON AS saison, h.TYPE_VACANCES AS type_vacances FROM {holidays_table} h LEFT JOIN {countries_table} c ON c.CODE_PAYS = h.CODE_PAYS WHERE h.DEBUT IS NOT NULL AND h.FIN IS NOT NULL", timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            holiday_rows = cursor.fetchall()
    finally:
        connection.close()

    holidays, countries = [], set()
    for row in holiday_rows:
        week_start, week_end = week_label_from_date(read_ci(row, "debut")), week_label_from_date(read_ci(row, "fin"))
        if not week_start or not week_end: continue
        country_name = str(read_ci(row, "country_name") or read_ci(row, "code_pays") or "").strip()
        if country_name: countries.add(country_name)
        holidays.append({"country_code": str(read_ci(row, "code_pays") or "").strip(), "country_name": country_name, "season": str(read_ci(row, "saison") or "").strip(), "holiday_type": str(read_ci(row, "type_vacances") or "").strip(), "week_start": week_start, "week_end": week_end})

    observed_values = [week_values[w]["total_aura"] for w in ordered_weeks]
    series = [
        {"id": "observed", "label": "Frequentation observee", "values": observed_values, "color": "#086cb2"},
        {"id": "prediction", "label": "Prediction", "values": [None]*len(ordered_weeks), "color": "#d14247"},
        {"id": "rural", "label": "Feature rural", "values": [week_values[w]["rural"] for w in ordered_weeks], "color": "#1a7251"},
        {"id": "urbain", "label": "Feature urbain", "values": [week_values[w]["urbain"] for w in ordered_weeks], "color": "#90437d"},
        {"id": "stations", "label": "Feature stations montagne", "values": [week_values[w]["stations_montagne"] for w in ordered_weeks], "color": "#d4a434"},
        {"id": "villages", "label": "Feature villages montagne", "values": [week_values[w]["villages_montagne"] for w in ordered_weeks], "color": "#00a0df"},
    ]

    return {
        "weeks": ordered_weeks,
        "series": series,
        "holidays": holidays,
        "countries": sorted(list(countries)),
        "frequentation": {
            "weeks": ordered_weeks,
            "values_observed": observed_values,
            "values_predicted": [None]*len(ordered_weeks),
            "year": 2024
        }
    }