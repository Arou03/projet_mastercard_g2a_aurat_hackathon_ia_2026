from snowflake.connector import DictCursor
from config import *
from database import get_connection, fq_table
from utils.helpers import read_ci, week_label_from_date, parse_week_number, season_week_sort
import datetime

def is_winter_week(week_label):
    num = parse_week_number(week_label)
    return num in [51, 52] or (num is not None and 1 <= num <= 15)

def filter_winter_holidays(holidays):
    return [h for h in holidays if is_winter_week(h.get("week_start")) or is_winter_week(h.get("week_end"))]

def build_mock_global_holidays(year=2024):
    year = max(2024, int(year))
    weeks = ["S51", "S52"] + [f"S{i}" for i in range(1, 16)]
    holidays = [
        {"country_code": "FR", "country_name": "France", "season": "HIVER", "holiday_type": "VACANCES_NOEL", "week_start": "S51", "week_end": "S1", "date_start": f"{year-1}-12-23", "date_end": f"{year}-01-08"},
        {"country_code": "FR", "country_name": "France", "season": "HIVER", "holiday_type": "VACANCES_HIVER", "week_start": "S6", "week_end": "S9", "date_start": f"{year}-02-10", "date_end": f"{year}-03-03"},
    ]
    winter_holidays = filter_winter_holidays(holidays)
    countries = sorted({item.get("country_name") for item in winter_holidays if item.get("country_name")})
    return {"weeks": weeks, "holidays": winter_holidays, "countries": countries}

def fetch_global_holidays_from_snowflake(year):
    holidays_table = fq_table(SNOWFLAKE_FACT_SCHEMA, "CALENDAR_SCHOOL_HOLIDAYS")
    countries_table = fq_table(SNOWFLAKE_DIM_SCHEMA, "DIM_COUNTRIES")
    season_code = f"H{str(int(year))[-2:]}".upper()

    connection = get_connection("aura_global_holidays")
    try:
        with connection.cursor(DictCursor) as cursor:
            query = f"SELECT h.CODE_PAYS AS code_pays, COALESCE(c.NOM, h.CODE_PAYS) AS country_name, h.DEBUT AS debut, h.FIN AS fin, h.SAISON AS saison, h.TYPE_VACANCES AS type_vacances FROM {holidays_table} h LEFT JOIN {countries_table} c ON c.CODE_PAYS = h.CODE_PAYS WHERE h.DEBUT IS NOT NULL AND h.FIN IS NOT NULL AND UPPER(h.SAISON) = %s"
            cursor.execute(query, (season_code,), timeout=SNOWFLAKE_QUERY_TIMEOUT_SECONDS)
            rows = cursor.fetchall()
    finally:
        connection.close()

    holidays, countries, weeks = [], set(), set()
    for row in rows:
        week_start, week_end = week_label_from_date(read_ci(row, "debut")), week_label_from_date(read_ci(row, "fin"))
        if not week_start or not week_end: continue
        
        country_name = str(read_ci(row, "country_name") or read_ci(row, "code_pays") or "Inconnu").strip()
        date_start = read_ci(row, "debut").isoformat() if hasattr(read_ci(row, "debut"), 'isoformat') else str(read_ci(row, "debut") or "")
        date_end = read_ci(row, "fin").isoformat() if hasattr(read_ci(row, "fin"), 'isoformat') else str(read_ci(row, "fin") or "")

        holidays.append({"country_code": str(read_ci(row, "code_pays") or "").strip().upper(), "country_name": country_name, "season": str(read_ci(row, "saison") or "").strip(), "holiday_type": str(read_ci(row, "type_vacances") or "").strip(), "week_start": week_start, "week_end": week_end, "date_start": date_start, "date_end": date_end})

    winter_holidays = filter_winter_holidays(holidays)
    winter_weeks = sorted({w for item in winter_holidays for w in (item.get("week_start"), item.get("week_end")) if w}, key=season_week_sort)
    winter_countries = sorted({item.get("country_name") for item in winter_holidays if item.get("country_name")})

    return {"year": int(year), "season": season_code, "weeks": winter_weeks, "holidays": winter_holidays, "countries": winter_countries}