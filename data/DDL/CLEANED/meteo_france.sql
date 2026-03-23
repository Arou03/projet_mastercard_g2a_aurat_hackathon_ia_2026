create or replace dynamic table M2_ISD_EQUIPE_5_DB.CLEANED.METEO_FRANCE(
	DEPARTEMENT_CODE,
	DATE,
	PRECIPITATIONS_MM_MOY,
	PRECIPITATIONS_DUREE_MN_MOY,
	TEMP_MIN_MOY,
	TEMP_MAX_MOY,
	TEMP_MOYENNE,
	TEMP_AMPLITUDE_MOY,
	VENT_MOYEN_MOY,
	VENT_RAFALE_MAX_MOY
) target_lag = '1 day' refresh_mode = AUTO initialize = ON_CREATE warehouse = HACKATHON_WH
 as
SELECT 
    departement_code,
    TO_TIMESTAMP(AAAAMMJJ::VARCHAR, 'YYYYMMDD') AS date,
    
    AVG(TRY_CAST(REPLACE(RR, ',', '.') AS FLOAT)) AS precipitations_mm_moy,
    AVG(TRY_CAST(REPLACE(DRR, ',', '.') AS FLOAT)) AS precipitations_duree_mn_moy, 
    
    AVG(TRY_CAST(REPLACE(TN, ',', '.') AS FLOAT)) AS temp_min_moy,
    AVG(TRY_CAST(REPLACE(TX, ',', '.') AS FLOAT)) AS temp_max_moy,
    AVG(TRY_CAST(REPLACE(TM, ',', '.') AS FLOAT)) AS temp_moyenne,
    AVG(TRY_CAST(REPLACE(TAMPLI, ',', '.') AS FLOAT)) AS temp_amplitude_moy,
    
    AVG(TRY_CAST(REPLACE(FFM, ',', '.') AS FLOAT)) AS vent_moyen_moy,
    AVG(TRY_CAST(REPLACE(FXI, ',', '.') AS FLOAT)) AS vent_rafale_max_moy

FROM RAW.METEO_AURAT_2020_2024
WHERE departement_code IS NOT NULL 
  AND AAAAMMJJ IS NOT NULL
GROUP BY 
    departement_code, 
    AAAAMMJJ;