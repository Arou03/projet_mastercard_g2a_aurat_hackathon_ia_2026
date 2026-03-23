create or replace dynamic table M2_ISD_EQUIPE_5_DB.REF.REF_STATIONS(
	NOM_INSTALLATION,
	CODE_DEPARTEMENT,
	NOM_DEPARTEMENT,
	TYPE_EQUIPEMENT,
	LONGITUDE,
	LATITUDE,
	ACTIVITES_LISTE
) target_lag = '1 day' refresh_mode = AUTO initialize = ON_CREATE warehouse = HACKATHON_WH
 as
SELECT 
    TRY_CAST(NOM AS VARCHAR(255)) AS nom_installation,
    
    TRY_CAST(CODE_DEPARTEMENT AS VARCHAR(3)) AS code_departement,
    
    TRY_CAST(NOM_DEPARTEMENT AS VARCHAR(50)) AS nom_departement,
    
    TRY_CAST(TYPE AS VARCHAR(100)) AS type_equipement,
    
    TRY_CAST(REPLACE(LONGITUDE::VARCHAR, ',', '.') AS FLOAT) AS longitude,
    TRY_CAST(REPLACE(LATITUDE::VARCHAR, ',', '.') AS FLOAT) AS latitude,

    SPLIT(ACTIVITES, ',') AS activites_liste

FROM RAW.RAW_STATIONS;