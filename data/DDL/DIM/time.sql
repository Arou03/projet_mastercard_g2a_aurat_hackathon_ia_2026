create or replace dynamic table M2_ISD_EQUIPE_5_DB.DIMENSION.TIME(
	DATE_TS,
	SATURDAY_TS,
	ANNEE,
	MOIS,
	JOUR,
	JOUR_SEMAINE,
	ISO_WEEK,
	ISO_YEAR,
	NOM_JOUR,
	SEMAINE,
	SAISON
) target_lag = '1 day' refresh_mode = AUTO initialize = ON_CREATE warehouse = HACKATHON_WH
 as
SELECT 
    TRY_TO_TIMESTAMP(DATE::VARCHAR) AS date_ts,
    TRY_TO_TIMESTAMP(SATURDAY::VARCHAR) AS saturday_ts,
    
    ANNEE AS annee,
    MOIS AS mois,
    JOUR AS jour,
    JOUR_SEMAINE AS jour_semaine,
    ISO_WEEK AS iso_week,
    ISO_YEAR AS iso_year,
    NOM_JOUR AS nom_jour,
    SEMAINE AS semaine,
    SAISON AS saison

FROM RAW.DIM_TEMPS;