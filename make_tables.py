import helper_funcs.db_conn as al
import utils

""" MLAC """

def _create_mlac_alf(mlac_alf, lac_alf, lac_epi, wdsdalf):
  utils.drop_if_exists(mlac_alf)

  al.call_no_return(f'''
    CREATE TABLE {mlac_alf} AS (
      WITH LAC AS (
        SELECT SYSTEM_ID_E, LOCAL_AUTHORITY_CODE, ALF_E,
          ALF_MTCH_PCT, ALF_STS_CD, GNDR_CD, WOB,
          CONCAT(CONCAT(SYSTEM_ID_E, '_'), LOCAL_AUTHORITY_CODE) AS HYBRID_ID
        FROM {lac_alf}
      ), 
      EPISODES AS (
        SELECT CONCAT(CONCAT(SYSTEM_ID_E, '_'), LOCAL_AUTHORITY_CODE) AS HYBRID_ID,
          MIN(EPISODE_START_DATE) AS MIN_ESTART
        FROM {lac_epi}
        GROUP BY CONCAT(CONCAT(SYSTEM_ID_E, '_'), LOCAL_AUTHORITY_CODE)
      )
      SELECT DISTINCT LAC.HYBRID_ID, LAC.SYSTEM_ID_E, LAC.LOCAL_AUTHORITY_CODE, LAC.ALF_E,
        LAC.ALF_MTCH_PCT, LAC.ALF_STS_CD, LAC.GNDR_CD, LAC.WOB AS ORIGINAL_LAC_WOB,
        CASE WHEN LAC.WOB != WDSD.WOB AND LAC.ALF_E IS NOT NULL THEN WDSD.WOB
          ELSE LAC.WOB
        END AS WOB,
        CASE WHEN LAC.ALF_E IS NULL 
              AND DAY(LAC.WOB) <= 15 
              AND DAY(LAC.WOB) + 7 >= 15
              AND EPI.MIN_ESTART >= LAC.WOB
            THEN ADD_DAYS(LAC.WOB, 7)
          WHEN LAC.ALF_E IS NULL AND EPI.MIN_ESTART < LAC.WOB THEN EPI.MIN_ESTART
          ELSE NULL
        END AS MAX_POSS_WOB,
        CASE WHEN LAC.ALF_E IS NULL AND DAY(LAC.WOB) <= 15 
              AND DAY(LAC.WOB) + 7 >= 15 
             THEN 1
          ELSE 0
        END AS POTENTIAL_UNKNOWN_WOB,
        CASE WHEN LAC.WOB != WDSD.WOB AND LAC.ALF_E IS NOT NULL THEN 1
          WHEN LAC.WOB = WDSD.WOB AND LAC.ALF_E IS NOT NULL THEN 0
          ELSE NULL
        END AS HAS_WOB_BEEN_CORRECTED,
        CASE WHEN EPI.MIN_ESTART < LAC.WOB THEN 1
          ELSE 0
        END AS LACWOB_DEF_WRONG,
        EPI.MIN_ESTART AS FIRST_EP_START
      FROM LAC AS LAC
      LEFT JOIN {wdsdalf} AS WDSD
        ON LAC.ALF_E = WDSD.ALF_E
      LEFT JOIN EPISODES AS EPI
        ON LAC.HYBRID_ID = EPI.HYBRID_ID
    ) WITH DATA;
  ''')

def _create_mlac_main(mlac_main, lac_main, lsoa_decode, la_codes):
  utils.drop_if_exists(mlac_main)

  al.call_no_return(f'''
    CREATE TABLE {mlac_main} AS (
      WITH LAC AS (
        SELECT *,
          CONCAT(CONCAT(SYSTEM_ID_E, '_'), LOCAL_AUTHORITY_CODE) AS HYBRID_ID,
          FIRST_VALUE(IRN_E) OVER (
            PARTITION BY CONCAT(CONCAT(SYSTEM_ID_E, '_'), LOCAL_AUTHORITY_CODE) 
            ORDER BY IRN_E
          ) AS BACKFILL_IRN
        FROM {lac_main}
      )
      SELECT LAC.SYSTEM_ID_E, LAC.BACKFILL_IRN AS IRN_E, LAC.HYBRID_ID,
        LAC.YEAR_CODE, LAC.LOCAL_AUTHORITY_CODE, LAC.DISABILITY_CODE, 
        LAC.ASYLUM, LAC.DATE_CEASED_UASC, LAC.DATE_OF_DATA_COLLECTION,
        LAC.ETHNICITY, LAC.LSOA2011_HOME_POSTCODE, LAC.AVAIL_FROM_DT,
        CASE WHEN LAC.IRN_E IS NULL AND LAC.BACKFILL_IRN IS NOT NULL THEN 1
            ELSE 0
        END AS IRN_BACKFILLED,
        LCD.LA_NAME,
        LSA.WD20NM AS HOME_AREA,
        LSA.LAD20NM AS HOME_LOCALAUTH
      FROM LAC AS LAC
      LEFT JOIN {lsoa_decode} AS LSA
        ON LAC.LSOA2011_HOME_POSTCODE = LSA.LSOA11CD
      LEFT JOIN {la_codes} AS LCD
        ON LAC.LOCAL_AUTHORITY_CODE = LCD.LA_ID
      ORDER BY LAC.YEAR_CODE
    ) WITH DATA;
  ''')

def create_mlac_tables(base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')
  lac_alf = base_tables.get('lac_alf')
  lac_main = base_tables.get('lac_main')
  lac_epi = base_tables.get('lac_epi')
  wdsdalf = base_tables.get('wdsd_pers')
  lsoa_decode = created_tables.get('lsoa_decode')
  la_codes = created_tables.get('la_codes')

  # MLAC ALF
  _create_mlac_alf(mlac_alf, lac_alf, lac_epi, wdsdalf)

  # MLAC Main
  _create_mlac_main(mlac_main, lac_main, lsoa_decode, la_codes)

""" EPISODES """

def create_epi_table(base_tables, created_tables): # 2099-01-01
  mlac_epi = created_tables.get('mlac_epi')
  lac_epi = base_tables.get('lac_epi')
  lsoa_decode = created_tables.get('lsoa_decode')
  la_codes = created_tables.get('la_codes')

  utils.drop_if_exists(mlac_epi)

  al.call_no_return(f'''
    CREATE TABLE {mlac_epi} AS (
      SELECT CONCAT(CONCAT(system_id_e, '_'), local_authority_code) AS hybrid_id, 
        lac.*,
        lcd.LA_NAME,
        lsa.WD20NM AS placement_area,
        lsa.LAD20NM AS placement_localauth,
        CASE 
            WHEN EPISODE_END_DATE IS NULL THEN DATE('2099-09-09')
            ELSE EPISODE_END_DATE 
        END AS FILLED_EPISODE_END_DATE 
      FROM {lac_epi} AS lac
      LEFT JOIN {lsoa_decode} AS lsa
          ON lac.LSOA2011_PLACEMENT_POSTCODE = LSA.LSOA11CD 
      LEFT JOIN {la_codes} AS lcd
          ON lac.LOCAL_AUTHORITY_CODE = lcd.LA_ID 
      ORDER BY year_code, episode_number
    ) WITH DATA;
  ''')

""" CIN """

def _create_mcin_alf(mcin_alf, cin_alf, wdsdalf):
  utils.drop_if_exists(mcin_alf)

  al.call_no_return(f'''
    CREATE TABLE {mcin_alf} AS (
      SELECT DISTINCT cin.child_code_e, cin.local_authority_code, cin.alf_e, 
        cin.alf_mtch_pct, cin.alf_sts_cd, 
        CASE WHEN cin.wob != wdsd.wob AND cin.alf_e IS NOT NULL THEN wdsd.wob
             ELSE cin.wob
        END AS wob,
        cin.gndr_cd, 
        cin.lsoa2011_cd,
        CONCAT(CONCAT(cin.child_code_e, '_'), cin.local_authority_code) AS hybrid_id, 
        CASE WHEN cin.wob != wdsd.wob AND cin.alf_e IS NOT NULL THEN 1
             WHEN cin.wob = wdsd.wob AND cin.alf_e IS NOT NULL THEN 0
             ELSE NULL
        END AS has_wob_been_corrected,
        cin.wob AS original_cin_wob
      FROM {cin_alf} AS cin
      LEFT JOIN {wdsdalf} AS wdsd
        ON cin.alf_e = wdsd.alf_e
    ) WITH DATA;
  ''')

def _create_mcin_main(mcin_main, cin_main):
  utils.drop_if_exists(mcin_main)

  al.call_no_return(f'''
    CREATE TABLE {mcin_main} AS (
      SELECT CHILD_CODE_E,
        CONCAT(CONCAT(child_code_e, '_'), local_authority_code) AS hybrid_id,  
        FIRST_VALUE(irn_e) OVER (
          PARTITION BY CONCAT(CONCAT(child_code_e, '_'), local_authority_code
        ) ORDER BY irn_e) AS irn_e,
        LOCAL_AUTHORITY_CODE, YEAR_CODE, UNBORN_CHILD, ETHNIC_ORIGIN_CODE, 
      	ASYLUM_SEEKER_CODE, CHILD_PROTECTION_REGISTER, LOOKED_AFTER_CHILD, 
      	SOURCE_MOST_RECENT_REFERRAL_CODE, RECENT_REFERRAL_CHILD_PREVIOUSLY_ON_CPR, 
      	RECENT_REFERRAL_CHILD_PREVIOUSLY_LAC, RECENT_REFERRAL_PARENTAL_SUBSTANCE_ALCOHOL_MISUSE, 
      	RECENT_REFERRAL_PARENTAL_LEARNING_DISABILITIES, RECENT_REFERRAL_PARENTAL_MENTAL_ILL_HEALTH, 
      	RECENT_REFERRAL_PARENTAL_PHYSICAL_ILL_HEALTH, RECENT_REFERRAL_PARENTAL_DOMESTIC_ABUSE, 
      	CATEGORY_OF_NEED_CODE, PERMANENT_EXCLUSIONS, FIXED_TERM_EXCLUSIONS, 
      	FIXED_TERM_EXCLUSIONS_NUMBER_DAYS_EXCLUDED, YOUTH_OFFENDING, 
      	CHILD_HEALTH_SURVEILLANCE_CHECKS, HEALTH_IMMUNISATIONS, HEALTH_DENTAL_CHECK, 
      	HEALTH_SUBSTANCE_MISUSE, HEALTH_MENTAL_HEALTH, HEALTH_AUTISTIC_SPECTRUM_DISORDER, 
      	DISABILITY_NONE, DISABILITY_MOBILITY, DISABILITY_MANUAL_DEXTERITY, 
      	DISABILITY_PHYSICAL_COORDINATION, DISABILITY_CONTINENCE, DISABILITY_LIFT_CARRY_OBJECTS, 
      	DISABILITY_SPEECH_HEARING_EYE_SIGHT, DISABILITY_MEMORY, DISABILITY_PERCEPTION_RISK_DANGER, 
      	PARENTING_CAPACITY_SUBSTANCE_ALCOHOL_MISUSE, PARENTING_CAPACITY_LEARNING_DISABILITIES, 
      	PARENTING_CAPACITY_MENTAL_ILL_HEALTH, PARENTING_CAPACITY_PHYSICAL_ILL_HEALTH, 
      	PARENTING_CAPACITY_DOMESTIC_ABUSE, DATE_OF_DATA_COLLECTION, AVAIL_FROM_DT
      FROM {cin_main}
    ) WITH DATA;
  ''')

def create_mcin_tables(base_tables, created_tables):
  mcin_alf = created_tables.get('mcin_alf')
  mcin_main = created_tables.get('mcin_main')
  cin_alf = base_tables.get('cin_alf')
  cin_main = base_tables.get('cin_main')
  wdsdalf = base_tables.get('wdsd_pers')

  # MCIN ALF
  _create_mcin_alf(mcin_alf, cin_alf, wdsdalf)

  # MCIN Main
  _create_mcin_main(mcin_main, cin_main)

""" CRCS """

def _create_crcs_alf(mcrcs_alf, crcs_alf, wdsdalf):
  utils.drop_if_exists(mcrcs_alf)

  al.call_no_return(f'''
    CREATE TABLE {mcrcs_alf} AS (
      SELECT DISTINCT cin.child_code_e, cin.local_authority_code, cin.alf_e,
        cin.alf_mtch_pct, cin.alf_sts_cd, cin.gndr_cd, cin.lsoa2011_cd, cin.wob AS original_crcs_wob,
        CASE WHEN cin.wob != wdsdalf.wob AND cin.alf_e IS NOT NULL THEN wdsdalf.wob
             ELSE cin.wob
        END AS wob,
        CONCAT(CONCAT(cin.child_code_e, '_'), cin.local_authority_code) AS hybrid_id, 
        CASE WHEN cin.wob != wdsdalf.wob AND cin.alf_e IS NOT NULL THEN 1
             WHEN cin.wob = wdsdalf.wob AND cin.alf_e IS NOT NULL THEN 0
             ELSE NULL
        END AS has_wob_been_corrected
      FROM {crcs_alf} AS cin
      LEFT JOIN {wdsdalf} AS wdsdalf
        ON cin.alf_e = wdsdalf.alf_e
    ) WITH DATA
  ''')

def _create_crcs_main(mcrcs_main, crcs_main):
  utils.drop_if_exists(mcrcs_main)

  al.call_no_return(f'''
    CREATE TABLE {mcrcs_main} AS (
      SELECT CHILD_CODE_E, LOCAL_AUTHORITY_CODE, YEAR_CODE, ETHNIC_ORIGIN_CODE, 
        ASYLUM_SEEKER_CODE, CHILD_PROTECTION_REGISTER, LOOKED_AFTER_CHILD, 
        CATEGORY_OF_NEED_CODE, LANGUAGE, YOUTH_OFFENDING, 
        CHILD_HEALTH_SURVEILLANCE_CHECKS, HEALTH_IMMUNISATIONS, 
        HEALTH_DENTAL_CHECK, HEALTH_SUBSTANCE_MISUSE, HEALTH_MENTAL_HEALTH, 
        HEALTH_AUTISTIC_SPECTRUM_DISORDER, DISABILITY_NONE, DISABILITY_MOBILITY, 
        DISABILITY_MANUAL_DEXTERITY, DISABILITY_PHYSICAL_COORDINATION, 
        DISABILITY_CONTINENCE, DISABILITY_LIFT_CARRY_OBJECTS, 
        DISABILITY_SPEECH_HEARING_EYE_SIGHT, DISABILITY_MEMORY, 
        DISABILITY_PERCEPTION_RISK_DANGER, 
        PARENTING_CAPACITY_SUBSTANCE_ALCOHOL_MISUSE, 
        PARENTING_CAPACITY_LEARNING_DISABILITIES, 
        PARENTING_CAPACITY_MENTAL_ILL_HEALTH, 
        PARENTING_CAPACITY_PHYSICAL_ILL_HEALTH, 
        PARENTING_CAPACITY_DOMESTIC_ABUSE, 
        DATE_OF_DATA_COLLECTION, CPR_DATE, CPR_NEGLECT, CPR_PHYSICAL_ABUSE, 
        CPR_SEXUAL_ABUSE, CPR_FINANCIAL_ABUSE, CPR_EMOTIONAL_ABUSE, AVAIL_FROM_DT,
        CONCAT(CONCAT(child_code_e, '_'), local_authority_code) AS hybrid_id,
        FIRST_VALUE(irn_e) OVER (
          PARTITION BY CONCAT(CONCAT(child_code_e, '_'), local_authority_code) 
          ORDER BY irn_e
        ) AS irn_e
      FROM {crcs_main}
    ) WITH DATA
  ''')

def create_crcs_tables(base_tables, created_tables):
  mcrcs_alf = created_tables.get('mcrcs_alf')
  mcrcs_main = created_tables.get('mcrcs_main')
  crcs_alf = base_tables.get('crcs_alf')
  crcs_main = base_tables.get('crcs_main')
  wdsdalf = base_tables.get('wdsd_pers')

  # CRCS ALF
  _create_crcs_alf(mcrcs_alf, crcs_alf, wdsdalf)

  # CRCS Main
  _create_crcs_main(mcrcs_main, crcs_main)
