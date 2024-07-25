import helper_funcs.db_conn as al
import utils

def _delete_duplicates(schematable):
  al.call_commit(f'''
    DELETE FROM (
      SELECT ROWNUMBER() OVER (PARTITION BY HYBRID_ID, ALF_E, WOB) AS RN
      FROM {schematable}
    )
     WHERE RN > 1
  ''')

def _boost_via_eduw(alf_table, main_table, eduw_alf, wdsdalf):
  al.call_commit(f'''
    MERGE INTO {alf_table} AS TRG
    USING (
      SELECT ALF.HYBRID_ID, EDUW.ALF_E, EDUW.ALF_MTCH_PCT, 
        EDUW.ALF_STS_CD, WDSD.WOB, EDUW.GNDR_CD,
        CASE WHEN ALF.WOB != WDSD.WOB THEN 1
          ELSE 0
        END AS HAS_WOB_BEEN_CORRECTED
      FROM {alf_table} AS ALF
      LEFT JOIN (
        SELECT HYBRID_ID, IRN_E
        FROM {main_table}
        WHERE IRN_E IS NOT NULL
        GROUP BY HYBRID_ID, IRN_E
      ) AS MAIN
        USING (HYBRID_ID)
      LEFT JOIN (
        SELECT *
        FROM {eduw_alf}
          WHERE ALF_STS_CD IN (4, 39, 35)
            AND (ALF_MTCH_PCT IS NULL OR ALF_MTCH_PCT >= 0.8)
      ) AS EDUW
        ON MAIN.IRN_E = EDUW.IRN_E
      LEFT JOIN {wdsdalf} AS WDSD
        ON EDUW.ALF_E = WDSD.ALF_E
      WHERE ALF.ALF_E IS NULL
        AND MAIN.IRN_E IS NOT NULL
        AND EDUW.ALF_E IS NOT NULL
        AND ABS(DAYS(WDSD.WOB) - DAYS(ALF.WOB)) < 365
      GROUP BY ALF.HYBRID_ID, EDUW.ALF_E, EDUW.ALF_MTCH_PCT,
        EDUW.ALF_STS_CD, WDSD.WOB, EDUW.GNDR_CD,
        CASE WHEN ALF.WOB != WDSD.WOB THEN 1
          ELSE 0
        END
    ) AS SRC
      ON TRG.HYBRID_ID = SRC.HYBRID_ID
    AND TRG.ALF_E IS NULL
    WHEN MATCHED THEN 
      UPDATE SET TRG.ALF_E = SRC.ALF_E,
          TRG.ALF_MTCH_PCT = SRC.ALF_MTCH_PCT,
          TRG.ALF_STS_CD = SRC.ALF_STS_CD,
          TRG.WOB = SRC.WOB,
          TRG.GNDR_CD = SRC.GNDR_CD,
          TRG.HAS_WOB_BEEN_CORRECTED = SRC.HAS_WOB_BEEN_CORRECTED;
  ''')

  _delete_duplicates(alf_table)

def _boost_lac_via_cin_crcs(mlac_alf, alf_table, source):
  al.call_commit(f'''
    MERGE INTO {mlac_alf} AS TRG
    USING (
      SELECT LAC.UNIFIED_ID, CRCS.ALF_E, CRCS.ALF_MTCH_PCT, 
        CRCS.ALF_STS_CD, CRCS.WOB, CRCS.GNDR_CD, CRCS.HAS_WOB_BEEN_CORRECTED
      FROM {mlac_alf} AS LAC
      LEFT JOIN {alf_table} AS CRCS
        ON LAC.UNIFIED_ID = CRCS.HYBRID_ID
      WHERE LAC.ALF_E IS NULL
        AND CRCS.ALF_E IS NOT NULL
        AND ABS(DAYS(CRCS.WOB) - DAYS(LAC.WOB)) < 365
      GROUP BY LAC.UNIFIED_ID, CRCS.ALF_E, CRCS.ALF_MTCH_PCT, 
        CRCS.ALF_STS_CD, CRCS.WOB, CRCS.GNDR_CD, CRCS.HAS_WOB_BEEN_CORRECTED
    ) AS SRC
      ON TRG.UNIFIED_ID = SRC.UNIFIED_ID
     AND TRG.ALF_E IS NULL
    WHEN MATCHED THEN 
      UPDATE SET TRG.ALF_E = SRC.ALF_E,
          TRG.ALF_MTCH_PCT = SRC.ALF_MTCH_PCT,
          TRG.ALF_STS_CD = SRC.ALF_STS_CD,
          TRG.WOB = SRC.WOB,
          TRG.GNDR_CD = SRC.GNDR_CD,
          TRG.HAS_WOB_BEEN_CORRECTED = SRC.HAS_WOB_BEEN_CORRECTED,
          TRG.MAX_POSS_WOB = NULL,
          TRG.POTENTIAL_UNKNOWN_WOB = 0,
          TRG.LACWOB_DEF_WRONG = 0,
          TRG.MISSING_FROM_LACALF = 0,
          TRG.ALF_SOURCE = '{source}',
          TRG.LAC_ALF_MATCH = 1;
  ''')

  _delete_duplicates(mlac_alf)

def boost_cin_crcs(base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mcrcs_alf = created_tables.get('mcrcs_alf')
  mcrcs_main = created_tables.get('mcrcs_main')
  mcin_alf = created_tables.get('mcin_alf')
  mcin_main = created_tables.get('mcin_main')
  wdsdalf = base_tables.get('wdsd_pers')
  eduw_alf = base_tables.get('eduw')

  # Boost CRCS via EDUW and then boost LAC
  _boost_via_eduw(mcrcs_alf, mcrcs_main, eduw_alf, wdsdalf)
  _boost_lac_via_cin_crcs(mlac_alf, mcrcs_alf, 'CRCS')

  # Boost CINW via EDUW and then boost LAC
  _boost_via_eduw(mcin_alf, mcin_main, eduw_alf, wdsdalf)
  _boost_lac_via_cin_crcs(mlac_alf, mcin_alf, 'CINW')

def boost_lac(base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')
  wdsdalf = base_tables.get('wdsd_pers')
  eduw_alf = base_tables.get('eduw')

  al.call_commit(f'''
    MERGE INTO {mlac_alf} AS TRG
    USING (
      SELECT DISTINCT LAC.UNIFIED_ID, EDUW.ALF_E, EDUW.ALF_MTCH_PCT, 
        EDUW.ALF_STS_CD, EDUW.WOB, EDUW.GNDR_CD, 
        CASE WHEN LAC.WOB != EDUW.WOB THEN 1
          ELSE 0
        END AS HAS_WOB_BEEN_CORRECTED
      FROM {mlac_alf} AS LAC
      LEFT JOIN (
        SELECT UNIFIED_ID, IRN_E
        FROM {mlac_main}
        WHERE IRN_E IS NOT NULL
        GROUP BY UNIFIED_ID, IRN_E
      ) AS MAIN
        USING (UNIFIED_ID)
      LEFT JOIN (
          SELECT EDUW.ALF_E, EDUW.IRN_E, EDUW.ALF_MTCH_PCT, EDUW.ALF_STS_CD,
            CASE WHEN WDSD.WOB IS NOT NULL THEN WDSD.WOB
                ELSE EDUW.WOB
            END AS WOB,
            CASE WHEN WDSD.gndr_cd IS NOT NULL THEN WDSD.gndr_cd
                ELSE EDUW.GNDR_CD
            END AS GNDR_CD
          FROM {eduw_alf} AS EDUW
          LEFT JOIN {wdsdalf} AS WDSD
            ON EDUW.ALF_E = WDSD.ALF_E
          WHERE EDUW.ALF_STS_CD IN (4, 39, 35)
            AND (EDUW.ALF_MTCH_PCT IS NULL OR EDUW.ALF_MTCH_PCT >= 0.7)
      ) AS EDUW
        ON MAIN.IRN_E = EDUW.IRN_E
      WHERE LAC.ALF_E IS NULL
        AND MAIN.IRN_E IS NOT NULL
        AND EDUW.IRN_E IS NOT NULL
        AND ABS(DAYS(EDUW.WOB) - DAYS(LAC.WOB)) < 365
    ) AS SRC
      ON TRG.UNIFIED_ID = SRC.UNIFIED_ID
     AND TRG.ALF_E IS NULL
    WHEN MATCHED THEN 
      UPDATE SET TRG.ALF_E = SRC.ALF_E,
          TRG.ALF_MTCH_PCT = SRC.ALF_MTCH_PCT,
          TRG.ALF_STS_CD = SRC.ALF_STS_CD,
          TRG.WOB = SRC.WOB,
          TRG.GNDR_CD = SRC.GNDR_CD,
          TRG.HAS_WOB_BEEN_CORRECTED = SRC.HAS_WOB_BEEN_CORRECTED,
          TRG.ALF_SOURCE = 'EDUW',
          TRG.LAC_ALF_MATCH = 1;
  ''')

  _delete_duplicates(mlac_alf)
