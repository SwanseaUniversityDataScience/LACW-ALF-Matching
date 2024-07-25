import helper_funcs.db_conn as al
import utils

def _crossfill_cin_crcs(tables):
  for target in tables:
    source = [tbl for tbl in tables if tbl != target][0]

    al.call_commit(f'''
      MERGE INTO {target} TRG
      USING (
        SELECT TARGET_DATA.HYBRID_ID, CROSS.MAX_IRN AS IRN_E
        FROM {target} AS TARGET_DATA
        LEFT JOIN (
            SELECT HYBRID_ID, MAX(IRN_E) AS MAX_IRN
            FROM {source}
            GROUP BY HYBRID_ID
        ) AS CROSS
        ON TARGET_DATA.HYBRID_ID = CROSS.HYBRID_ID
        WHERE TARGET_DATA.IRN_E IS NULL
          AND CROSS.MAX_IRN IS NOT NULL
        GROUP BY TARGET_DATA.HYBRID_ID, CROSS.MAX_IRN
      ) AS SRC
        ON TRG.HYBRID_ID = SRC.HYBRID_ID
       AND TRG.IRN_E IS NULL
      WHEN MATCHED THEN
        UPDATE SET TRG.IRN_E = SRC.IRN_E;
    ''')

def _crossfill_lac(mlac_main, tables):
  query = '''
    MERGE INTO %(mlac_main)s TRG
    USING (
      SELECT LAC.UNIFIED_ID, CROSS.IRN_E
      FROM %(mlac_main)s AS LAC
      LEFT JOIN (
          SELECT HYBRID_ID, IRN_E
          FROM %(source)s
          WHERE IRN_E IS NOT NULL
      ) AS CROSS
        ON LAC.UNIFIED_ID = CROSS.HYBRID_ID
      WHERE LAC.IRN_E IS NULL
        AND LAC.SYS_ID_CHANGES = %(sys_id_changes)s
        AND CROSS.IRN_E IS NOT NULL
      GROUP BY LAC.UNIFIED_ID, CROSS.IRN_E
    ) AS SRC
      ON TRG.UNIFIED_ID = SRC.UNIFIED_ID
     AND TRG.IRN_E IS NULL
     AND TRG.SYS_ID_CHANGES = %(sys_id_changes)s
    WHEN MATCHED THEN
      UPDATE SET TRG.IRN_E = SRC.IRN_E;
  '''

  for schematable in tables:
    for value in [0, 1]:
      al.call_commit(query % {
        'mlac_main': mlac_main,
        'source': schematable,
        'sys_id_changes': value
      })

def perform_crossfill(base_tables, created_tables):
  mlac_main = created_tables.get('mlac_main')
  mcin_main = created_tables.get('mcin_main')
  mcrcs_main = created_tables.get('mcrcs_main')
  
  # Crossfill CIN & CRCS
  _crossfill_cin_crcs([mcin_main, mcrcs_main])

  # Crossfill LAC
  _crossfill_lac(mlac_main, [mcin_main, mcrcs_main])

def correct_crossfill(base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')
  eduw_alf = base_tables.get('eduw')

  al.call_commit(f'''
    MERGE INTO {mlac_alf} TRG
    USING (
      SELECT LAC.UNIFIED_ID, EDUW.WOB, EDUW.GNDR_CD
      FROM (
          SELECT ALF.*, MAIN.IRN_E
          FROM {mlac_alf} AS alf
          LEFT JOIN (
              SELECT UNIFIED_ID, MAX(IRN_E) AS IRN_E
              FROM {mlac_main}
              GROUP BY UNIFIED_ID
          ) AS MAIN
          ON ALF.UNIFIED_ID = MAIN.UNIFIED_ID
          WHERE ALF.WOB IS NULL
            AND MAIN.IRN_E IS NOT NULL
      ) AS LAC
      LEFT JOIN {eduw_alf} AS EDUW
      ON LAC.IRN_E = EDUW.IRN_E
    ) AS SRC
      ON TRG.UNIFIED_ID = SRC.UNIFIED_ID
    AND TRG.WOB IS NULL
    WHEN MATCHED THEN
      UPDATE SET TRG.WOB = SRC.WOB,
          TRG.GNDR_CD = SRC.GNDR_CD,
          TRG.HAS_WOB_BEEN_CORRECTED = 1,
          TRG.LACWOB_DEF_WRONG = 1;
  ''')
