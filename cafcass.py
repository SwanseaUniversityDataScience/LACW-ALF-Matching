import pandas as pd
import recordlinkage

import helper_funcs.db_conn as al
import utils

def _get_lac_epi(mlac_alf, mlac_main, mlac_epi, eduw_alf, cafhear):
  maximum_hearing_date = al.call(f'''
    SELECT MAX(CAP_HEARINGDATETIME) AS MAX_HEAR
    FROM {cafhear}
  ''').iloc[0]['max_hear']

  return al.call(f'''
    SELECT EP.UNIFIED_ID, EP.LOCAL_AUTHORITY_CODE, EP.LA_NAME, EP.EPISODE_START_DATE, 
      EP.LSOA2011_PLACEMENT_POSTCODE, MAIN.LSOA2011_HOME_POSTCODE, 
      ALF.WOB, ALF.GNDR_CD, EDUW.WOB AS IRN_WOB,
      CASE WHEN EP.LEGAL_STATUS_CODE = 'E1' THEN 'Placement Order'
          WHEN EP.LEGAL_STATUS_CODE = 'C2' THEN 'Care Order'
          ELSE EP.LEGAL_STATUS_CODE
      END AS LEGAL_STATUS_CODE
    FROM {mlac_epi} AS EP
    LEFT JOIN {mlac_main} AS MAIN
      ON EP.UNIFIED_ID = MAIN.UNIFIED_ID
    AND EP.YEAR_CODE = MAIN.YEAR_CODE
    LEFT JOIN (
        SELECT DISTINCT ALF_E, UNIFIED_ID, WOB, GNDR_CD
        FROM {mlac_alf}
    ) AS ALF
    ON EP.UNIFIED_ID = ALF.UNIFIED_ID
    LEFT JOIN {eduw_alf} AS EDUW
    ON MAIN.IRN_E = EDUW.IRN_E
    WHERE EP.REASON_EPISODE_STARTED_CODE IN ('B', 'L')
      AND EP.EPISODE_START_DATE >= '2012-01-01'
      AND EP.LEGAL_STATUS_CODE IN ('C2','E1')
      AND ALF.ALF_E IS NULL
      AND EP.LOCAL_AUTHORITY_CODE NOT IN (522, 516)
      AND EP.EPISODE_START_DATE < '{maximum_hearing_date}'
      AND ((EP.EPISODE_END_DATE IS NOT NULL AND EP.YEAR_CODE != 202021) OR EP.YEAR_CODE = 202021)
  ''')

def _get_cafcass(mlac_alf, cafalf, cafhoa, cafho, cafhear, cafapp, cafasu):
  return al.call(f'''
    SELECT ALF.SYSTEM_ID_E, ALF.ALF_E, ALF.ALF_STS_CD, ALF.ALF_MTCH_PCT, 
      ALF.WOB, ALF.GNDR_CD, ALF.LSOA2011_CD, HEAR.CAP_HEARINGDATETIME, 
      HEAR.CAP_COURTIDNAME, HO.CAP_HEARINGOUTCOMEFINALNAME, 
      HO.CAP_HEARINGOUTCOMETYPEIDNAME, APP.CAP_DATEOFCOMPLETION, 
      APP.CAP_ISSUEDATE, APP.CAP_RECEIPTDATE, APP.CAP_PRIMARYAPPLICATIONTYPENAME
    FROM {cafhear} AS HEAR
    INNER JOIN {cafho} AS HO
      ON HEAR.CAP_HEARINGID_E = HO.CAP_HEARINGID_E
    AND HEAR.CAP_CASEID_E = HO.CAP_CASEID_E
    INNER JOIN {cafhoa} AS HOA
      ON HEAR.CAP_CASEID_E = HOA.CAP_CASEID_E
    AND HO.CAP_HEARINGOUTCOMEID_E = HOA.CAP_HEARINGOUTCOMEID_E
    INNER JOIN {cafapp} AS APP
      ON HOA.CAP_APPLICATIONID_E = APP.CAP_APPLICATIONID_E
    AND HOA.CAP_CASEID_E = APP.CAP_INCIDENTID_E
    INNER JOIN (
        SELECT * 
        FROM {cafasu}
        WHERE UPPER(RECORD2ROLEIDNAME) = 'SUBJECT'
    ) AS ASU
      ON APP.CAP_APPLICATIONID_E = ASU.RECORD1ID_E
    INNER JOIN {cafalf} AS ALF
      ON ASU.RECORD2ID_E = ALF.SYSTEM_ID_E
    AND APP.CAP_LAWTYPENAME = 'Public'
    AND HO.CAP_HEARINGOUTCOMEFINALNAME = 'Yes'
    LEFT JOIN {mlac_alf} AS LAC
    ON ALF.ALF_E = LAC.ALF_E
    WHERE LAC.ALF_E IS NULL;
  ''')

def _perform_record_linkage(lac_epi, cafcass, tmp_cafalf):
  # Perform record linkage on WOB and GNDR_CD from the UNIFIED_ID
  indexer = recordlinkage.Index()
  indexer.block(['wob', 'gndr_cd'])
  candidate_links = indexer.index(lac_epi, cafcass)

  comparator = recordlinkage.Compare()
  comparator.exact('wob', 'wob', label='wob')
  comparator.exact('gndr_cd', 'gndr_cd', label='gndr_cd')
  comparator.exact('episode_start_date', 'cap_hearingdatetime', label='hearing_date')
  comparator.exact('legal_status_code', 'cap_hearingoutcometypeidname', label='hearing_outcome')
  
  features = comparator.compute(candidate_links, lac_epi, cafcass)

  # Get records with only 1 match
  matches = features[features.sum(axis=1) == 4].reset_index()
  matches = matches.merge(lac_epi, left_on='level_0', right_index=True) \
    .merge(cafcass, left_on='level_1', right_index=True)

  rows_to_keep = matches.groupby(['unified_id'])['system_id_e'] \
    .count() \
    .to_frame() \
    .reset_index()
  rows_to_keep = list(
    rows_to_keep[rows_to_keep['system_id_e'] == 1]['unified_id'].unique()
  )

  final_matches = matches[matches['unified_id'].isin(rows_to_keep)]

  # Perform record linkage using WOB and GNDR_CD from the the IRN_E
  lac_epi = lac_epi[~lac_epi['unified_id'].isin(list(matches['unified_id'].unique()))] \
    .reset_index() \
    .drop(columns=['index'])
  cafcass = cafcass[~cafcass['system_id_e'].isin(list(matches['system_id_e'].unique()))] \
    .reset_index() \
    .drop(columns=['index'])
  
  indexer = recordlinkage.Index()
  indexer.block(['gndr_cd'])
  candidate_links = indexer.index(lac_epi, cafcass)

  comparator = recordlinkage.Compare()
  comparator.exact('irn_wob', 'wob', label='wob')
  comparator.exact('gndr_cd', 'gndr_cd', label='gndr_cd')
  comparator.exact('episode_start_date', 'cap_hearingdatetime', label='hearing_date')
  comparator.exact('legal_status_code', 'cap_hearingoutcometypeidname', label='hearing_outcome')

  features = comparator.compute(candidate_links, lac_epi, cafcass)

  # Get records with only 1 match
  matches = features[features.sum(axis=1) == 4].reset_index()
  matches = matches.merge(lac_epi, left_on='level_0', right_index=True) \
    .merge(cafcass, left_on='level_1', right_index=True)

  rows_to_keep = matches.groupby(['unified_id'])['system_id_e'] \
    .count() \
    .to_frame() \
    .reset_index()
  rows_to_keep = list(
    rows_to_keep[rows_to_keep['system_id_e'] == 1]['unified_id'].unique()
  )

  matches = matches[matches['unified_id'].isin(rows_to_keep)]

  # Clean up
  final_matches = pd.concat([final_matches, matches])
  final_matches = final_matches.reset_index() \
    .drop(columns=[
      'index', 'level_0', 'level_1', 'wob_x', 'gndr_cd_x', 'hearing_date', 
      'hearing_outcome', 'la_name', 'episode_start_date', 'legal_status_code',
      'lsoa2011_placement_postcode', 'lsoa2011_home_postcode', 'wob_y', 
      'gndr_cd_y', 'irn_wob', 'lsoa2011_cd', 'cap_hearingdatetime', 
      'cap_courtidname', 'cap_hearingoutcomefinalname', 
      'cap_hearingoutcometypeidname', 'cap_dateofcompletion', 'cap_issuedate', 
      'cap_receiptdate', 'cap_primaryapplicationtypename'
    ])
  
  # Upload to the DB
  utils.drop_if_exists(tmp_cafalf)

  schema, table = utils.split_schema_table(tmp_cafalf)
  al.make_table(final_matches, schema, table)

def _update_mlac_alf(tmp_cafalf, tmp_cafref, tmp_twins, mlac_alf, cafalf, cafrel, ncchd, wdsdalf):
  utils.drop_if_exists(tmp_cafref)
  utils.drop_if_exists(tmp_twins)

  # Create temporary CAFCASS reference table
  al.call_no_return(f'''
    CREATE TABLE {tmp_cafref} AS (
      SELECT palf.system_id_e AS parent_system_id,
          palf.alf_sts_cd AS parent_alf_sts_cd,
          palf.alf_mtch_pct AS parent_alf_mtch_pct,
          palf.alf_e AS parent_alf,
          palf.lsoa2011_cd AS parent_lsoa,
          palf.wob AS parent_wob,
          palf.gndr_cd AS parent_gndr,
          chalf.system_id_e AS child_system_id,
          chalf.alf_sts_cd AS child_alf_sts_cd,
          chalf.alf_mtch_pct AS child_alf_mtch_pct,
          chalf.alf_e AS child_alf,
          chalf.lsoa2011_cd AS child_lsoa,
          chalf.wob AS child_wob,
          chalf.gndr_cd AS child_gndr
      FROM {cafrel} as rel
      INNER JOIN {cafalf} as palf
        ON rel.record1id_e = palf.system_id_e
      INNER JOIN {cafalf} AS chalf
        ON rel.record2id_e = chalf.system_id_e
      WHERE UPPER(rel.record1roleidname) = 'PARENT'
        AND UPPER(rel.record2roleidname) = 'CHILD'
    ) WITH DATA;
  ''')
  
  # Flag twins
  al.call_no_return(f'''
    CREATE TABLE {tmp_twins} AS (
      SELECT *
      FROM (
        SELECT MATCH.UNIFIED_ID, MATCH.SYSTEM_ID_E, 
          NCH.ALF_E, NCH.BIRTH_ORDER,
          CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS in_lac
        FROM {ncchd} AS nch
        JOIN (
          SELECT DISTINCT ALF.UNIFIED_ID, ALF.SYSTEM_ID_E, REF.PARENT_ALF, ALF.WOB
          FROM {tmp_cafalf} AS ALF
          JOIN {tmp_cafref} AS REF
            ON ALF.SYSTEM_ID_E = REF.CHILD_SYSTEM_ID
          AND REF.PARENT_GNDR = 2
          AND REF.PARENT_ALF IS NOT NULL
          WHERE ALF.ALF_E IS NULL
        ) AS MATCH
          ON NCH.MAT_ALF_E = MATCH.PARENT_ALF
        AND NCH.WOB = MATCH.WOB
        LEFT JOIN {wdsdalf} AS wds
          ON nch.alf_e = wds.alf_e
        LEFT JOIN {cafalf} AS caf
          ON nch.alf_e = caf.alf_e
        LEFT JOIN {mlac_alf} AS lac
          ON nch.alf_e = lac.alf_e
        WHERE caf.alf_e IS NULL
      )
      WHERE IN_LAC != 1
    ) WITH DATA;
  ''')

  # Update matches table
  al.call_commit(f'''
    ALTER TABLE {tmp_cafalf}
      ADD COLUMN IS_TWIN INT DEFAULT 0
  ''')

  al.call_commit(f'''
    MERGE INTO {tmp_cafalf} AS TRG
    USING (
      SELECT *
      FROM (
        SELECT CAF.UNIFIED_ID, CAF.SYSTEM_ID_E,
          FIRST_VALUE(CAF.ALF_E) OVER ( 
            PARTITION BY CAF.UNIFIED_ID 
            ORDER BY CAF.BIRTH_ORDER, CAF.ALF_E
          ) AS ALF_E, 
          CAF.IN_LAC,
          CASE WHEN TWINS.UNIFIED_ID IS NULL THEN 0 ELSE 1 END AS IS_TWIN
        FROM {tmp_twins} AS CAF
        LEFT JOIN (
          SELECT UNIFIED_ID, MAX(BIRTH_ORDER) AS NUM_BIRTHS
          FROM {tmp_twins}
          GROUP BY UNIFIED_ID
          HAVING COUNT(*) > 1
        ) AS TWINS
        USING (UNIFIED_ID)
        WHERE TWINS.UNIFIED_ID IS NULL OR TWINS.NUM_BIRTHS > 1
      )
      GROUP BY UNIFIED_ID, SYSTEM_ID_E, ALF_E, IN_LAC, IS_TWIN
    ) AS SRC
      ON TRG.UNIFIED_ID = SRC.UNIFIED_ID
    WHEN MATCHED THEN
      UPDATE SET IS_TWIN = SRC.IS_TWIN;
  ''')

  al.call_commit(f''' 
    MERGE INTO {tmp_cafalf} AS TRG
    USING (
      SELECT *
      FROM (
        SELECT MATCH.UNIFIED_ID, MATCH.SYSTEM_ID_E, 
          FIRST_VALUE(NCH.ALF_E) OVER ( 
            PARTITION BY MATCH.UNIFIED_ID 
            ORDER BY NCH.BIRTH_ORDER
          ) AS ALF_E,
          CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS in_lac
        FROM {ncchd} AS nch
        JOIN (
          SELECT DISTINCT ALF.UNIFIED_ID, ALF.SYSTEM_ID_E, REF.PARENT_ALF, ALF.WOB
          FROM {tmp_cafalf} AS ALF
          JOIN {tmp_cafref} AS REF
            ON ALF.SYSTEM_ID_E = REF.CHILD_SYSTEM_ID
          AND REF.PARENT_GNDR = 2
          AND REF.PARENT_ALF IS NOT NULL
          WHERE ALF.ALF_E IS NULL
        ) AS MATCH
          ON NCH.MAT_ALF_E = MATCH.PARENT_ALF
        AND NCH.WOB = MATCH.WOB
        LEFT JOIN {wdsdalf} AS wds
          ON nch.alf_e = wds.alf_e
        LEFT JOIN {cafalf} AS caf
          ON nch.alf_e = caf.alf_e
        LEFT JOIN {mlac_alf} AS lac
          ON nch.alf_e = lac.alf_e
        WHERE caf.alf_e IS NULL
      )
      WHERE IN_LAC != 1
      GROUP BY UNIFIED_ID, SYSTEM_ID_E, ALF_E, IN_LAC
    ) AS SRC
      ON TRG.UNIFIED_ID = SRC.UNIFIED_ID
    WHEN MATCHED THEN
      UPDATE SET TRG.ALF_E = SRC.ALF_E,
        ALF_STS_CD = 1;
  ''')

  # Finally, update MLAC ALF table
  al.call_commit(f'''
    MERGE INTO {mlac_alf} AS TRG
    USING (
      SELECT *
      FROM {tmp_cafalf}
      WHERE ALF_STS_CD != 99
    ) AS SRC
      ON TRG.UNIFIED_ID  = SRC.UNIFIED_ID
    WHEN MATCHED THEN
      UPDATE SET TRG.ALF_E = SRC.ALF_E,
        TRG.ALF_STS_CD = NULL,
        TRG.ALF_MTCH_PCT = NULL,
        TRG.ALF_SOURCE = 'CAFW',
        TRG.LAC_ALF_MATCH = 1,
        TRG.LAC_MATCH_TWIN = SRC.IS_TWIN;
  ''')

def link_cafcass(options, base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')
  mlac_epi = created_tables.get('mlac_epi')
  eduw_alf = base_tables.get('eduw')
  ncchd = base_tables.get('ncchd')
  wdsdalf = base_tables.get('wdsd_pers')

  cafalf = base_tables.get('cafalf')
  cafhoa = base_tables.get('cafhoa')
  cafho = base_tables.get('cafho')
  cafhear = base_tables.get('cafhear')
  cafapp = base_tables.get('cafapp')
  cafasu = base_tables.get('cafasu')
  cafrel = base_tables.get('cafrel')

  tmp_cafalf = f'''{options.get('WORKING_SCHEMA')}.TMP_CAFCASS_ALF_TBL'''
  tmp_cafref = f'''{options.get('WORKING_SCHEMA')}.TMP_CAFCASS_REF_TBL'''
  tmp_twins = f'''{options.get('WORKING_SCHEMA')}.TMP_CHECK_CAF'''

  # Get LAC episode data
  lac_epi = _get_lac_epi(mlac_alf, mlac_main, mlac_epi, eduw_alf, cafhear)

  # Get CAFCASS data
  cafcass = _get_cafcass(mlac_alf, cafalf, cafhoa, cafho, cafhear, cafapp, cafasu)

  # Perform CAFCASS record linkage
  _perform_record_linkage(lac_epi, cafcass, tmp_cafalf)

  # Update MLAC ALF
  _update_mlac_alf(tmp_cafalf, tmp_cafref, tmp_twins, mlac_alf, cafalf, cafrel, ncchd, wdsdalf)

  # Cleanup
  utils.drop_if_exists(tmp_cafalf)
  utils.drop_if_exists(tmp_cafref)
  utils.drop_if_exists(tmp_twins)
