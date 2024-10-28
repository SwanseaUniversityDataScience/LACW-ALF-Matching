import pandas as pd
import numpy as np
import recordlinkage

import helper_funcs.db_conn as al
import utils

def _get_mlac_duplicates(mlac_alf, mlac_main, eduw_alf):
  result = al.call(f'''
    WITH ALF AS (
      SELECT HYBRID_ID, 
        COUNT(*) AS NUMOCC, 
        COUNT(DISTINCT(WOB)) AS N_WOB,
        COUNT(DISTINCT(GNDR_CD)) AS N_GNDR,
        COUNT(DISTINCT(ALF_E)) AS N_ALF
      FROM {mlac_alf}
        GROUP BY HYBRID_ID
    ),
    MAIN AS (
      SELECT HYBRID_ID, MAX(IRN_E) AS IRN_E
      FROM {mlac_main}
      GROUP BY HYBRID_ID
    ),
    DUPLICATES AS (
      SELECT IRN_E, ALF.*
      FROM ALF AS ALF
      JOIN MAIN AS MAIN
        USING (HYBRID_ID)
      WHERE NUMOCC > 1
    )
    SELECT EDUW.ALF_E AS ALF_E, EDUW.GNDR_CD, DUPLICATES.*
    FROM {eduw_alf} AS EDUW
    JOIN DUPLICATES AS DUPLICATES
      USING (IRN_E);
  ''')
  result['alf_e'] = result['alf_e'].astype('Int64')

  return result

def _remove_mlac_duplicates(mlac_alf, mlac_main, eduw_alf):
  # Get duplicates in mlac tables
  duplicates = _get_mlac_duplicates(mlac_alf, mlac_main, eduw_alf)

  # Delete duplicates with >1 ALF
  duplicate_alfs = duplicates[duplicates['n_alf'] > 1]
  for _, row in duplicate_alfs.iterrows():
    al.call_no_return(f'''
      DELETE FROM {mlac_alf}
      WHERE ALF_E != {row['alf_e']}
        AND HYBRID_ID = '{row['hybrid_id']}'
    ''')

  # Delete duplicates with >1 GNDR_CD
  duplicate_gndr = duplicates[duplicates['gndr_cd'] > 1]
  for _, row in duplicate_gndr.iterrows():
    al.call_no_return(f'''
      DELETE FROM {mlac_alf}
      WHERE GNDR_CD != {row['gndr_cd']}
        AND HYBRID_ID = '{row['hybrid_id']}'
    ''')

def _insert_mlac_missing_records(mlac_alf, mlac_main, eduw_alf, mlac_epi):
  al.call_no_return(f'''
    ALTER TABLE {mlac_alf}
      ADD COLUMN MISSING_FROM_LACALF INT DEFAULT 0
      ADD COLUMN ALF_SOURCE VARCHAR(255) DEFAULT 'DHCW'
      ADD COLUMN LAC_ALF_MATCH INT DEFAULT 0
      ADD COLUMN LAC_MATCH_TWIN INT DEFAULT 0;
  ''')

  al.call_no_return(f'''
    INSERT INTO {mlac_alf} (
      HYBRID_ID, SYSTEM_ID_E, LOCAL_AUTHORITY_CODE, ALF_E, ALF_MTCH_PCT, 
      ALF_STS_CD, GNDR_CD, ORIGINAL_LAC_WOB, WOB, MAX_POSS_WOB, 
      POTENTIAL_UNKNOWN_WOB, HAS_WOB_BEEN_CORRECTED, LACWOB_DEF_WRONG, 
      FIRST_EP_START, MISSING_FROM_LACALF, ALF_SOURCE, LAC_ALF_MATCH, 
      LAC_MATCH_TWIN
    )
      WITH EPISODES AS (
          SELECT HYBRID_ID, MIN(EPISODE_START_DATE) AS FSTART
          FROM {mlac_epi}
          GROUP BY HYBRID_ID
      )
      SELECT DISTINCT MAIN.HYBRID_ID, MAIN.SYSTEM_ID_E, MAIN.LOCAL_AUTHORITY_CODE, 
        CASE WHEN EDUW.ALF_STS_CD != 99 THEN EDUW.ALF_E
          ELSE NULL
        END AS ALF_E,
        EDUW.ALF_MTCH_PCT, EDUW.ALF_STS_CD, EDUW.GNDR_CD, 
        NULL AS ORIGINAL_LAC_WOB,
        EDUW.WOB, 
        CASE WHEN EDUW.IRN_E IS NOT NULL THEN NULL
            ELSE EPIS.FSTART
        END AS MAX_POSS_WOB, 
        CASE WHEN EDUW.IRN_E IS NOT NULL THEN 0
            ELSE 1
        END AS POTENTIAL_UNKNOWN_WOB, 
        NULL AS HAS_WOB_BEEN_CORRECTED,
        1 AS LACWOB_DEF_WRONG,
        EPIS.FSTART AS FIRST_EP_START,
        1 AS MISSING_FROM_LACALF,
        CASE WHEN EDUW.IRN_E IS NOT NULL AND EDUW.ALF_STS_CD != 99 THEN 'EDUW'
            ELSE NULL
        END AS ALF_SOURCE, 
        CASE WHEN EDUW.IRN_E IS NOT NULL AND EDUW.ALF_STS_CD != 99 THEN 1
             ELSE 0
        END AS LAC_ALF_MATCH,
        0
      FROM {mlac_main} AS MAIN
      LEFT JOIN {mlac_alf} AS ALF
        USING (HYBRID_ID)
      LEFT JOIN {eduw_alf} AS EDUW
        ON MAIN.IRN_E = EDUW.IRN_E
      LEFT JOIN EPISODES AS EPIS
        USING (HYBRID_ID)
      WHERE ALF.WOB IS NULL;
  ''')

def fix_mlac_tables(base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')
  mlac_epi = created_tables.get('mlac_epi')
  eduw_alf = base_tables.get('eduw')

  # Handle duplicate rows
  _remove_mlac_duplicates(mlac_alf, mlac_main, eduw_alf)

  # Insert records missing from MLAC ALF
  _insert_mlac_missing_records(mlac_alf, mlac_main, eduw_alf, mlac_epi)

def _get_missing_episode_data(mlac_alf, mlac_main, mlac_epi):
  query = '''
    WITH EPI AS (
      SELECT *
      FROM (
        SELECT ROW_NUMBER() OVER (
            PARTITION BY HYBRID_ID
            ORDER BY YEAR_CODE %(order)s, EPISODE_NUMBER %(order)s
          ) AS ROW_NUM,
          HYBRID_ID, SYSTEM_ID_E, YEAR_CODE, LOCAL_AUTHORITY_CODE, 
          EPISODE_START_DATE, PLACEMENT_TYPE_CODE, CATEGORY_OF_NEED_CODE, 
          EPISODE_END_DATE, LEGAL_STATUS_CODE, REASON_EPISODE_STARTED_CODE, 
          REASON_EPISODE_FINISHED_CODE, LSOA2011_PLACEMENT_POSTCODE
        FROM %(mlac_epi)s 
        ORDER BY HYBRID_ID, YEAR_CODE, EPISODE_NUMBER
      )
      WHERE ROW_NUM = 1 %(filter)s
    ),
    MAIN AS (
      SELECT DISTINCT HYBRID_ID, %(irn_stmt)s, MAX(ASYLUM) AS EVER_ASYLUM
      FROM %(mlac_main)s
      GROUP BY HYBRID_ID %(irn_group)s
    ),
    MAINHP AS (
      SELECT *
      FROM (
        SELECT HYBRID_ID, LSOA2011_HOME_POSTCODE,
          ROW_NUMBER() OVER (
            PARTITION BY HYBRID_ID 
            ORDER BY YEAR_CODE %(order)s
          ) AS ROW_NUM
        FROM %(mlac_main)s
      )
      WHERE ROW_NUM = 1
    )
    SELECT EPI.*, 
      ALF.ORIGINAL_LAC_WOB AS WOB, ALF.GNDR_CD, MAIN.IRN_E, 
      MAINHP.LSOA2011_HOME_POSTCODE, ALF.ALF_E, ALF.ALF_STS_CD, MAIN.EVER_ASYLUM
    FROM EPI AS EPI
    LEFT JOIN MAIN AS MAIN
      USING (HYBRID_ID)
    LEFT JOIN MAINHP AS MAINHP
      USING (HYBRID_ID)
    LEFT JOIN %(mlac_alf)s AS alf
      USING (HYBRID_ID);
  '''
  
  missing_start = al.call(query % {
    'order': 'asc',
    'mlac_epi': mlac_epi,
    'mlac_main': mlac_main,
    'mlac_alf': mlac_alf,
    'irn_stmt': 'irn_e',
    'irn_group': ', irn_e',
    'filter': '''
      AND YEAR_CODE > 200304
      AND UPPER(STRIP(REASON_EPISODE_STARTED_CODE, BOTH)) != 'S'
    '''
  })

  missing_end = al.call(query % {
    'order': 'desc',
    'mlac_epi': mlac_epi,
    'mlac_main': mlac_main,
    'mlac_alf': mlac_alf,
    'irn_stmt': 'irn_e',
    'irn_group': ', irn_e',
    'filter': '''
      AND YEAR_CODE != 202021
      AND EPISODE_END_DATE IS NULL
    '''
  })

  all_first = al.call(query % {
    'order': 'asc',
    'mlac_epi': mlac_epi,
    'mlac_main': mlac_main,
    'mlac_alf': mlac_alf,
    'irn_stmt': 'MAX(IRN_E) AS IRN_E',
    'irn_group': '',
    'filter': ''
  })
  all_first = all_first[
    ~all_first['hybrid_id'].isin(list(missing_end['hybrid_id'].unique()))
  ]

  return all_first, missing_start, missing_end

def _add_to_match_result(df, earlier, later, all_first):
  tser = pd.DataFrame({'earlier_sysid': [earlier], 'later_sysid': [later]})
  udf = pd.concat([df, tser], ignore_index=True, axis=0)
  
  all_first = all_first[all_first['hybrid_id'] != later]
  return udf, all_first

def _perform_record_linkage(options, mlac_map, mlac_main, all_first, missing_start, missing_end):
  all_first_index = all_first.set_index('hybrid_id')
  missing_start_index = missing_start.set_index('hybrid_id')
  missing_end_index = missing_end.set_index('hybrid_id')

  indexer = recordlinkage.Index()
  indexer.block([
    'episode_start_date', 'local_authority_code', 'wob', 'gndr_cd',
    'legal_status_code', 'reason_episode_started_code'
  ])
  candidate_links = indexer.index(missing_end_index, all_first_index)

  comparator = recordlinkage.Compare()
  comparator.exact('wob', 'wob', label='wob')
  comparator.exact('gndr_cd', 'gndr_cd', label='gndr_cd')
  comparator.exact('local_authority_code', 'local_authority_code', label='loc_auth_code')
  comparator.exact('episode_start_date', 'episode_start_date', label='episode_start_date')
  comparator.exact('reason_episode_started_code', 'reason_episode_started_code', label='reason_episode_started_code')
  comparator.exact('placement_type_code', 'placement_type_code', label='placement_type_code')
  comparator.exact('legal_status_code', 'legal_status_code', label='legal_status_code')
  comparator.string('lsoa2011_placement_postcode', 'lsoa2011_placement_postcode', threshold=1, label='placement_lsoa')
  comparator.string('lsoa2011_placement_postcode', 'lsoa2011_home_postcode', threshold=1, label='home_lsoa')

  features = comparator.compute(
    candidate_links,
    missing_end_index,
    all_first_index
  )
  matches = features[features.sum(axis=1) >= 7].reset_index()

  # Get records with only 1 match
  result_match = matches.groupby(['hybrid_id_1'])['hybrid_id_2'] \
    .count() \
    .to_frame() \
    .reset_index()
  result_match = list(
    result_match[result_match['hybrid_id_2'] == 1]['hybrid_id_1'].unique()
  )
  result_match = matches[matches['hybrid_id_1'].isin(result_match)]
  result_match = result_match[['hybrid_id_1', 'hybrid_id_2']].rename(columns={
    'hybrid_id_1': 'earlier_sysid', 
    'hybrid_id_2': 'later_sysid'
  })

  # Handle records recordlinkage library wasn't able to link
  all_first = all_first[
    (~all_first['hybrid_id'].isin(list(result_match['earlier_sysid'].unique()))) &
    (~all_first['hybrid_id'].isin(list(result_match['later_sysid'].unique())))
  ]
  start_matches = missing_start[
    (~missing_start['hybrid_id'].isin(list(result_match['later_sysid'].unique())))
  ]
  end_matches = missing_end[
    (~missing_end['hybrid_id'].isin(list(result_match['earlier_sysid'].unique())))
  ]

  for index, row in end_matches.iterrows():
    if int(float(row['ever_asylum'])) == 1:
      continue
    
    # Map on column subset
    next_year = row['year_code'] + 101
    cur_match = all_first[
      (all_first['wob'] == row['wob']) &
      (all_first['gndr_cd'] == row['gndr_cd']) &
      (all_first['local_authority_code'] == row['local_authority_code']) &
      (all_first['year_code'] == next_year) &
      (all_first['episode_start_date'] == row['episode_start_date']) & 
      (all_first['legal_status_code'] == row['legal_status_code'])
    ]
    if len(cur_match) == 1:
      result_match, all_first = _add_to_match_result(
        result_match, row['hybrid_id'], cur_match['hybrid_id'].iloc[0], all_first
      )
      continue
    
    # Map on column subset
    cur_match = all_first[
      (all_first['local_authority_code'] == row['local_authority_code']) &
      (all_first['year_code'] == next_year) &
      (all_first['episode_start_date'] == row['episode_start_date']) & 
      (all_first['legal_status_code'] == row['legal_status_code']) &
      (
        (all_first['lsoa2011_placement_postcode'] == row['lsoa2011_placement_postcode']) | 
        (all_first['lsoa2011_home_postcode'] == row['lsoa2011_home_postcode'])
      )
    ]
    if len(cur_match) == 1:
      result_match, all_first = _add_to_match_result(
        result_match, row['hybrid_id'], cur_match['hybrid_id'].iloc[0], all_first
      )
      continue

    # Map further to ethnicity
    if options.get('ETHNICITY_IN_LAC') and len(cur_match) > 1:
      id_list = ','.join([f'\'{x}\'' for x in list(cur_match['hybrid_id'].unique())])
      
      ethnicity_match = al.call(f'''
        WITH PERSON_ETH AS (
          SELECT DISTINCT YEAR_CODE, ETHNICITY
          FROM {mlac_main}
          WHERE HYBRID_ID = '{row['hybrid_id']}'
          ORDER BY YEAR_CODE DESC
          LIMIT 1
        )
        SELECT DISTINCT HYBRID_ID
        FROM {mlac_main}
        WHERE HYBRID_ID IN ({id_list})
          AND ETHNICITY = (SELECT ETHNICITY FROM PERSON_ETH)
      ''')

      cur_match = cur_match[
        cur_match['hybrid_id'].isin(list(ethnicity_match['hybrid_id'].unique()))
      ]

      if len(cur_match) == 1:
        result_match, all_first = _add_to_match_result(
          result_match, row['hybrid_id'], cur_match['hybrid_id'].iloc[0], all_first
        )
        continue
    
    # If there are multiple identical matches, pick the first one
    if len(cur_match) > 1:
      columns_to_match = [
        'year_code', 'local_authority_code', 'episode_start_date', 
        'placement_type_code', 'category_of_need_code', 'legal_status_code',
        'reason_episode_started_code', 'lsoa2011_placement_postcode', 'wob', 
        'gndr_cd', 'lsoa2011_home_postcode'
      ]
      if np.all(cur_match[columns_to_match].iloc[0] == cur_match[columns_to_match].iloc[1]) \
        and cur_match['alf_e'].iloc[0] != cur_match['alf_e'].iloc[1]:
        cur_match = cur_match.iloc[:1]

        if len(cur_match) == 1:
          result_match, all_first = _add_to_match_result(
            result_match, row['hybrid_id'], cur_match['hybrid_id'].iloc[0], all_first
          )
          continue
      
      cur_match = cur_match[
        ~(~cur_match['wob'].isna()) & (cur_match['wob'] != row['wob'])
      ]
      if len(cur_match) == 1:
        result_match, all_first = _add_to_match_result(
          result_match, row['hybrid_id'], cur_match['hybrid_id'].iloc[0], all_first
        )
        continue

  # Create mapping table
  utils.drop_if_exists(mlac_map)
  
  schema, table = utils.split_schema_table(mlac_map)
  al.make_table(result_match, schema, table)

  # TODO: Compare previous mapping table to current one here

def _update_tables_with_linkage(options, mlac_map, tables_to_update):
  working_schema = options.get('WORKING_SCHEMA')

  for schematable in tables_to_update:
    utils.drop_if_exists(f'{working_schema}.TMP_MLAC_MAP_TBL')

    al.call_no_return(f'''
      CREATE TABLE {working_schema}.TMP_MLAC_MAP_TBL AS (
        SELECT *
        FROM {schematable}
      ) WITH DATA;
    ''')

    utils.drop_if_exists(schematable)

    al.call_no_return(f'''
      CREATE TABLE {schematable} AS (
        WITH MAPPED_DATA AS (
          SELECT DATA.*,
            CASE WHEN MAP.EARLIER_SYSID IS NOT NULL OR LMAP.LATER_SYSID IS NOT NULL THEN 1
                 ELSE 0
            END AS SYS_ID_CHANGES,
            MAP.LATER_SYSID AS SYSID_CHANGES_TO,
            LMAP.EARLIER_SYSID AS SYSID_CHANGED_FROM
            FROM {working_schema}.TMP_MLAC_MAP_TBL AS DATA
            LEFT JOIN {mlac_map} AS MAP
              ON DATA.HYBRID_ID = MAP.EARLIER_SYSID
            LEFT JOIN {mlac_map} AS LMAP
              ON DATA.HYBRID_ID = LMAP.LATER_SYSID
        )
        SELECT MAPPED.*,
          CASE WHEN SYSID_CHANGES_TO IS NOT NULL THEN SYSID_CHANGES_TO
              ELSE HYBRID_ID
          END AS UNIFIED_ID
        FROM MAPPED_DATA AS MAPPED 
      ) WITH DATA;
    ''')

    al.call_no_return(f'DROP TABLE {working_schema}.TMP_MLAC_MAP_TBL;')

    # TODO: Compare prev. table and new table here

def fix_record_linkage(options, base_tables, created_tables):
  mlac_map = created_tables.get('mlac_map')
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')
  mlac_epi = created_tables.get('mlac_epi')

  # Get records with missing epsiode start or end date
  all_first, missing_start, missing_end = _get_missing_episode_data(
    mlac_alf, mlac_main, mlac_epi
  )

  # Record linkage
  _perform_record_linkage(
    options, mlac_map, mlac_main, all_first, missing_start, missing_end
  )

  _update_tables_with_linkage(options, mlac_map, [mlac_alf, mlac_main, mlac_epi])

def _fix_wob_and_gndr(mlac_alf):
  ''' 
    Update WOB and GNDR_CD in duplicate rows with the same UNIFIED_IDs 
      but different HYBRID_IDs
  '''

  # TODO: Add logging for number of rows changed

  al.call_commit(f'''
    MERGE INTO {mlac_alf} AS TRG
    USING (
      SELECT MISSING_VALS.HYBRID_ID, 
        FILLED_VALS.ALF_E, FILLED_VALS.ALF_MTCH_PCT, FILLED_VALS.ALF_STS_CD,
        FILLED_VALS.WOB, FILLED_VALS.GNDR_CD, FILLED_VALS.MAX_POSS_WOB,
        FILLED_VALS.POTENTIAL_UNKNOWN_WOB, FILLED_VALS.HAS_WOB_BEEN_CORRECTED,
        FILLED_VALS.ORIGINAL_LAC_WOB, FILLED_VALS.LACWOB_DEF_WRONG,
        FILLED_VALS.FIRST_EP_START, FILLED_VALS.ALF_SOURCE, 
        FILLED_VALS.LAC_ALF_MATCH, FILLED_VALS.LAC_MATCH_TWIN
      FROM (
        SELECT *
        FROM {mlac_alf}
        WHERE SYS_ID_CHANGES = 1
        ORDER BY UNIFIED_ID, HYBRID_ID
      ) AS FILLED_VALS
      JOIN (
        SELECT *
        FROM {mlac_alf}
        WHERE SYS_ID_CHANGES = 1
          AND MISSING_FROM_LACALF = 1
        ORDER BY UNIFIED_ID, HYBRID_ID
      ) AS MISSING_VALS
        ON FILLED_VALS.UNIFIED_ID = MISSING_VALS.UNIFIED_ID
      AND FILLED_VALS.HYBRID_ID <> MISSING_VALS.HYBRID_ID
    ) AS SRC
      ON SRC.HYBRID_ID = TRG.HYBRID_ID
    WHEN MATCHED THEN
      UPDATE SET TRG.ALF_E = SRC.ALF_E,
        TRG.ALF_MTCH_PCT = SRC.ALF_MTCH_PCT,
        TRG.ALF_STS_CD = SRC.ALF_STS_CD,
        TRG.WOB = SRC.WOB,
        TRG.GNDR_CD = SRC.GNDR_CD,
        TRG.MAX_POSS_WOB = SRC.MAX_POSS_WOB,
        TRG.POTENTIAL_UNKNOWN_WOB = SRC.POTENTIAL_UNKNOWN_WOB,
        TRG.HAS_WOB_BEEN_CORRECTED = SRC.HAS_WOB_BEEN_CORRECTED,
        TRG.ORIGINAL_LAC_WOB = SRC.ORIGINAL_LAC_WOB,
        TRG.LACWOB_DEF_WRONG = SRC.LACWOB_DEF_WRONG,
        TRG.FIRST_EP_START = (
          CASE WHEN TRG.SYSID_CHANGES_TO IS NOT NULL THEN TRG.FIRST_EP_START 
               ELSE SRC.FIRST_EP_START 
          END
        ),
        TRG.MISSING_FROM_LACALF = 0,
        TRG.ALF_SOURCE = SRC.ALF_SOURCE,
        TRG.SYS_ID_CHANGES = 1;
  ''')

def _fix_irn(mlac_main):
  ''' 
    Update IRN_E in duplicate rows with the same UNIFIED_IDs 
      but different HYBRID_IDs
  '''

  al.call_commit(f'''
    MERGE INTO {mlac_main} AS TRG
    USING (
      SELECT MISSING_VALS.HYBRID_ID, FILLED_VALS.IRN_E
      FROM (
        SELECT *
        FROM {mlac_main}
        WHERE SYS_ID_CHANGES = 1
          AND IRN_E IS NOT NULL
      ) AS FILLED_VALS
      JOIN (
        SELECT *
        FROM {mlac_main}
        WHERE SYS_ID_CHANGES = 1
          AND IRN_E IS NULL
      ) AS MISSING_VALS
        ON FILLED_VALS.UNIFIED_ID = MISSING_VALS.UNIFIED_ID
      AND FILLED_VALS.HYBRID_ID <> MISSING_VALS.HYBRID_ID
      GROUP BY MISSING_VALS.HYBRID_ID, FILLED_VALS.IRN_E
    ) AS SRC
      ON SRC.HYBRID_ID = TRG.HYBRID_ID
    WHEN MATCHED THEN
      UPDATE SET TRG.IRN_E = SRC.IRN_E;
  ''')

def fix_missing_fields(base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')

  # Fix missing WOBs and GNDR_CDs in MLAC ALF
  _fix_wob_and_gndr(mlac_alf)

  # Fix missing IRNs in MLAC main
  _fix_irn(mlac_main)
