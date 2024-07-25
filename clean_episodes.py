from tqdm import tqdm

import helper_funcs.db_conn as al
import utils

def _create_initial_table(tmp_cepi, mlac_alf, mlac_epi): # 9999-12-31
  utils.drop_if_exists(tmp_cepi)

  al.call_no_return(f'''
    CREATE TABLE {tmp_cepi} AS (
      SELECT *, 
        CASE WHEN placement_localauth IS NULL AND placement_in_la = 1 THEN la_name
        ELSE placement_localauth
        END AS placement_la
      FROM (
        SELECT lem.unified_id, lem.sys_id_changes, lem.hybrid_id, 
          lem.EPISODE_START_DATE, 
          coalesce(lem.EPISODE_END_DATE, '2099-09-09') AS EPISODE_END_DATE, 
          CASE WHEN upper(lem.REASON_EPISODE_STARTED_CODE) != 'L' THEN 1
            ELSE 0
          END AS is_placement_move,
          CASE WHEN lem.LSOA2011_PLACEMENT_POSTCODE IS NULL THEN 'X'
            ELSE lem.LSOA2011_PLACEMENT_POSTCODE
          END AS lsoa2011_placement_postcode, 
          lem.la_name, lem.placement_area, lem.placement_localauth,
          CASE WHEN UPPER(lem.PLACEMENT_TYPE_CODE) IN ('F1', 'F2', 'F3', 'H1', 'H3') THEN 1
            WHEN UPPER(lem.PLACEMENT_TYPE_CODE) IN ('F4', 'F5', 'F6', 'H21', 'H22', 'H4') THEN 0
            ELSE 2 END AS placement_in_la,
          CASE WHEN UPPER(lem.CATEGORY_OF_NEED_CODE) = 'N9' THEN 1
            ELSE 0 
          END AS adopt_disrupt,
          al.gndr_cd, al.wob 
        FROM {mlac_epi} lem 
        LEFT JOIN {mlac_alf} al
          ON lem.unified_id = al.unified_id
      )
    ) WITH DATA;
  ''')

def _update_values(tmp_cepi, mlac_cepi):
  data = al.call(f'''
    SELECT *
    FROM {tmp_cepi}
    ORDER BY EPISODE_START_DATE ASC, EPISODE_END_DATE ASC,
      LSOA2011_PLACEMENT_POSTCODE ASC, PLACEMENT_IN_LA DESC
  ''')
  data = data.groupby(['episode_start_date', 'unified_id', 'gndr_cd', 'wob']) \
    .agg({
      'episode_end_date':'first', 
      'is_placement_move':'first', 
      'lsoa2011_placement_postcode':'min',
      'placement_in_la':'first', 
      'la_name':'first', 
      'placement_area':'first', 
      'placement_la':'first', 
      'adopt_disrupt':'first'
    }).reset_index()
  data['la_name'] = data['la_name'].str.strip()
  data['placement_la'] = data['placement_la'].str.strip(' ')

  uids = list(data['unified_id'].unique())
  for person in tqdm(uids):
    epi_data = list(data[data['unified_id'] == person].index)
    epi_data.reverse()

    for i in range(len(epi_data) - 1):
      current_row = data.iloc[epi_data[i]]
      previous_row = data.iloc[epi_data[i+1]]

      if current_row['placement_la'] is None:
        placeholder = i
        # Do nothing?
      elif current_row['placement_la'].strip(' ').lower() == current_row['la_name'].strip(' ').lower():
        if current_row['placement_in_la'] == 0:
          data.at[epi_data[i], 'placement_in_la'] = 3
        elif current_row['placement_in_la'] == 2:
          data.at[epi_data[i], 'placement_in_la'] = 1
      
      current_start = current_row['episode_start_date']
      previous_end = previous_row['episode_end_date']
      if current_row['is_placement_move'] == 0 and current_start == previous_end:
        data.at[epi_data[i+1], 'episode_end_date'] = current_row['episode_end_date']

        previous_lsoa = previous_row['lsoa2011_placement_postcode']
        current_lsoa = current_row['lsoa2011_placement_postcode']
        if current_lsoa != 'X' and previous_lsoa == 'X':
          data.at[epi_data[i+1], 'lsoa2011_placement_postcode'] = current_lsoa
          data.at[epi_data[i+1], 'placement_area'] = current_row['placement_area']
          data.at[epi_data[i+1], 'placement_la'] = current_row['placement_la']

  # Upload to DB
  utils.drop_if_exists(mlac_cepi)

  schema, table = utils.split_schema_table(mlac_cepi)
  al.make_table(data, schema, table)

  # Add time_placement
  al.call_commit(f'''
    ALTER TABLE {mlac_cepi}
      ADD COLUMN TIME_PLACEMENT INTEGER;
  ''')

  al.call_commit(f'''
    UPDATE {mlac_cepi}
      SET TIME_PLACEMENT = DAYS(EPISODE_END_DATE) - DAYS(EPISODE_START_DATE);
  ''')

  # Clean up
  utils.drop_if_exists(tmp_cepi)

def create_table(options, base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_epi = created_tables.get('mlac_epi')
  mlac_cepi = created_tables.get('mlac_cepi')

  working_schema = options.get('WORKING_SCHEMA')
  tmp_cepi = f'{working_schema}.TMP_MLAC_CEPI'

  # Create the initial cleaned table
  _create_initial_table(tmp_cepi, mlac_alf, mlac_epi)

  # Update episodes
  _update_values(tmp_cepi, mlac_cepi)
