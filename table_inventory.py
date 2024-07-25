from datetime import datetime
import pandas as pd

import helper_funcs.db_conn as al
import utils

def get_tables(options, filepath=None):
  # Load table inventory if filepath provided
  if filepath:
    data = utils.read_json_file(filepath)

    base_tables = { k: v if isinstance(v, str) else v['fullname'] for k, v in data['base'].items() }
    created_tables = { k: v if isinstance(v, str) else v['fullname'] for k, v in data['create'].items() }

    return base_tables, created_tables
  
  # If filepath wasn't provided, create a new table inventory file
  schema = options.get('SCHEMA')
  working_schema = options.get('WORKING_SCHEMA')

  data = utils.read_json_file('./data/table_inventory.json')

  base_tables = data.get('base')
  for key, value in base_tables.items():
    current_schema = value.get('name').split('.')[0]
    regexpr = value.get('name').split('.')[1] + r'_\d{8}'

    fullname = al.call(f'''
      SELECT TABNAME
      FROM syscat.tables
      WHERE TABSCHEMA = '{current_schema}'
      AND REGEXP_LIKE(TABNAME, '{regexpr}', 'i')
      ORDER BY CREATE_TIME DESC
    ''').iloc[0]['tabname']

    base_tables[key] = f'{current_schema}.{fullname}'
  
  # Created tables
  today = datetime.now()
  formatted_today = '{:%Y%m%d}'.format(today)

  created_tables = data.get('create')
  for key, value in created_tables.items():
    table_name = value.get('name')
    if table_name:
      table_name = table_name.rsplit('_', 1)[0]
      created_tables[key] = f'{working_schema}{table_name}_{formatted_today}'
    else:
      created_tables[key] = value.get('fullname')

  formatted_today = '{:%Y-%m-%d}'.format(today)
  utils.write_json_file(
    f'./data/table_inventory_{formatted_today}.json', 
    { 'base': base_tables, 'create': created_tables }
  )
  
  return base_tables, created_tables

def create_helper_tables(base_tables, created_tables):
  # Create lookup tables
  educ_la_decode = created_tables.get('educ_la_decode')
  if not utils.table_exists(educ_la_decode):
    data = pd.read_excel('./data/helper_tables/nlac-2011.xls')
    data.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    schema, table = utils.split_schema_table(educ_la_decode)
    al.make_table(data, schema, table)
  
  la_codes = created_tables.get('la_codes')
  if not utils.table_exists(la_codes):
    data = pd.read_csv('./data/helper_tables/LACODES.csv')

    schema, table = utils.split_schema_table(la_codes)
    al.make_table(data, schema, table)

  lsoa_decode = created_tables.get('lsoa_decode')
  if not utils.table_exists(lsoa_decode):
    data = pd.read_csv('./data/helper_tables/LSOA_LA.csv')

    schema, table = utils.split_schema_table(lsoa_decode)
    al.make_table(data, schema, table)
  
  # Create GP event reference tables
  gp_event_table = base_tables.get('gp_event')

  data = utils.read_json_file('./data/helper_tables/gp_read_codes.json')
  for key, value in data.items():
    read_codes_list = ','.join([ f'\'{x}\'' for x in value.keys()])

    tableschema = created_tables[f'gpev_{key}']
    if not utils.table_exists(tableschema):
      query = f'''
      CREATE TABLE {tableschema} AS (
        SELECT DISTINCT ALF_E
        FROM {gp_event_table}
        WHERE EVENT_CD IN ({read_codes_list})
      ) WITH DATA;
      '''
      al.call_no_return(query)
