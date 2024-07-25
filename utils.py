import json

import helper_funcs.db_conn as al

def debug_message(message):
  if DEBUG:
    print(f'[DEBUG]: {message}')

def read_json_file(fpath):
  with open(fpath) as f:
    return json.load(f)

def write_json_file(fpath, data):
  with open(fpath, 'w') as f:
    json.dump(data, f, indent=2)

def split_schema_table(schematable):
  return schematable.split('.')[0], schematable.split('.')[1]

def table_exists(schematable):
  schema, tabname = split_schema_table(schematable)

  query = '''
  SELECT COUNT(*) AS N 
  FROM SYSCAT.TABLES 
  WHERE TABSCHEMA = '{0}' 
    AND tabname = '{1}';
  '''.format(schema, tabname)

  result = al.call(query)['n'].iloc[0]
  return result > 0

def drop_if_exists(schematable):
  if table_exists(schematable):
    al.call_commit(f'DROP TABLE {schematable};')

def ulist(ser):
  return list(ser.unique())
