import getpass

import helper_funcs.db_conn as al

# Options
OPTIONS = {
  'ETHNICITY_IN_LAC': True,
  'SCHEMA': 'SAIL1533V',
  'WORKING_SCHEMA': 'SAILW1533V',
  'READ_CODES_LOC': './data/helper_tables/gp_read_codes.json'
}

if __name__ == '__main__':
  # Get username and password
  username = getpass.getuser()
  password = getpass.getpass()

  # Connect to db2
  al.start(username, password)

  # Handle table inventory
  import table_inventory
  base_tables, created_tables = table_inventory.get_tables(
    OPTIONS,
    filepath='./data/table_inventory_2024-06-26.json'
  )

  table_inventory.create_helper_tables(base_tables, created_tables)

  # Create tables
  import make_tables
  make_tables.create_mlac_tables(base_tables, created_tables)
  make_tables.create_epi_table(base_tables, created_tables)
  make_tables.create_mcin_tables(base_tables, created_tables)
  make_tables.create_crcs_tables(base_tables, created_tables)

  # Fix tables
  import fix_tables
  fix_tables.fix_mlac_tables(base_tables, created_tables)
  fix_tables.fix_record_linkage(OPTIONS, base_tables, created_tables)
  fix_tables.fix_missing_fields(base_tables, created_tables)

  # Crossfill CIN and CRCS, then correct WOBs
  import crossfill
  crossfill.perform_crossfill(base_tables, created_tables)
  crossfill.correct_crossfill(base_tables, created_tables)

  # Basic matching
  import basic_matching
  basic_matching.boost_cin_crcs(base_tables, created_tables)
  basic_matching.boost_lac(base_tables, created_tables)

  # Make cleaned episode table
  import clean_episodes
  clean_episodes.create_table(OPTIONS, base_tables, created_tables)

  # CAFCASS matching
  import cafcass
  cafcass.link_cafcass(OPTIONS, base_tables, created_tables)

  # NCCHD matching
  import ncchd
  ncchd.patch_mcafcpar(OPTIONS, base_tables, created_tables)
  ncchd.link_births(base_tables, created_tables)

  # GP events
  import gp_events
  gp_events.link_gp(OPTIONS, base_tables, created_tables)
