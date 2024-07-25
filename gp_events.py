from datetimerange import DateTimeRange
from tqdm import tqdm
import pandas as pd
import datetime

import helper_funcs.generic_helpers as gn
import helper_funcs.db_conn as al
import utils

def _get_gprs_table(lacminwob, lacmaxwob, mlac_alf, gpreg, gpalf, gpncare, gppcare, gpadopt, gpparent, ncchd_births):
  data = al.call(f'''
    SELECT gp.alf_e, alf.wob, alf.gndr_cd, gp.start_date, gp.end_date, 
      alf.lsoa_cd, CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac,
      CASE WHEN ncare.alf_e IS NULL THEN 0 ELSE 1 END AS narrow_care_ev,
      CASE WHEN pcare.alf_e IS NULL THEN 0 ELSE 1 END AS precare_ev,
      CASE WHEN adopt.alf_e IS NULL THEN 0 ELSE 1 END AS adopt_ev,
      CASE WHEN gpparent.alf_e IS NULL THEN 0 ELSE 1 END AS parent_ev
    FROM {gpreg} AS gp
    LEFT JOIN {gpalf} AS alf
      ON gp.alf_e = alf.alf_e
    LEFT JOIN (
        SELECT DISTINCT alf_e 
        FROM {mlac_alf} 
    ) AS lac
      ON alf.alf_e = lac.alf_e
    LEFT JOIN {gpncare} AS ncare
      ON alf.alf_e = ncare.alf_e
    LEFT JOIN {gppcare} AS pcare
      ON alf.alf_e = pcare.alf_e
    LEFT JOIN {gpadopt} AS adopt
      ON alf.alf_e = adopt.alf_e
    LEFT JOIN (
        SELECT DISTINCT ncchd_births.alf_e, ncchd_births.mat_alf_e 
        FROM {ncchd_births} AS ncchd_births
        INNER JOIN {gpparent} AS pgpev
          ON ncchd_births.mat_alf_e = pgpev.alf_e
    ) AS gpparent
      ON alf.alf_e = gpparent.alf_e
    WHERE alf.wob >= '{lacminwob}'
      AND alf.wob <= '{lacmaxwob}'
    ORDER BY start_date, GP.ALF_E
  ''')

  data['sum'] = data[[
    'narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev'
  ]].sum(axis=1)

  return data

def _get_wdsdrs_table(lacminwob, lacmaxwob, mlac_alf, wdsdalf, wdsdralf, gpncare, gppcare, gpadopt, gpparent, ncchd_births):
  data = al.call(f'''
    SELECT wdsdralf.alf_e, alf.wob, alf.gndr_cd, wdsdralf.start_date, 
      wdsdralf.end_date, wdsdralf.ralf_e, wdsdralf.lsoa2011_cd, 
      CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac,
      CASE WHEN ncare.alf_e IS NULL THEN 0 ELSE 1 END AS narrow_care_ev,
      CASE WHEN pcare.alf_e IS NULL THEN 0 ELSE 1 END AS precare_ev,
      CASE WHEN adopt.alf_e IS NULL THEN 0 ELSE 1 END AS adopt_ev, 
      CASE WHEN gpparent.alf_e IS NULL THEN 0 ELSE 1 END AS parent_ev
    FROM {wdsdralf} AS wdsdralf
    LEFT JOIN {wdsdalf} AS alf
      ON wdsdralf.alf_e = alf.alf_e
    LEFT JOIN (
        SELECT DISTINCT alf_e 
        FROM {mlac_alf} 
    ) AS lac
      ON wdsdralf.alf_e = lac.alf_e
    LEFT JOIN {gpncare} AS ncare
      ON alf.alf_e = ncare.alf_e
    LEFT JOIN {gppcare} AS pcare
      ON alf.alf_e = pcare.alf_e
    LEFT JOIN {gpadopt} AS adopt
      ON alf.alf_e = adopt.alf_e
    LEFT JOIN (
        SELECT DISTINCT ncchd_births.alf_e, ncchd_births.mat_alf_e 
        FROM {ncchd_births} AS ncchd_births
        INNER JOIN {gpparent} AS pgpev
          ON ncchd_births.mat_alf_e = pgpev.alf_e
    ) AS gpparent
      ON alf.alf_e = gpparent.alf_e
    WHERE alf.wob >= '{lacminwob}'
      AND alf.wob <= '{lacmaxwob}'
    ORDER BY start_date, WDSDRALF.ALF_E
  ''')

  data['sum'] = data[[
    'narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev'
  ]].sum(axis=1)

  return data

def _get_gpev_table(lacminwob, lacmaxwob, mlac_alf, gpevent, readcode, child_codes, lsoa_decode):
  child_event_codes = ','.join(["'"+x+"'" for x in child_codes])

  data = al.call(f'''
    SELECT DISTINCT gp.alf_e, gp.wob, gp.gndr_cd, 
      gp.lsoa_cd, la.lad20nm, gp.event_cd, 
      CASE WHEN gp.event_dt = '0001-01-01' THEN '2099-01-01' 
        ELSE gp.event_dt 
      END AS event_dt, 
      dc.pref_term_30,
      CASE WHEN lac.alf_e IS NOT NULL THEN 1
        ELSE 0
      END AS alf_in_lac
    FROM {gpevent} AS gp
    LEFT JOIN {readcode} AS dc
      ON gp.event_cd = dc.READ_CODE
    LEFT JOIN {lsoa_decode} AS la
      ON gp.lsoa_cd = la.lsoa11cd
    LEFT JOIN {mlac_alf} AS lac
      ON gp.alf_e = lac.alf_e
    WHERE gp.wob >= '{lacminwob}'
      AND gp.wob <= '{lacmaxwob}'
      AND gp.event_cd IN ({child_event_codes})
      AND gp.alf_e IS NOT NULL
    ORDER BY GP.ALF_E, 
      CASE WHEN gp.event_dt = '0001-01-01' THEN '2099-01-01' 
        ELSE gp.event_dt 
      END
  ''')

  return data

def wide_ev_type(wob, gender, evtype, read_codes, gpev_child):
  rcs = list(read_codes[evtype].keys())
  
  gdf = gpev_child[
    (gpev_child['wob'] == wob) & 
    (gpev_child['gndr_cd'] == str(gender)) &
    (gpev_child['event_cd'].isin(rcs))
  ]
  
  return gdf

def inplace_udate_alf(alf, gpev_child, wdsdrs, gprs):
  idxs = gpev_child[gpev_child['alf_e'] == alf].index.to_list()
  for idx in idxs:
    gpev_child.at[idx, 'alf_in_lac'] = 1

  idxs = wdsdrs[wdsdrs['alf_e'] == alf].index.to_list()
  for idx in idxs:
    wdsdrs.at[idx, 'alf_in_lac'] = 1

  idxs = gprs[gprs['alf_e'] == alf].index.to_list()
  for idx in idxs:
    gprs.at[idx, 'alf_in_lac'] = 1
  
  return gpev_child, wdsdrs, gprs

def add_alf(nalf, alf_mtch, alf_sts, uid, mlac_alf):
  alfmtch = alf_mtch
  if not alf_mtch:
    alfmtch = 'NULL'
  
  q = '''
    UPDATE {0}
    SET alf_e = {1}, 
      alf_mtch_pct = NULL, 
      alf_sts_cd = NULL, 
      alf_source = 'GP_EVENT',
      LAC_ALF_MATCH = 1
    WHERE unified_id = '{2}'
  '''.format(mlac_alf, int(nalf), uid)
  
  al.call_no_return(q)

def get_base(pers, mdf, edf, cedf):
  tep = edf[edf['unified_id'] == pers]
  tcep = cedf[cedf['unified_id'] == pers]
  tmn = mdf[mdf['unified_id'] == pers]

  wob = tep['wob'].iloc[0]
  gndr = int(tmn['gndr_cd'].iloc[0])

  lanm = tep['la_name'].iloc[0]
  flac = tep['episode_start_date'].min()
  llac = tcep['episode_end_date'].max()

  plsoa = [x for x in list(tep['lsoa2011_placement_postcode'].unique()) if x is not None]
  hlsoa = [x for x in list(tmn['lsoa2011_home_postcode'].unique()) if x is not None]

  adopt = tep['reason_episode_finished_code'].isin(['E1','E11','E12']).any()

  return tep, tcep, wob, gndr, lanm, flac, llac, plsoa, hlsoa, adopt

def link_gp(options, base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')
  mlac_epi = created_tables.get('mlac_epi')
  mlac_cepi = created_tables.get('mlac_cepi')
  ncchd_births = base_tables.get('ncchd')
  wdsdalf = base_tables.get('wdsd_pers')
  wdsdralf = base_tables.get('wdsd_ralf')
  gpreg = base_tables.get('gp_reg')
  gpalf = base_tables.get('gp_alf')
  gpevent = base_tables.get('gp_event')
  gpncare = created_tables.get('gpev_care')
  gpadopt = created_tables.get('gpev_adoption child')
  gpparent = created_tables.get('gpev_parent of LAC')
  gppcare = created_tables.get('gpev_pre care')

  readcode = created_tables.get('readcode')
  lsoa_decode = created_tables.get('lsoa_decode')
  la_ref = created_tables.get('la_codes')
  
  # Get read codes
  read_codes = utils.read_json_file(options.get('READ_CODES_LOC'))
  child_codes = list(set(
    list(read_codes['adoption child'].keys()) + 
    list(read_codes['care'].keys()) + 
    list(read_codes['narrow care'].keys()) +
    list(read_codes['pre care'].keys())
  ))

  # Get lac data
  adf, mdf, edf, cedf = gn.get_refreshed_lac_tables(
    mlac_alf, mlac_main, mlac_epi, la_ref, mlac_cepi, True
  )

  lacmaxwob = adf[~adf['wob'].isna()]['wob'].max()
  lacminwob = adf[~adf['wob'].isna()]['wob'].min()

  # Get GP data
  gprs = _get_gprs_table(
    lacminwob, lacmaxwob, mlac_alf, gpreg, gpalf, 
    gpncare, gppcare, gpadopt, gpparent, ncchd_births 
  )

  wdsdrs = _get_wdsdrs_table(
    lacminwob, lacmaxwob, mlac_alf, wdsdalf, wdsdralf,
    gpncare, gppcare, gpadopt, gpparent, ncchd_births
  )

  gpev_child = _get_gpev_table(
    lacminwob, lacmaxwob, mlac_alf, 
    gpevent, readcode, child_codes, lsoa_decode
  )

  # MAIN
  ### remove kids who are only ever on respite care, and who are only ever placed with their parents
  fedf = edf[(edf['legal_status_code'] != 'V1') & (edf['placement_type_code'] != 'P1') & (~edf['wob'].isna())]

  tmp = fedf.groupby(['unified_id'])['age_start'].min().reset_index()
  tmp = tmp[tmp['age_start'] < 0]
  earlystart = list(tmp['unified_id'].unique())

  fedf = fedf[~fedf['unified_id'].isin(earlystart)]
  to_match = fedf['unified_id'].unique()

  for pers in tqdm(to_match):
    tep, tcep, wob, gndr, lanm, flac, llac, plsoa, hlsoa, adopt = get_base(pers, mdf, edf, cedf)
    
    if tep['episode_start_date'].min() < datetime.date(2002,1,1):
      continue
    
    # if they're never in a placement for more than 3 weeks (21 days) then skip, can't match
    if tcep['time_placement'].max() < 21:
      continue
        
    # get all care-related GP events for people with this wob and gender
    gplac = wide_ev_type(wob, gndr, 'narrow care', read_codes, gpev_child)
    gplac = gplac[gplac['alf_in_lac'] == 0]
    gprec = wide_ev_type(wob, gndr, 'pre care', read_codes, gpev_child)
    gprec['event_dt'] = gn.dt_type_fix(gprec['event_dt'])
    gprec = gprec[gprec['alf_in_lac'] == 0]
    gprec = gprec[gprec['event_dt'] <= flac]
    
    gpall = wide_ev_type(wob, gndr, 'care', read_codes, gpev_child)
    gpall = gpall[gpall['alf_in_lac'] == 0]
    gpall['event_dt'] = gn.dt_type_fix(gpall['event_dt'])
    gpall = gpall[gpall['event_dt'] <= llac]
    
    gplpre = gprec[gprec['lsoa_cd'].isin(hlsoa)]
    gpldur = gpall[gpall['lsoa_cd'].isin(plsoa)]
    
    gploc = pd.concat([gplpre, gpldur])
    
    if len(plsoa) == 0 or len(gploc) == 0:
      gploc = gpall[gpall['lad20nm'] == lanm]
        
    intersec = set(utils.ulist(gplac['alf_e'])).intersection(set(utils.ulist(gprec['alf_e']))).intersection(set(utils.ulist(gploc['alf_e'])))
    
    if adopt and len(intersec) > 1:
      gpapt = wide_ev_type(wob, gndr, 'adoption child', read_codes, gpev_child)
      intersec = intersec.intersection(utils.ulist(gpapt['alf_e']))
    
    if len(intersec) == 1:
      add_alf(list(intersec)[0], 'NULL', 1, pers, mlac_alf)
      gpev_child, wdsdrs, gprs = inplace_udate_alf(list(intersec)[0], gpev_child, wdsdrs, gprs)
      continue
    
    if len(plsoa) < 2:
      continue
        
    # do extra/more filtering using the LSOAs, so above we stop trying to find an answer for kids without enough of them
    wdpl = wdsdrs[(wdsdrs['wob'] == wob) & (wdsdrs['gndr_cd'] == str(gndr)) &
                  (wdsdrs['lsoa2011_cd'].isin(plsoa)) & (wdsdrs['alf_in_lac'] == 0)] 

    wdpl = wdpl[(wdpl.apply(lambda x: DateTimeRange(flac, llac).is_intersection(DateTimeRange(x['start_date'], x['end_date'])), axis=1))]
    
    wdhm = wdsdrs[(wdsdrs['wob'] == wob) & (wdsdrs['gndr_cd'] == str(gndr)) &
                  (wdsdrs['lsoa2011_cd'].isin(hlsoa)) & (wdsdrs['alf_in_lac'] == 0)]

    wdhm = wdhm[(wdhm['start_date'] < flac) & (wdhm['end_date'] < llac)]
    
    intersec = set(utils.ulist(wdhm['alf_e'])).intersection(set(utils.ulist(wdpl['alf_e'])))
    
    check = wdpl[wdpl['alf_e'].isin(list(intersec))][
                    ['alf_e', 'alf_in_lac', 'narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev', 'sum']
                ].drop_duplicates(keep='first')
    if adopt:        
      checkev = check[check['adopt_ev'] == 1]
    else:
      check = check[(check['adopt_ev'] == 0) & (check['sum'] > 0)]
      checkev = check[check['sum'] == check['sum'].max()]
    
    if checkev['alf_e'].nunique() == 1:
      add_alf(checkev['alf_e'].iloc[0], 'NULL', 1, pers, mlac_alf)
      gpev_child, wdsdrs, gprs = inplace_udate_alf(checkev['alf_e'].iloc[0], gpev_child, wdsdrs, gprs)
      continue
        
    flt = gprs[(gprs['wob'] == wob) & (gprs['gndr_cd'] == str(gndr)) &
      (gprs['lsoa_cd'].isin(plsoa)) & (gprs['alf_in_lac'] == 0)]

    gppl = flt[(flt.apply(lambda x: DateTimeRange(flac, llac).is_intersection(DateTimeRange(x['start_date'], x['end_date'])), axis=1))]
    
    flt = gprs[(gprs['wob'] == wob) & (gprs['gndr_cd'] == str(gndr)) &
      (gprs['lsoa_cd'].isin(hlsoa)) & (gprs['alf_in_lac'] == 0)]

    gphm = flt[(flt['start_date'] < flac) & (flt['end_date'] < llac)]
    
    intersec = set(utils.ulist(gppl['alf_e'])).intersection(set(utils.ulist(gphm['alf_e'])))
    
    check = wdpl[wdpl['alf_e'].isin(list(intersec))][
                    ['alf_e', 'alf_in_lac', 'narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev', 'sum']
                ].drop_duplicates(keep='first')
    
    if adopt:        
      checkev = check[check['adopt_ev'] == 1]
    else:
      check = check[(check['adopt_ev'] == 0) & (check['sum'] > 0)]
      checkev = check[check['sum'] == check['sum'].max()]
    
    if checkev['alf_e'].nunique() == 1:
      add_alf(checkev['alf_e'].iloc[0], 'NULL', 1, pers, mlac_alf)
      gpev_child, wdsdrs, gprs = inplace_udate_alf(checkev['alf_e'].iloc[0], gpev_child, wdsdrs, gprs)
      continue
