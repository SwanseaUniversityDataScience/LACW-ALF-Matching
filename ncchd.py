from datetimerange import DateTimeRange
import pandas as pd
import datetime

import helper_funcs.generic_helpers as gn
import helper_funcs.db_conn as al
import utils

def _create_mcafcpar_table(mcafcpar, cafrel, cafalf, cafasu, cafapp):
  utils.drop_if_exists(mcafcpar)

  al.call_no_return(f'''
    CREATE TABLE {mcafcpar} AS (
      WITH CH_SUBJECT_PUBLIC AS (
        SELECT DISTINCT system_id_e
          FROM {cafalf} AS alf
          LEFT JOIN (
              SELECT DISTINCT record2id_e 
              FROM {cafasu} AS asu
              LEFT JOIN (
                SELECT DISTINCT cap_applicationid_e
                FROM {cafapp}
                WHERE cap_lawtypename = 'Public'
              ) AS app
                ON asu.record1id_e = app.cap_applicationid_e
            WHERE record2roleidname = 'Subject'
              AND app.cap_applicationid_e IS NOT NULL
          ) AS appsu
            ON alf.system_id_e = appsu.record2id_e
          WHERE appsu.record2id_e IS NOT NULL
      ),
      PA_RESPONDENT_PUBLIC AS (
        SELECT DISTINCT system_id_e
            FROM {cafalf} AS alf
            LEFT JOIN (
              SELECT DISTINCT record2id_e 
                FROM {cafasu} AS asu
                LEFT JOIN (
                  SELECT DISTINCT cap_applicationid_e
                    FROM {cafapp}
                    WHERE cap_lawtypename = 'Public'
                ) AS app
                  ON asu.record1id_e = app.cap_applicationid_e
                WHERE record2roleidname = 'Respondent'
                  AND app.cap_applicationid_e IS NOT NULL
            ) AS appsu
              ON alf.system_id_e = appsu.record2id_e
            WHERE appsu.record2id_e IS NOT NULL
      )
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
          chalf.gndr_cd AS child_gndr,
          CASE WHEN chalf.system_id_e IN (SELECT SYSTEM_ID_E FROM CH_SUBJECT_PUBLIC) THEN 1
              ELSE 0
          END AS child_subj_public,
          CASE WHEN palf.system_id_e IN (SELECT SYSTEM_ID_E FROM PA_RESPONDENT_PUBLIC) THEN 1
              ELSE 0
          END AS parent_resp_public
      FROM {cafrel} as rel
      INNER JOIN {cafalf} as palf
        ON rel.record1id_e = palf.system_id_e
      INNER JOIN {cafalf} AS chalf
        ON rel.record2id_e = chalf.system_id_e
      WHERE UPPER(rel.record1roleidname) = 'PARENT' 
        AND UPPER(rel.record2roleidname) = 'CHILD'
    ) WITH DATA;
  ''')

def _patch_mcafcpar_table(tmp_ncaf, tmp_ncchb, mcafcpar, ncch):
  # Create temporary caf table
  utils.drop_if_exists(tmp_ncaf)

  al.call_no_return(f'''
    CREATE TABLE {tmp_ncaf} AS (
      WITH CAFC AS (
        SELECT parent_system_id, PARENT_ALF, parent_wob, 
          child_system_id, child_wob, child_gndr 
        FROM {mcafcpar}
        WHERE CHILD_ALF IS NULL 
          AND PARENT_ALF IS NOT NULL 
          AND PARENT_GNDR = 2
      ), 
      NCCH AS (
        SELECT alf_e, wob, birth_weight, birth_tm, mat_alf_e, lsoa_cd_birth, 
        CASE WHEN gndr_cd = 'M' THEN 1
          WHEN gndr_cd = 'F' THEN 2
          ELSE 9 END AS gndr_cd
        FROM {ncch}
      ),
      TWINS AS (
          SELECT lf.alf_e AS ch1_alf, lf.CHILD_ID_e AS ch1_id, lf.wob, 
            lf.BIRTH_WEIGHT AS ch1_bw, lf.birth_tm AS ch1_bt, 
            lf.gndr_cd AS ch1_gndr, rt.alf_e AS ch2_alf, rt.CHILD_ID_e AS ch2_id, 
            rt.BIRTH_WEIGHT AS ch2_bw, rt.BIRTH_TM AS ch2_bt, rt.GNDR_CD AS ch2_gndr
          FROM {ncch} AS lf
          INNER JOIN {ncch} AS rt
            ON lf.MAT_alf_e = rt.MAT_alf_e 
          AND lf.wob = rt.wob
          AND lf.BIRTH_ORDER != rt.BIRTH_ORDER 
          AND lf.BIRTH_TM != rt.BIRTH_TM 
      )
      SELECT DISTINCT cafc.*, ncch.*,
        CASE WHEN twins.ch1_alf IS NOT NULL THEN 1 ELSE 0 END AS twin,
        CASE WHEN cchild.child_alf IS NOT NULL THEN 1 ELSE 0 END AS alf_already_caf,
        CASE WHEN dcheck.child_alf IS NOT NULL THEN 1 ELSE 0 END AS alf_caf_same_mother
      FROM CAFC AS cafc
      LEFT JOIN NCCH AS ncch
        ON cafc.parent_alf = ncch.MAT_alf_e 
      AND cafc.child_wob = ncch.wob
      AND cafc.child_gndr = ncch.gndr_cd
      LEFT JOIN TWINS AS twins
        ON ncch.alf_e = twins.ch1_alf
        OR ncch.alf_e = twins.ch2_alf
      LEFT JOIN {mcafcpar} AS cchild
        ON ncch.alf_e = cchild.child_alf
      LEFT JOIN {mcafcpar} AS dcheck
        ON ncch.alf_e = dcheck.child_alf
      AND ncch.mat_alf_e = dcheck.parent_alf
    ) WITH DATA;
  ''')

  # Create patch table
  utils.drop_if_exists(tmp_ncchb)

  al.call_no_return(f'''
    CREATE TABLE {tmp_ncchb} AS (
      WITH PARENT AS (
        SELECT NCCH.ALF_E, NCCH.WOB
        FROM {mcafcpar} AS CAF
        LEFT JOIN {ncch} AS NCCH
          ON CAF.PARENT_ALF = NCCH.MAT_ALF_E
        AND CAF.PARENT_GNDR = 2
      ),
      CHECK_MOTHER AS (
        SELECT TMP.ALF_E, TMP.CHILD_SYSTEM_ID, TMP.ALF_ALREADY_CAF, TMP.ALF_CAF_SAME_MOTHER,
          CASE WHEN TMP.ALF_E IN (
              SELECT PARENT.ALF_E
              FROM PARENT AS PARENT 
              WHERE PARENT.WOB = TMP.WOB
            ) THEN 1
            ELSE 0
          END AS CHECK_ANY
        FROM (
          SELECT ALF_E, CHILD_SYSTEM_ID, WOB, ALF_ALREADY_CAF, ALF_CAF_SAME_MOTHER
          FROM {tmp_ncaf}
          WHERE TWIN = 0
            AND ALF_E IS NOT NULL
          GROUP BY ALF_E, CHILD_SYSTEM_ID, WOB, ALF_ALREADY_CAF, ALF_CAF_SAME_MOTHER
        ) AS TMP
        LEFT JOIN (
          SELECT CHILD_SYSTEM_ID
          FROM {tmp_ncaf}
          GROUP BY CHILD_SYSTEM_ID
          HAVING COUNT(*) > 1
        ) AS DUPES
          ON TMP.CHILD_SYSTEM_ID = DUPES.CHILD_SYSTEM_ID
        WHERE DUPES.CHILD_SYSTEM_ID IS NULL
      )
      SELECT DISTINCT ALF_E, CHILD_SYSTEM_ID,
        CASE WHEN ALF_ALREADY_CAF = 0 THEN 1
          WHEN ALF_ALREADY_CAF = 1 AND ALF_CAF_SAME_MOTHER = 1 THEN 1
          WHEN ALF_ALREADY_CAF = 1 AND ALF_CAF_SAME_MOTHER = 0 AND CHECK_ANY = 0 THEN 1
          ELSE 0
        END AS SHOULD_UPDATE
      FROM CHECK_MOTHER
    ) WITH DATA;
  ''')

  # Update MCAFCPAR table
  al.call_commit(f'''
    MERGE INTO {mcafcpar} AS TRG
    USING (
        SELECT *
        FROM {tmp_ncchb}
        WHERE SHOULD_UPDATE = 1
    ) AS SRC
      ON TRG.CHILD_SYSTEM_ID = SRC.CHILD_SYSTEM_ID
    WHEN MATCHED THEN
      UPDATE SET TRG.CHILD_ALF = SRC.ALF_E;
  ''')

  # Clean up
  utils.drop_if_exists(tmp_ncaf)
  utils.drop_if_exists(tmp_ncchb)

def patch_mcafcpar(options, base_tables, created_tables):
  mcafcpar = created_tables.get('mcafcpar')
  ncch = base_tables.get('ncchd')
  cafalf = base_tables.get('cafalf')
  cafrel = base_tables.get('cafrel')
  cafasu = base_tables.get('cafasu')
  cafapp = base_tables.get('cafapp')

  working_schema = options.get('WORKING_SCHEMA')
  tmp_ncaf = f'{working_schema}.TMP_NCAF_TBL'
  tmp_ncchb = f'{working_schema}.TMP_NCCHB_TBL'

  # Create MCAFCPAR table and patch
  _create_mcafcpar_table(mcafcpar, cafrel, cafalf, cafasu, cafapp)
  _patch_mcafcpar_table(tmp_ncaf, tmp_ncchb, mcafcpar, ncch)

def _get_base_births(pers, edf, mdf):
  gndr_map = { 1:'M', 2:'F' }
  
  tep = edf[edf['unified_id'] == pers]
  tmn = mdf[mdf['unified_id'] == pers]
  wob = tep['wob'].iloc[0]
  gndr_num = int(tmn['gndr_cd'].iloc[0])
  gndr = gndr_map[gndr_num]
  irn = tmn['irn_e'].iloc[0]
  fhp = tmn['lsoa2011_home_postcode'].iloc[0]
  plocs = [x for x in utils.ulist(tep['lsoa2011_placement_postcode']) if x is not None]
  
  return tep, tmn, wob, gndr, gndr_num, irn, fhp, plocs

def _query_ncchd_births(ncchd_births, mlac_alf, mcafcpar, wdsdralf, gpncare, gpadopt, gpparent, wob, gndr, fhp, gndr_n, plocs):
  pquery = ''
  pcase = '0 AS wdsd_ppc_match,'
  
  if plocs != []:
    pquery = '''
      LEFT JOIN (
          SELECT DISTINCT alf_e
          FROM {0}
          WHERE lsoa2011_cd IN ({1})
      ) AS wdsdchild
      ON nh.alf_e = wdsdchild.alf_e
    '''.format(wdsdralf, ','.join(["'"+x+"'" for x in plocs]))
    
    pcase = '''CASE WHEN wdsdchild.alf_e IS NOT NULL THEN 1 ELSE 0 END AS wdsd_ppc_match,'''

  q = '''
    SELECT DISTINCT nh.alf_e, nh.wob, nh.birth_weight, nh.birth_tm, nh.gndr_cd, nh.tot_birth_num, nh.mat_age, nh.mat_alf_e, 
    CASE WHEN caf.parent_alf IS NOT NULL THEN 1 ELSE 0 END AS mat_alf_in_caf,
    CASE WHEN cafchild.child_alf IS NOT NULL THEN 1 ELSE 0 END AS child_alf_in_caf,
    CASE WHEN caf.child_wob = nh.wob AND caf.child_gndr = {6} THEN 1 ELSE 0 END AS caf_wg_match_from_matalf,
    CASE WHEN nh.fill_lsoa IS NOT NULL THEN 1 ELSE 0 END AS lsoa_nnull,
    CASE WHEN cafchild.child_alf IS NOT NULL THEN cafchild.child_subj_public
        WHEN cafchild.child_alf IS NULL AND caf.child_wob = nh.wob AND caf.child_gndr = {6} THEN caf.child_subj_public
    ELSE NULL
    END AS child_subj_public,
    caf.parent_resp_public,
    CASE WHEN wdsd.alf_e IS NOT NULL THEN 1 ELSE 0 END AS wdsd_fhp_match,
    {12}
    CASE WHEN childcare.alf_e IS NOT NULL THEN 1 ELSE 0 END AS child_care_ev,
    CASE WHEN childadopt.alf_e IS NOT NULL THEN 1 ELSE 0 END AS child_adopt_ev,
    CASE WHEN parentcare.alf_e IS NOT NULL THEN 1 ELSE 0 END AS parent_lac_ev
    FROM (
        SELECT *, 
        FIRST_VALUE(lsoa_cd_birth) OVER (PARTITION BY wob, birth_weight, birth_tm, gndr_cd, gest_age, mat_alf_e ORDER BY lsoa_cd_birth) AS fill_lsoa 
        FROM
        {0} 
    ) AS nh
    LEFT JOIN {1} AS lac
    ON nh.alf_e = lac.alf_e
    LEFT JOIN {2} AS caf
    ON nh.mat_alf_e = caf.parent_alf
    LEFT JOIN {2} AS cafchild
    ON nh.alf_e = cafchild.child_alf
    LEFT JOIN (
        SELECT DISTINCT alf_e
        FROM {7}
        WHERE lsoa2011_cd = '{5}'
    ) AS wdsd
    ON nh.mat_alf_e = wdsd.alf_e
    {8}
    LEFT JOIN {9} AS childcare
    ON nh.alf_e = childcare.alf_e
    LEFT JOIN {10} AS childadopt
    ON nh.alf_e = childadopt.alf_e
    LEFT JOIN {11} AS parentcare
    ON nh.mat_alf_e = parentcare.alf_e
    WHERE nh.wob = '{3}'
    AND nh.gndr_cd = '{4}'
    AND (nh.fill_lsoa = '{5}' OR nh.fill_lsoa IS NULL)
    AND lac.alf_e IS NULL
    AND ((cafchild.child_alf IS NOT NULL AND cafchild.child_subj_public) OR (cafchild.child_alf IS NULL))
    order by nh.alf_e
  '''.format(
    ncchd_births, mlac_alf, mcafcpar, wob, gndr, fhp, gndr_n, wdsdralf, 
    pquery, gpncare, gpadopt, gpparent, pcase
  ) 
  return al.call(q)

def _add_alf_births(mlac_alf, nalf, alf_mtch, alf_sts, uid, is_twin=0):
  alfmtch = alf_mtch
  if not alf_mtch:
    alfmtch = 'NULL'
  
  q = '''
    UPDATE {0}
    SET alf_e = {1}, 
      alf_mtch_pct = NULL, 
      alf_sts_cd = NULL, 
      alf_source = 'NCCHD_BIRTHS',
      lac_match_twin = {3},
      lac_alf_match = 1
    WHERE unified_id = '{2}'
  '''.format(mlac_alf, int(nalf), uid, is_twin)
  
  al.call_commit(q)

def link_births(base_tables, created_tables):
  mlac_alf = created_tables.get('mlac_alf')
  mlac_main = created_tables.get('mlac_main')
  mlac_epi = created_tables.get('mlac_epi')
  mlac_cepi = created_tables.get('mlac_cepi')
  mcafcpar = created_tables.get('mcafcpar')
  ncchd = base_tables.get('ncchd')
  wdsdralf = base_tables.get('wdsd_ralf')
  gpncare = created_tables.get('gpev_narrow care')
  gpadopt = created_tables.get('gpev_adoption child')
  gpparent = created_tables.get('gpev_parent of LAC')
  la_codes = created_tables.get('la_codes')

  adf, mdf, edf, cedf = gn.get_refreshed_lac_tables(
    mlac_alf, mlac_main, mlac_epi, la_codes, mlac_cepi, True
  )

  entry_under6m = list(edf[edf['age_start'] <= 0.6]['unified_id'].unique())
  have_homepc = list(mdf[~mdf['lsoa2011_home_postcode'].isna()]['unified_id'].unique())
  eligible_children = list(
    mdf[(mdf['unified_id'].isin(entry_under6m)) &
    (mdf['unified_id'].isin(have_homepc))]['unified_id'].unique()
  )
  eligible_children.sort()

  results_e = []
  # link via births
  for pers in eligible_children:
    tep, tmn, wob, gndr, gndr_n, irn, fhp, plocs = _get_base_births(pers, edf, mdf)

    if fhp is None or fhp[0] != 'W' or utils.ulist(tep['legal_status_code']) == ['V2']:
      continue

    try:
      tnch = _query_ncchd_births(ncchd, mlac_alf, mcafcpar, wdsdralf, gpncare, gpadopt, gpparent, wob, gndr, fhp, gndr_n, plocs)
    except ValueError:
      pass
      continue

    tnch['sum'] = tnch[[
      'mat_alf_in_caf', 'child_alf_in_caf', 'caf_wg_match_from_matalf', 
      'lsoa_nnull', 'wdsd_fhp_match', 'wdsd_ppc_match', 'child_care_ev', 
      'child_adopt_ev', 'parent_lac_ev', 'child_subj_public', 'parent_resp_public'
    ]].sum(axis=1)
    if tnch['alf_e'].nunique() == 1:
      _add_alf_births(mlac_alf, tnch.iloc[0]['alf_e'], None, 1, pers)
      continue
      
    if tnch['alf_e'].nunique() > 1 and tnch['sum'].max() > 2:
      # pick the row with the best match
      pick = tnch[tnch['sum'] == tnch['sum'].max()]
      if pick['alf_e'].nunique() == 1:
        _add_alf_births(mlac_alf, pick.iloc[0]['alf_e'], None, 1, pers)
        continue
      
      # check the row with the best match when there are two equal rows         
      if pick['alf_e'].nunique() == 2 and pick['mat_alf_e'].nunique() == 1:
        # basically happens when there are twins, so just pick one of them
        _add_alf_births(mlac_alf, pick.iloc[0]['alf_e'], None, 1, pers, is_twin=1)
        continue
      
      # if there are multiple best matches we prefer the row where the birth lsoa matches the LAC census
      pick = pick[pick['lsoa_nnull'] == 1]
      if pick['alf_e'].nunique() == 1:
        _add_alf_births(mlac_alf, pick.iloc[0]['alf_e'], None, 1, pers)
        continue
      
      # if there are still multiple best matches, go for the best match between the two
      pick['sum_2'] = pick[[
        'child_alf_in_caf', 'lsoa_nnull', 'wdsd_fhp_match', 'wdsd_ppc_match', 
        'child_care_ev', 'child_adopt_ev', 'parent_lac_ev'
      ]].sum(axis=1)
      npick = pick[pick['sum_2'] == pick['sum_2'].max()]
      if npick['alf_e'].nunique() == 1:
        _add_alf_births(mlac_alf, npick.iloc[0]['alf_e'], None, 1, pers)
        continue
  print('matched via ncchd births')
