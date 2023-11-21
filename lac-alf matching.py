# import libraries
import pandas as pd
from tqdm import tqdm
import warnings
import json
import recordlinkage
from datetimerange import DateTimeRange
import datetime 
from tqdm import tqdm
import numpy as np

# import helper functions
import helper_funcs.db_conn as al
import helper_funcs.generic_helpers as gn

# log in to db2
al.start(username='username', pwd='password') 

# schema name where the tables you need are stored
sc = 'SAILXXXXV'
wsc = sc[:4] + 'W' + sc[4:]

# set to True if your LAC_MAIN table has ethnicity, otherwise set to False
ethn_in_lac = True

# read in the base table provisioning file 
f = open('./helper_files/table_inventory.json')
base_tbs = json.load(f)
f.close()

# 0 CREATE TABLE INVENTORY 

# loop through the tables in ase list and concat with the schema name 
base_list = base_tbs['base']

for k, v in tqdm(base_list.items()):
    refnm = k
    searchnm = v['name']
    schema=v['name'].partition(".")[0]
    regr = searchnm.partition(".")[2] + '_2%'
    q = '''
    SELECT TABNAME
    FROM syscat.tables
    WHERE TABSCHEMA = '{0}'
    AND TABNAME LIKE '{1}'
    ORDER BY CREATE_TIME DESC
    '''.format(schema, regr)
    ftnm = al.call(q).iloc[0]['tabname']
    v['fullname'] = schema + '.' + ftnm
create_list = base_tbs['create']

for k, v in tqdm(create_list.items()):
    if 'name' in v.keys():
        sname = v['name']
        v['fullname'] = wsc + sname

with open("./helper_files/table_inventory_complete.json", "w") as outfile:
    json.dump(base_tbs, outfile)

# STORE PROVSIONED TABLES 
ytabs = gn.get_tb_prov()
baset = ytabs['base']
ownt = ytabs['create']

# MAKE HELPER TABLES 
# grab base tables 
gpevent = baset['gp_event']['fullname']

lac_epi = baset['lac_epi']['fullname']
lac_main = baset['lac_main']['fullname']
lac_alf = baset['lac_alf']['fullname']
cin_alf = baset['cin_alf']['fullname']
cin_main = baset['cin_main']['fullname']

crcs_main = baset['crcs_main']['fullname']
crcs_alf = baset['crcs_alf']['fullname']

wdsdalf = baset['wdsd_pers']['fullname']
wdsd = baset['wdsd_lsoa']['fullname']
wdsdralf = baset['wdsd_ralf']['fullname']
wdsdlsoa = baset['wdsd_add']['fullname']
wdsdgp = baset['wdsd_gp']['fullname']

eduw_alf = baset['eduw']['fullname']

ncchd_births = baset['ncchd']['fullname']

cafcalf = baset['cafalf']['fullname']
cafchoa = baset['cafhoa']['fullname']
cafcho =  baset['cafho']['fullname']
cafchear = baset['cafhear']['fullname']
cafccase = baset['cafcase']['fullname']
cafcasu = baset['cafasu']['fullname']
cafcapp = baset['cafapp']['fullname']
cafcapporder = baset['cafapporder']['fullname']
cafcrel = baset['cafrel']['fullname']

gpreg = baset['gp_reg']['fullname']
gpalf = baset['gp_alf']['fullname']
gpevent = baset['gp_event']['fullname']

#grab own tables that will be created 
lsoa_decode = ownt['lsoa_decode']['fullname']
la_codes = ownt['la_codes']['fullname']

educ_la_decode = ownt['educ_la_decode']['fullname']

mlac_alf = ownt['mlac_alf']['fullname']
mlac_epi = ownt['mlac_epi']['fullname']
mlac_main = ownt['mlac_main']['fullname']
mlac_map = ownt['mlac_map']['fullname']
mlac_malf = ownt['mlac_malf']['fullname']
mlac_mepi = ownt['mlac_mepi']['fullname']
mlac_mmain = ownt['mlac_mmain']['fullname']
mlac_cepi = ownt['mlac_cepi']['fullname']

mcin_main = ownt['mcin_main']['fullname']
mcin_alf = ownt['mcin_alf']['fullname']

mcrcs_main = ownt['mcrcs_main']['fullname']
mcrcs_alf =  ownt['mcrcs_alf']['fullname']

mcafcpar = ownt['mcafcpar']['fullname']

readcode = ownt['readcode']['fullname']

gpcare = ownt['gpev_care']['fullname']
gpadopt = ownt['gpev_adoption child']['fullname']
gpncare = ownt['gpev_narrow care']['fullname']
gpparent = ownt['gpev_parent of LAC']['fullname']
gppcare = ownt['gpev_pre care']['fullname']

# create own tables if they do not exist 
if not gn.check_table_exists(educ_la_decode):
    print('Making {0} table'.format(educ_la_decode))
    tmp = pd.read_excel("helper_tables/nlac-2011.xls")
    tmp['new_la_code'] = tmp['new_la_code'].str.lstrip()
    tmp['new_la_code'] = tmp['new_la_code'].str.rstrip()
    tmp['la_name'] = tmp['la_name'].str.lstrip()
    tmp['la_name'] = tmp['la_name'].str.rstrip()
    al.make_table(tmp, educ_la_decode.split('.')[0], educ_la_decode.split('.')[1])
    
if not gn.check_table_exists(la_codes):
    print('Making {0} table'.format(la_codes))
    tmp = pd.read_csv("helper_tables/LACODES.csv")
    al.make_table(tmp, la_codes.split('.')[0], la_codes.split('.')[1])
    
if not gn.check_table_exists(lsoa_decode):
    print('Making {0} table'.format(lsoa_decode))
    tmp = pd.read_csv("helper_tables/LSOA_LA.csv")
    al.make_table(tmp, lsoa_decode.split('.')[0], lsoa_decode.split('.')[1])

# make GP reference table 
# read in the GP read code reference file
f = open('./helper_files/gp_read_codes.json')
read_codes = json.load(f)

for code in tqdm(read_codes.keys()):
    tdict = read_codes[code]
    t_str = ','.join(["'"+x+"'" for x in tdict])
    dkey = 'gpev_' + code
    tnm = ownt[dkey]['fullname']
    
    if not gn.check_table_exists(tnm):
        q = '''
        CREATE TABLE {0} AS (
        SELECT DISTINCT alf_e
        FROM {1}
        WHERE event_cd IN ({2})
        ) WITH DATA
        '''.format(tnm, gpevent, t_str)
        al.call_no_return(q)

# MAKE TABLES 
## LAC ALF TABLE 

# ALF table -- correct the WOBs where there's an ALF and where there isn't give a flag for if the wob is potentially unknown

q = '''
CALL fnc.drop_if_exists('{0}');
'''.format(mlac_alf)
al.call_no_return(q)

if not gn.check_table_exists(mlac_alf):
    q = '''
    CREATE TABLE {0} AS (
        SELECT DISTINCT lac.system_id_e, lac.local_authority_code, lac.alf_e, lac.alf_mtch_pct, lac.alf_sts_cd, 
        CASE
            WHEN lac.wob != wdsdalf.wob AND lac.alf_e IS NOT NULL THEN wdsdalf.wob
            ELSE lac.wob
        END AS wob,
        lac.gndr_cd, 
        CONCAT(CONCAT(lac.system_id_e, '_'), lac.local_authority_code) AS hybrid_id, 
        CASE 
            WHEN lac.alf_e IS NULL AND DAY(lac.wob) <= 15 AND DAY(lac.wob) + 7 >= 15 AND ep.min_estart >= lac.wob THEN ADD_DAYS(lac.wob, 7) 
            WHEN lac.alf_e IS NULL AND ep.min_estart < lac.wob THEN ep.min_estart
            ELSE NULL
        END AS max_poss_wob,
        CASE 
            WHEN lac.alf_e IS NULL AND DAY(lac.wob) <= 15 AND DAY(lac.wob) + 7 >= 15 THEN 1
            ELSE 0
        END AS potential_unknown_wob,
        CASE
            WHEN lac.wob != wdsdalf.wob AND lac.alf_e IS NOT NULL THEN 1
            WHEN lac.wob = wdsdalf.wob AND lac.alf_e IS NOT NULL THEN 0
            ELSE NULL
        END AS has_wob_been_corrected,
        lac.wob AS original_lac_wob,
        CASE
            WHEN ep.min_estart < lac.wob THEN 1
            ELSE 0
        END AS lacwob_def_wrong,
        ep.min_estart AS first_ep_start
        FROM {1} AS lac
        LEFT JOIN {2} AS wdsdalf
        ON lac.alf_e = wdsdalf.alf_e
        LEFT JOIN (
            SELECT CONCAT(CONCAT(system_id_e, '_'), local_authority_code) AS hybrid_id, MIN(episode_start_date) AS min_estart
            FROM {3}
            GROUP BY CONCAT(CONCAT(system_id_e, '_'), local_authority_code)
        ) AS ep
        ON CONCAT(CONCAT(lac.system_id_e, '_'), lac.local_authority_code) = ep.hybrid_id
    ) WITH DATA
'''.format(mlac_alf, lac_alf, wdsdalf, lac_epi)
al.call_no_return(q)

## MAIN TABLE 
# Backfill IRNS where children don't originally have them 

q = '''
SELECT * FROM {0} LIMIT 1
'''.format(lac_main)
maincols = al.call(q).columns.to_list()
irnidx = maincols.index('irn_e')
bfirn = maincols[:irnidx]
afirn = maincols[irnidx+1:]

q = '''
CALL fnc.drop_if_exists('{0}')
'''.format(mlac_main)
al.call_no_return(q)

if not gn.check_table_exists(mlac_main):
    q = '''
    CREATE TABLE {0} AS (
        SELECT hybrid_id, {1},
        backfill_irn AS irn_e, {2},
        CASE
            WHEN irn_e IS NULL AND backfill_irn IS NOT NULL THEN 1
            ELSE 0
        END AS irn_backfilled,
        lcd.LA_NAME,
        lsa.WD20NM AS home_area,
        lsa.LAD20NM AS home_localauth
        FROM (
            SELECT CONCAT(CONCAT(system_id_e, '_'), local_authority_code) AS hybrid_id, *,
                FIRST_VALUE(irn_e) OVER (PARTITION BY CONCAT(CONCAT(system_id_e, '_'), local_authority_code) ORDER BY irn_e) AS backfill_irn
            FROM {3} 
        ) AS lac
        LEFT JOIN {4} AS lsa
        ON lac.LSOA2011_HOME_POSTCODE = LSA.LSOA11CD
        LEFT JOIN {5} AS lcd
        ON lac.LOCAL_AUTHORITY_CODE = lcd.LA_ID
        ORDER BY year_code
    ) WITH DATA
'''.format(mlac_main, ','.join(bfirn), ','.join(afirn), lac_main, lsoa_decode, la_codes)
al.call_no_return(q)


## EPI TABLE

q = '''
CALL fnc.drop_if_exists('{0}')
'''.format(mlac_epi)
al.call_no_return(q)

if not gn.check_table_exists(mlac_epi):
    q = '''
    CREATE TABLE {0} AS (
        SELECT CONCAT(CONCAT(system_id_e, '_'), local_authority_code) AS hybrid_id, lac.*,
        lcd.LA_NAME,
        lsa.WD20NM AS placement_area,
        lsa.LAD20NM AS placement_localauth,
        CASE 
            WHEN EPISODE_END_DATE IS NULL THEN DATE('2099-01-01')
            ELSE EPISODE_END_DATE 
        END AS FILLED_EPISODE_END_DATE 
        FROM {1} AS lac
        LEFT JOIN {2} AS lsa
            ON lac.LSOA2011_PLACEMENT_POSTCODE = LSA.LSOA11CD 
        LEFT JOIN {3} AS lcd
            ON lac.LOCAL_AUTHORITY_CODE = lcd.LA_ID 
        ORDER BY year_code, episode_number
    ) WITH DATA
'''.format(mlac_epi, lac_epi, lsoa_decode, la_codes)
al.call_no_return(q)

## CIN ALF 
# ALF table -- correct the WOBs where there's an ALF and where there isn't give a flag for if the wob is potentially unknown

q = '''
CALL fnc.drop_if_exists('{0}');
'''.format(mcin_alf)
al.call_no_return(q)

if not gn.check_table_exists(mcin_alf):
    q = '''
    CREATE TABLE {0} AS (
        SELECT DISTINCT cin.child_code_e, cin.local_authority_code, cin.alf_e, cin.alf_mtch_pct, cin.alf_sts_cd, 
        CASE
            WHEN cin.wob != wdsdalf.wob AND cin.alf_e IS NOT NULL THEN wdsdalf.wob
            ELSE cin.wob
        END AS wob,
        cin.gndr_cd, 
        cin.lsoa2011_cd,
        CONCAT(CONCAT(cin.child_code_e, '_'), cin.local_authority_code) AS hybrid_id, 
        CASE
            WHEN cin.wob != wdsdalf.wob AND cin.alf_e IS NOT NULL THEN 1
            WHEN cin.wob = wdsdalf.wob AND cin.alf_e IS NOT NULL THEN 0
            ELSE NULL
        END AS has_wob_been_corrected,
        cin.wob AS original_cin_wob
        FROM {1} AS cin
        LEFT JOIN {2} AS wdsdalf
        ON cin.alf_e = wdsdalf.alf_e
    ) WITH DATA
'''.format(mcin_alf, cin_alf, wdsdalf)
al.call_no_return(q)

## CIN MAIN 
# Backfill IRNs from within the dataset
q = 'SELECT * FROM {0} LIMIT 1'.format(cin_main)
cincols = al.call(q).columns.to_list()

irndx = cincols.index('irn_e')
bfirn = cincols[:irndx]
afirn = cincols[irndx+1:]

q = '''
CALL fnc.drop_if_exists('{0}');
'''.format(mcin_main)
al.call_no_return(q)

if not gn.check_table_exists(mcin_main):
    q = '''
    CREATE TABLE {0} AS (
        SELECT CONCAT(CONCAT(child_code_e, '_'), local_authority_code) AS hybrid_id, {1}, 
            FIRST_VALUE(irn_e) OVER (PARTITION BY CONCAT(CONCAT(child_code_e, '_'), local_authority_code) ORDER BY irn_e) AS irn_e, {2}
        FROM {3}
    ) WITH DATA
'''.format(mcin_main, ','.join(bfirn), ','.join(afirn), cin_main)
al.call_no_return(q)

## CRCS ALF
# ALF table -- correct the WOBs where there's an ALF and where there isn't give a flag for if the wob is potentially unknown

q = '''
CALL fnc.drop_if_exists('{0}')
'''.format(mcrcs_alf)
al.call_no_return(q)

if not gn.check_table_exists(mcrcs_alf):
    q = '''
    CREATE TABLE {0} AS (
        SELECT DISTINCT cin.child_code_e, cin.local_authority_code, cin.alf_e, cin.alf_mtch_pct, cin.alf_sts_cd, 
        CASE
            WHEN cin.wob != wdsdalf.wob AND cin.alf_e IS NOT NULL THEN wdsdalf.wob
            ELSE cin.wob
        END AS wob,
        cin.gndr_cd, 
        cin.lsoa2011_cd,
        CONCAT(CONCAT(cin.child_code_e, '_'), cin.local_authority_code) AS hybrid_id, 
        CASE
            WHEN cin.wob != wdsdalf.wob AND cin.alf_e IS NOT NULL THEN 1
            WHEN cin.wob = wdsdalf.wob AND cin.alf_e IS NOT NULL THEN 0
            ELSE NULL
        END AS has_wob_been_corrected,
        cin.wob AS original_crcs_wob
        FROM {1} AS cin
        LEFT JOIN {2} AS wdsdalf
        ON cin.alf_e = wdsdalf.alf_e
    ) WITH DATA
'''.format(mcrcs_alf, crcs_alf, wdsdalf)
al.call_no_return(q)

## CRCS MAIN
# Backfill IRNs from within the dataset
q = 'SELECT * FROM {0} LIMIT 1'.format(crcs_main)
crcscols = al.call(q).columns.to_list()

irndx = crcscols.index('irn_e')
bfirn = crcscols[:irndx]
afirn = crcscols[irndx+1:]

q = '''
CALL fnc.drop_if_exists('{0}')
'''.format(mcrcs_main)
al.call_no_return(q)

if not gn.check_table_exists(mcrcs_main):
    q = '''
    CREATE TABLE {0} AS (
        SELECT CONCAT(CONCAT(child_code_e, '_'), local_authority_code) AS hybrid_id, {1}, 
            FIRST_VALUE(irn_e) OVER (PARTITION BY CONCAT(CONCAT(child_code_e, '_'), local_authority_code) ORDER BY irn_e) AS irn_e, {2}
        FROM {3}
    ) WITH DATA
'''.format(mcrcs_main, ','.join(bfirn), ','.join(afirn), crcs_main)
al.call_no_return(q)

# FIXING TABLES 

## Correct duplicates that are in the ALF table - duplicates of the hybrid_id i.e where a child has >1 gender or >1 alf
q = '''
SELECT alf.*, CASE WHEN main.irn_e IS NULL THEN 0 ELSE 1 END AS has_irn
FROM (
    SELECT hybrid_id, COUNT(*) AS numocc, COUNT(DISTINCT(wob)) AS nwob, COUNT(DISTINCT(gndr_cd)) AS ngender, COUNT(DISTINCT(alf_e)) AS nalf
    FROM {0}
    GROUP BY hybrid_id
) AS alf
LEFT JOIN (
    SELECT hybrid_id, MAX(irn_e) AS irn_e
    FROM {1}
    GROUP BY hybrid_id
) AS main
ON alf.hybrid_id = main.hybrid_id
WHERE numocc > 1
'''.format(mlac_alf, mlac_main)
try:
    probs = al.call(q)
except ValueError as e:
    pass
    probs = False
    print('no dupes to fix :-)')

# Deal with the multiple ALFs first, since they all have IRNs just go by what that says
if probs is not False:
    aprobs = list(probs[probs['nalf'] > 1]['hybrid_id'].unique())
    for child in aprobs:
        q = '''
        SELECT * 
        FROM {0} 
        WHERE irn_e IN (
            SELECT DISTINCT irn_e FROM {1} WHERE hybrid_id = '{2}' AND irn_e IS NOT NULL
        )
        '''.format(eduw_alf, mlac_main, child)
        deets2 = al.call(q)
        alf_keep = deets2['alf_e'].iloc[0]
        q = '''DELETE FROM {0} WHERE alf_e != {1} AND hybrid_id = '{2}' '''.format(mlac_alf, alf_keep, child)
        al.call_no_return(q)

# now deal with the rest -- all have multiple genders
if probs is not False:
    aprobs = list(probs[probs['nalf'] <= 1]['hybrid_id'].unique())
    for child in aprobs:
        q = "SELECT * FROM {0} WHERE hybrid_id = '{1}'".format(mlac_alf, child)
        deets = al.call(q)
        q = '''
        SELECT * 
        FROM {0} 
        WHERE irn_e IN (
            SELECT DISTINCT irn_e FROM {1} WHERE hybrid_id = '{2}' AND irn_e IS NOT NULL
        )
        '''.format(eduw_alf, mlac_main, child)
        deets2 = al.call(q)
        gndr_keep = deets2['gndr_cd'].iloc[0]
        q = '''DELETE FROM {0} WHERE gndr_cd != {1} AND hybrid_id = '{2}' '''.format(mlac_alf, gndr_keep, child)
        al.call_no_return(q)

## Deal with people missing from the ALF table
q = '''
ALTER TABLE {0}
ADD COLUMN missing_from_lacalf INT DEFAULT 0
ADD COLUMN alf_source VARCHAR(255) DEFAULT 'DHCW'
'''.format(mlac_alf)
al.call_no_return(q)
q = '''
INSERT INTO 
    {1} (system_id_e, local_authority_code, alf_e, alf_mtch_pct, alf_sts_cd, wob, gndr_cd, hybrid_id, max_poss_wob, potential_unknown_wob, has_wob_been_corrected,
    original_lac_wob, lacwob_def_wrong, first_ep_start, missing_from_lacalf, alf_source)
SELECT DISTINCT main.system_id_e, main.local_authority_code, 
CASE
    WHEN eduw_alf.alf_sts_cd != 99 THEN eduw_alf.alf_e
    ELSE NULL
END AS alf_e, eduw_alf.alf_mtch_pct, eduw_alf.alf_sts_cd, eduw_alf.wob, eduw_alf.gndr_cd, main.hybrid_id,
CASE
    WHEN eduw_alf.irn_e IS NOT NULL THEN NULL
    ELSE ep.fstart
END AS max_poss_wob, 
CASE
    WHEN eduw_alf.irn_e IS NOT NULL THEN 0
    ELSE 1
END potential_unknown_wob, NULL AS has_wob_been_corrected, NULL as original_lac_wob, 1 AS lacwob_def_wrong, ep.fstart AS first_ep_start,
1 AS missing_from_lacalf,
CASE
    WHEN eduw_alf.irn_e IS NOT NULL AND eduw_alf.alf_sts_cd != 99 THEN 'EDUW'
    ELSE NULL
END AS alf_source
FROM {0} AS main
LEFT JOIN {1} AS alf
ON main.hybrid_id = alf.hybrid_id
LEFT JOIN {2} AS eduw_alf
ON main.irn_e = eduw_alf.irn_e
LEFT JOIN (
    SELECT hybrid_id, MIN(episode_start_date) AS fstart
    FROM {3}
    GROUP BY hybrid_id
) AS ep
ON main.hybrid_id = ep.hybrid_id
WHERE alf.wob IS NULL
'''.format(mlac_main, mlac_alf, eduw_alf, mlac_epi)
al.call_no_return(q)

## Get people who don't have a final episode and date recorded 
q = '''
SELECT epi.*, alf.original_lac_wob AS wob, alf.gndr_cd, main.irn_e, mainhp.lsoa2011_home_postcode, alf.alf_e, alf.alf_sts_cd, main.ever_asylum
FROM (
    SELECT 
    ROW_NUMBER() OVER (
            PARTITION BY hybrid_id
            ORDER BY year_code DESC, episode_number DESC
        ) AS row_num, hybrid_id, system_id_e, year_code, local_authority_code, episode_start_date, placement_type_code, category_of_need_code, episode_end_date, 
        legal_status_code, reason_episode_started_code, reason_episode_finished_code, lsoa2011_placement_postcode
    FROM {0}
    ORDER BY hybrid_id, year_code, episode_number
) AS epi
LEFT JOIN (
    SELECT DISTINCT hybrid_id, irn_e, MAX(asylum) as ever_asylum
    FROM {1}
    GROUP BY hybrid_id, irn_e
) AS main
ON epi.hybrid_id = main.hybrid_id
LEFT JOIN (
    SELECT *
    FROM (
        SELECT hybrid_id, lsoa2011_home_postcode, 
        ROW_NUMBER() OVER (
            PARTITION BY hybrid_id
            ORDER BY year_code DESC
        ) AS row_num
        FROM {1}
    ) WHERE row_num = 1
) AS mainhp
ON epi.hybrid_id = mainhp.hybrid_id
LEFT JOIN {2} AS alf
ON epi.hybrid_id = alf.hybrid_id
WHERE episode_end_date IS NULL
AND epi.row_num = 1
AND year_code != 202021
'''.format(mlac_epi, mlac_main, mlac_alf)
missing_end = al.call(q)

## Get people who don't have an episode start date
q = '''
SELECT epi.*, alf.original_lac_wob AS wob, alf.potential_unknown_wob, alf.gndr_cd, main.irn_e, mainhp.lsoa2011_home_postcode, alf.alf_e, alf.alf_sts_cd, main.ever_asylum
FROM (
    SELECT 
    ROW_NUMBER() OVER (
            PARTITION BY hybrid_id
            ORDER BY year_code, episode_number 
        ) AS row_num, hybrid_id, system_id_e, year_code, local_authority_code, episode_start_date, placement_type_code, category_of_need_code, episode_end_date, 
        legal_status_code, reason_episode_started_code, reason_episode_finished_code, lsoa2011_placement_postcode
    FROM {0}
) AS epi
LEFT JOIN (
    SELECT hybrid_id, irn_e, MAX(asylum) AS ever_asylum
    FROM {1}
    GROUP BY hybrid_id, irn_e
) AS main
ON epi.hybrid_id = main.hybrid_id
LEFT JOIN (
    SELECT *
    FROM (
        SELECT hybrid_id, lsoa2011_home_postcode, 
        ROW_NUMBER() OVER (
            PARTITION BY hybrid_id
            ORDER BY year_code
        ) AS row_num
        FROM {1}
    ) WHERE row_num = 1
) AS mainhp
ON epi.hybrid_id = mainhp.hybrid_id
LEFT JOIN {2} AS alf
ON epi.hybrid_id = alf.hybrid_id
WHERE epi.row_num = 1
AND year_code > 200304
AND UPPER(STRIP(reason_episode_started_code, BOTH)) != 'S'
'''.format(mlac_epi, mlac_main, mlac_alf)
missing_start = al.call(q)

## Get all people
q = '''
SELECT epi.*, alf.original_lac_wob AS wob, alf.gndr_cd, main.irn_e, mainhp.lsoa2011_home_postcode, alf.alf_e, alf.alf_sts_cd, main.ever_asylum
FROM (
    SELECT 
    ROW_NUMBER() OVER (
            PARTITION BY hybrid_id
            ORDER BY year_code, episode_number 
        ) AS row_num, hybrid_id, system_id_e, year_code, local_authority_code, episode_start_date, placement_type_code, category_of_need_code, episode_end_date, 
        legal_status_code, reason_episode_started_code, reason_episode_finished_code, lsoa2011_placement_postcode
    FROM {0}
) AS epi
LEFT JOIN (
    SELECT hybrid_id, MAX(irn_e) AS irn_e, MAX(asylum) AS ever_asylum
    FROM {1}
    GROUP BY hybrid_id
) AS main
ON epi.hybrid_id = main.hybrid_id
LEFT JOIN (
    SELECT *
    FROM (
        SELECT hybrid_id, lsoa2011_home_postcode, 
        ROW_NUMBER() OVER (
            PARTITION BY hybrid_id
            ORDER BY year_code
        ) AS row_num
        FROM {1}
    ) WHERE row_num = 1
) AS mainhp
ON epi.hybrid_id = mainhp.hybrid_id
LEFT JOIN {2} AS alf
ON epi.hybrid_id = alf.hybrid_id
WHERE epi.row_num = 1
'''.format(mlac_epi, mlac_main, mlac_alf)
all_first = al.call(q)
all_first = all_first[~all_first['hybrid_id'].isin(list(missing_end['hybrid_id'].unique()))]

## Use RecordLinkage program to link easy records 

# make the hybrid_id the dataframe indexes so it's easier when we get to the output
miss_end_idx = missing_end.set_index('hybrid_id')
miss_start_idx = missing_start.set_index('hybrid_id')
all_first_idx = all_first.set_index('hybrid_id')

indexer = recordlinkage.Index()
# *at a minimum* they HAVE to match on epi start, la code, gender, and their wob
indexer.block(["episode_start_date", "local_authority_code", 'wob', 'gndr_cd', 'legal_status_code', 'reason_episode_started_code'])
# candidate_links = indexer.index(miss_end_idx, miss_start_idx)
candidate_links = indexer.index(miss_end_idx, all_first_idx)

compare_cl = recordlinkage.Compare()
compare_cl.exact('wob', 'wob', label='wob')
compare_cl.exact('gndr_cd', 'gndr_cd', label='gndr_cd')
compare_cl.exact('local_authority_code', 'local_authority_code', label='loc_auth_code')
compare_cl.exact('episode_start_date', 'episode_start_date', label='episode_start_date')
compare_cl.exact('reason_episode_started_code', 'reason_episode_started_code', label='reason_episode_started_code')
compare_cl.exact('placement_type_code', 'placement_type_code', label='placement_type_code')
compare_cl.exact('legal_status_code', 'legal_status_code', label='legal_status_code')
compare_cl.string('lsoa2011_placement_postcode', 'lsoa2011_placement_postcode', threshold=0.5, label='placement_lsoa')
compare_cl.string('lsoa2011_placement_postcode', 'lsoa2011_home_postcode', threshold=0.5, label='home_lsoa')
# features = compare_cl.compute(candidate_links, miss_end_idx, miss_start_idx)
features = compare_cl.compute(candidate_links, miss_end_idx, all_first_idx)

# they have to match on wob, gender, epi start date, la code, and at least another 2 columns (reason episode started, placement type, legal status, placement lsoa, home lsoa)
matches = features[features.sum(axis=1) >= 7].reset_index()

# just get the people who only have 1 match
mr = matches.groupby(['hybrid_id_1'])['hybrid_id_2'].count().to_frame().reset_index()
keepers = list(mr[mr['hybrid_id_2'] == 1]['hybrid_id_1'].unique())
fmatches = matches[matches['hybrid_id_1'].isin(keepers)]
rlmatchdf = fmatches[['hybrid_id_1','hybrid_id_2']].rename(columns={'hybrid_id_1':'earlier_sysid', 'hybrid_id_2':'later_sysid'})

## Moving onto the ones that RecordLinkage wasn't able to do
# we know that 516, 518 changed all their system ids in 200809, 520 in 200910, and 514 in 201011
# there are such tiny numbers of people outside these LAs and these years with issues that we can put them down to
# data entry errors
change_map = { 516: 200809, 518: 200809, 520: 200910, 514: 201011 }

rmissing_end = missing_end[~missing_end['hybrid_id'].isin(list(rlmatchdf['earlier_sysid'].unique()))]
all_first = all_first[(~all_first['hybrid_id'].isin(list(rlmatchdf['earlier_sysid'].unique()))) &
                      (~all_first['hybrid_id'].isin(list(rlmatchdf['later_sysid'].unique())))]

rmissing_start = missing_start[(~missing_start['hybrid_id'].isin(list(rlmatchdf['later_sysid'].unique())))]

def add_to_matchdf(df, earlier, later, all_first):
    tser = pd.DataFrame({'earlier_sysid': [earlier], 'later_sysid': [later]})
    udf = pd.concat([df, tser], ignore_index=True, axis=0)
    
    # also inplace remove them from the pool of potential future matches 
    all_first = all_first[all_first['hybrid_id'] != later]
    return udf, all_first

for index, row in tqdm(rmissing_end.iterrows(), total=rmissing_end.shape[0]):
    if int(float(row['ever_asylum'])) == 1:
        continue
        
    next_year = row['year_code'] + 101
    matcher = all_first[(all_first['wob'] == row['wob']) & 
          (all_first['gndr_cd'] == row['gndr_cd']) &
          (all_first['local_authority_code'] == row['local_authority_code']) &
          (all_first['year_code'] == next_year) &
          (all_first['episode_start_date'] == row['episode_start_date']) & 
          (all_first['legal_status_code'] == row['legal_status_code'])]
    if len(matcher) == 1:
        rlmatchdf, all_first = add_to_matchdf(rlmatchdf, row['hybrid_id'], matcher['hybrid_id'].iloc[0], all_first)
        continue
        
    matcher = all_first[(all_first['local_authority_code'] == row['local_authority_code']) &
          (all_first['year_code'] == next_year) &
          (all_first['episode_start_date'] == row['episode_start_date']) & 
          (all_first['legal_status_code'] == row['legal_status_code']) &
          ((all_first['lsoa2011_placement_postcode'] == row['lsoa2011_placement_postcode']) | (all_first['lsoa2011_home_postcode'] == row['lsoa2011_home_postcode']))]
    if len(matcher) == 1:
        rlmatchdf, all_first = add_to_matchdf(rlmatchdf, row['hybrid_id'], matcher['hybrid_id'].iloc[0], all_first)
        continue
        
    if ethn_in_lac and len(matcher) > 0:
        q = '''SELECT DISTINCT year_code, ethnicity FROM {0} WHERE hybrid_id = '{1}' ORDER BY year_code DESC LIMIT 1'''.format(mlac_main, row['hybrid_id'])
        pers_ethn = al.call(q)['ethnicity'].iloc[0]
        q = '''SELECT DISTINCT hybrid_id FROM {0} WHERE hybrid_id IN ({1}) AND ethnicity = '{2}' '''.format(mlac_main, 
                                                                                                            ','.join(["'"+x+"'" for x in list(matcher['hybrid_id'].unique())]), 
                                                                                                            pers_ethn)
        matching = al.call(q)
        matcher = matcher[matcher['hybrid_id'].isin(list(matching['hybrid_id'].unique()))]
    if len(matcher) == 1:
        rlmatchdf, all_first = add_to_matchdf(rlmatchdf, row['hybrid_id'], matcher['hybrid_id'].iloc[0], all_first)
        continue
    
    if len(matcher) > 1:
        # if the two match rows are fully identical, with the exception of the alf columns, then pick the first one
        subcols = ['year_code','local_authority_code','episode_start_date','placement_type_code','category_of_need_code',
               'legal_status_code','reason_episode_started_code','lsoa2011_placement_postcode','wob','gndr_cd','lsoa2011_home_postcode']
        if np.all(matcher[subcols].iloc[0] == matcher[subcols].iloc[1]) and matcher['alf_e'].iloc[0] != matcher['alf_e'].iloc[1]:
            matcher = matcher.iloc[:1]
        if len(matcher) == 1:
            rlmatchdf, all_first = add_to_matchdf(rlmatchdf, row['hybrid_id'], matcher['hybrid_id'].iloc[0], all_first)
            continue

        # if there are multiple matches but the wob in the match is a) present and b) doesn't match this child then dispose of that row
        matcher = matcher[~(~matcher['wob'].isna()) & (matcher['wob'] != row['wob'])]
        if len(matcher) == 1:
            rlmatchdf, all_first = add_to_matchdf(rlmatchdf, row['hybrid_id'], matcher['hybrid_id'].iloc[0], all_first)
            continue
        
    if len(matcher) != 1 and row['local_authority_code'] in change_map.keys():
        continue

print("Found matches for {0} of {1} children who were missing final episodes!".format(len(rlmatchdf), len(missing_end)))

# Put the mapping table into the DB 
al.make_table(rlmatchdf, mlac_map.split('.')[0], mlac_map.split('.')[1])

### Add correction columns to the other existing LAC tables
#### ALF 

wsc = mlac_map.split('.')[0]

q = '''
CREATE TABLE {0}.TMP_LAC_ALF AS (
    SELECT *
    FROM {1}
) WITH DATA
'''.format(wsc, mlac_alf)
al.call_no_return(q)

q = '''
DROP TABLE {0}
'''.format(mlac_alf)
al.call_no_return(q)

q = '''
CREATE TABLE {0} AS (
    SELECT *,
    CASE
        WHEN sysid_changes_to IS NOT NULL THEN sysid_changes_to
        ELSE hybrid_id
    END AS unified_id
    FROM (
          SELECT alf.*,
            CASE
                WHEN map.earlier_sysid IS NOT NULL or lmap.later_sysid IS NOT NULL THEN 1 
                ELSE 0
            END AS sys_id_changes,
            map.later_sysid AS sysid_changes_to,
            lmap.earlier_sysid AS sysid_changed_from
            FROM {1}.TMP_LAC_ALF AS alf
            LEFT JOIN {2} AS map
            ON alf.hybrid_id = map.earlier_sysid
            LEFT JOIN {2} AS lmap
            ON alf.hybrid_id = lmap.later_sysid
        )
) WITH DATA
'''.format(mlac_alf, wsc, mlac_map)
al.call_no_return(q)

q = '''
DROP TABLE {0}.TMP_LAC_ALF
'''.format(wsc)
al.call_no_return(q)

### LAC Main 
q = '''
CREATE TABLE {0}.TMP_LAC_MAIN AS (
    SELECT *
    FROM {1}
) WITH DATA
'''.format(wsc, mlac_main)
al.call_no_return(q)

q = '''
DROP TABLE {0}
'''.format(mlac_main)
al.call_no_return(q)

q = '''
CREATE TABLE {0} AS (
    SELECT *,
    CASE
        WHEN sysid_changes_to IS NOT NULL THEN sysid_changes_to
        ELSE hybrid_id
    END AS unified_id
    FROM (
          SELECT main.*,
            CASE
                WHEN map.earlier_sysid IS NOT NULL or lmap.later_sysid IS NOT NULL THEN 1 
                ELSE 0
            END AS sys_id_changes,
            map.later_sysid AS sysid_changes_to,
            lmap.earlier_sysid AS sysid_changed_from
            FROM {1}.TMP_LAC_MAIN AS main
            LEFT JOIN {2} AS map
            ON main.hybrid_id = map.earlier_sysid
            LEFT JOIN {2} AS lmap
            ON main.hybrid_id = lmap.later_sysid
        )
) WITH DATA
'''.format(mlac_main, wsc, mlac_map)
al.call_no_return(q)

q = '''
DROP TABLE {0}.TMP_LAC_MAIN
'''.format(wsc)
al.call_no_return(q)

# LAC Episode

q = '''
CREATE TABLE {0}.TMP_LAC_EPI AS (
    SELECT *
    FROM {1}
) WITH DATA
'''.format(wsc, mlac_epi)
al.call_no_return(q)

q = '''
DROP TABLE {0}
'''.format(mlac_epi)
al.call_no_return(q)

q = '''
CREATE TABLE {0} AS (
    SELECT *,
    CASE
        WHEN sysid_changes_to IS NOT NULL THEN sysid_changes_to
        ELSE hybrid_id
    END AS unified_id
    FROM (
          SELECT epi.*,
            CASE
                WHEN map.earlier_sysid IS NOT NULL or lmap.later_sysid IS NOT NULL THEN 1 
                ELSE 0
            END AS sys_id_changes,
            map.later_sysid AS sysid_changes_to,
            lmap.earlier_sysid AS sysid_changed_from
            FROM {1}.TMP_LAC_EPI AS epi
            LEFT JOIN {2} AS map
            ON epi.hybrid_id = map.earlier_sysid
            LEFT JOIN {2} AS lmap
            ON epi.hybrid_id = lmap.later_sysid
        )
) WITH DATA
'''.format(mlac_epi, wsc, mlac_map)
al.call_no_return(q)

q = '''
DROP TABLE {0}.TMP_LAC_EPI
'''.format(wsc)
al.call_no_return(q)

## Use these newly-found unfied IDs to fix rows where WOB or gender are missing 

q = '''
SELECT *
FROM {0}
WHERE sys_id_changes = 1
'''.format(mlac_alf)
csid = al.call(q)

wgm = csid[csid['missing_from_lacalf'] == 1]

for index, row in tqdm(wgm.iterrows(), total=len(wgm)):
    hid = row['hybrid_id']
    uid = row['unified_id']
    sid = row['system_id_e']
    lacd = row['local_authority_code']
    
    replace = csid[(csid['unified_id'] == uid) & (csid['hybrid_id'] != hid)]
    if len(replace) != 1:
        print('errr')
        break
        
    rrow = replace.iloc[0]    
    alf = rrow['alf_e']
    alf_pct = rrow['alf_mtch_pct']
    alf_sts = int(rrow['alf_sts_cd'])
    wob = rrow['wob']
    gndr = int(rrow['gndr_cd'])
    max_wob = rrow['max_poss_wob']
    pot_uk_wob = int(rrow['potential_unknown_wob'])
    wob_corr = rrow['has_wob_been_corrected']
    orig_wob = rrow['original_lac_wob']
    lacwob_wrong = int(rrow['lacwob_def_wrong'])
    if row['sysid_changes_to']:
        fepstart = row['first_ep_start']
    else:
        fepstart = rrow['first_ep_start']
    missing_from_lacalf = 0
    alf_source = rrow['alf_source']
    sidchanges = 1
    sysid_changes_to = row['sysid_changes_to']
    sysid_change_from = row['sysid_changed_from']
    
    newrow = [sid, lacd, alf, alf_pct, alf_sts, wob, gndr, hid, max_wob, pot_uk_wob, wob_corr, orig_wob, lacwob_wrong, 
              fepstart, missing_from_lacalf, alf_source, sidchanges, sysid_changes_to, sysid_change_from, uid]
    lnewrow = ['NULL' if str(x) == 'nan' or x is None else x for x in newrow]
    
    q = '''
    INSERT INTO {0}(system_id_e, local_authority_code, alf_e, alf_mtch_pct, alf_sts_cd, wob, gndr_cd, hybrid_id, max_poss_wob, potential_unknown_wob, has_wob_been_corrected,
    original_lac_wob, lacwob_def_wrong, first_ep_start, missing_from_lacalf, alf_source, sys_id_changes, sysid_changes_to, sysid_changed_from, unified_id)
    
    VALUES({1})
    '''.format(mlac_alf, ','.join([str(x) if type(x) == int or x=='NULL' else "'"+str(x)+"'" for x in lnewrow]))
    
    al.call_no_return(q)
    
    q = '''
    DELETE FROM {0} 
    WHERE hybrid_id = '{1}'
    AND wob IS NULL '''.format(mlac_alf, hid)
    
    al.call_no_return(q)

## Use these newly-found unified IDS to fix rows where IRNs are missing 
q = '''
SELECT DISTINCT hybrid_id, unified_id, irn_e, sys_id_changes, sysid_changes_to, sysid_changed_from
FROM {0}
WHERE sys_id_changes = 1
'''.format(mlac_main)
csid = al.call(q)

irnm = csid[csid['irn_e'].isna()]

q = '''
SELECT * FROM {0} LIMIT 1
'''.format(mlac_main)
maincols = al.call(q).columns.to_list()
irnidx = maincols.index('irn_e')
bfirn = maincols[:irnidx]
afirn = maincols[irnidx+1:]

for index, row in tqdm(irnm.iterrows(), total=len(irnm)):
    uid = row['unified_id']
    hid = row['hybrid_id']
    
    rrow = csid[(csid['unified_id'] == uid) & (csid['hybrid_id'] != hid)].iloc[0]
    if np.isnan(rrow['irn_e']):
        continue
    nirn = int(rrow['irn_e'])
    
    q = '''
    INSERT INTO {0}({1})

    SELECT {2}, {3} AS irn_e, {4}
    FROM {0}
    WHERE hybrid_id = '{5}' 
    '''.format(mlac_main, ','.join(maincols), ','.join(bfirn), nirn, ','.join(afirn), hid)
    
    al.call_no_return(q)
    
    q = '''
    DELETE FROM {0} 
    WHERE hybrid_id = '{1}'
    AND irn_e IS NULL '''.format(mlac_main, hid)
    
    al.call_no_return(q)

# CROSSFILL CIN CRCS
## Crossfill into CIN 
q = 'SELECT * FROM {0} LIMIT 1'.format(mcin_main)
cincols = al.call(q).columns.to_list()

irndx = cincols.index('irn_e')
bfirn = cincols[:irndx]
afirn = cincols[irndx+1:]

q = '''
INSERT INTO {0}({1})

SELECT {2}, crcs_irn AS irn_e, {3} 
FROM (
    SELECT cin.*, crcs.crcs_irn
    FROM {0} AS cin
    LEFT JOIN (
        SELECT hybrid_id, MAX(irn_e) AS crcs_irn
        FROM {4}
        GROUP BY hybrid_id
    ) AS crcs
    ON cin.hybrid_id = crcs.hybrid_id
    WHERE cin.irn_e IS NULL
    AND crcs.crcs_irn IS NOT NULL
)
'''.format(mcin_main, ','.join(cincols), ','.join(bfirn), ','.join(afirn), mcrcs_main)
al.call_no_return(q)

q = '''
DELETE 
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id
    FROM (
        SELECT cin.*, crcs.crcs_irn
        FROM {0} AS cin
        LEFT JOIN (
            SELECT hybrid_id, MAX(irn_e) AS crcs_irn
            FROM {1}
            GROUP BY hybrid_id
        ) AS crcs
        ON cin.hybrid_id = crcs.hybrid_id
        WHERE cin.irn_e IS NULL
        AND crcs.crcs_irn IS NOT NULL
    )
) AND irn_e IS NULL
'''.format(mcin_main, mcrcs_main)
al.call_no_return(q)

## Crossfill into CRCS 
q = 'SELECT * FROM {0} LIMIT 1'.format(mcrcs_main)
crcscols = al.call(q).columns.to_list()

irndx = crcscols.index('irn_e')
bfirn = crcscols[:irndx]
afirn = crcscols[irndx+1:]

q = '''
INSERT INTO {0}({1})

SELECT {2}, cin_irn AS irn_e, {3} 
FROM (
    SELECT crcs.*, cin.cin_irn
    FROM {0} AS crcs
    LEFT JOIN (
        SELECT hybrid_id, MAX(irn_e) AS cin_irn
        FROM {4}
        GROUP BY hybrid_id
    ) AS cin
    ON crcs.hybrid_id = cin.hybrid_id
    WHERE crcs.irn_e IS NULL
    AND cin.cin_irn IS NOT NULL
)
'''.format(mcrcs_main, ','.join(crcscols), ','.join(bfirn), ','.join(afirn), mcin_main)
al.call_no_return(q)

q = '''
DELETE 
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id
    FROM (
        SELECT crcs.*, cin.cin_irn
        FROM {0} AS crcs
        LEFT JOIN (
            SELECT hybrid_id, MAX(irn_e) AS cin_irn
            FROM {1}
            GROUP BY hybrid_id
        ) AS cin
        ON crcs.hybrid_id = cin.hybrid_id
        WHERE crcs.irn_e IS NULL
        AND cin.cin_irn IS NOT NULL
    )
) AND irn_e IS NULL
'''.format(mcrcs_main, mcin_main)
al.call_no_return(q)

## Crossfill into LAC from CIN 
q = 'SELECT * FROM {0} LIMIT 1'.format(mlac_main)
laccols = al.call(q).columns.to_list()

irndx = laccols.index('irn_e')
bfirn = laccols[:irndx]
afirn = laccols[irndx+1:]

### unchanged system IDs first
q = '''
INSERT INTO {0}({1})

SELECT {2}, cin.cin_irn AS irn_e, {3}
FROM {0} AS lac
LEFT JOIN (
    SELECT DISTINCT hybrid_id AS chybrid_id, irn_e AS cin_irn
    FROM {4}
    WHERE irn_e IS NOT NULL
) AS cin
ON lac.unified_id = cin.chybrid_id
WHERE lac.irn_e IS NULL
AND lac.sys_id_changes = 0
AND cin.cin_irn IS NOT NULL
'''.format(mlac_main, ','.join(laccols), ','.join(bfirn), ','.join(afirn), mcin_main)
al.call_no_return(q)

q = '''
DELETE 
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id
    FROM (
        SELECT lac.*, cin.cin_irn
        FROM {0} AS lac
        LEFT JOIN (
            SELECT hybrid_id, MAX(irn_e) AS cin_irn
            FROM {1}
            GROUP BY hybrid_id
        ) AS cin
        ON lac.unified_id = cin.hybrid_id
        WHERE lac.irn_e IS NULL
        AND lac.sys_id_changes = 0
        AND cin.cin_irn IS NOT NULL
    )
) AND irn_e IS NULL
'''.format(mlac_main, mcin_main)
al.call_no_return(q)

### Changed system IDs
q = '''
INSERT INTO {0}({1})

SELECT {2}, cin.cin_irn AS irn_e, {3}
FROM {0} AS lac
LEFT JOIN (
    SELECT DISTINCT hybrid_id AS chybrid_id, irn_e AS cin_irn
    FROM {4}
    WHERE irn_e IS NOT NULL
) AS cin
ON lac.unified_id = cin.chybrid_id
WHERE lac.irn_e IS NULL
AND lac.sys_id_changes = 1
AND cin.cin_irn IS NOT NULL
'''.format(mlac_main, ','.join(laccols), ','.join(bfirn), ','.join(afirn), mcin_main)
al.call_no_return(q)

q = '''
DELETE 
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id
    FROM (
        SELECT lac.*, cin.cin_irn
        FROM {0} AS lac
        LEFT JOIN (
            SELECT hybrid_id, MAX(irn_e) AS cin_irn
            FROM {1}
            GROUP BY hybrid_id
        ) AS cin
        ON lac.unified_id = cin.hybrid_id
        WHERE lac.irn_e IS NULL
        AND lac.sys_id_changes = 1
        AND cin.cin_irn IS NOT NULL
    )
) AND irn_e IS NULL
'''.format(mlac_main, mcin_main)
al.call_no_return(q)

## Crossfill into LAC from CRCS
q = 'SELECT * FROM {0} LIMIT 1'.format(mlac_main)
laccols = al.call(q).columns.to_list()

irndx = laccols.index('irn_e')
bfirn = laccols[:irndx]
afirn = laccols[irndx+1:]

### Unchanged system IDs first 
q = '''
INSERT INTO {0}({1})

SELECT {2}, cin.cin_irn AS irn_e, {3}
FROM {0} AS lac
LEFT JOIN (
    SELECT DISTINCT hybrid_id AS chybrid_id, irn_e AS cin_irn
    FROM {4}
    WHERE irn_e IS NOT NULL
) AS cin
ON lac.unified_id = cin.chybrid_id
WHERE lac.irn_e IS NULL
AND lac.sys_id_changes = 0
AND cin.cin_irn IS NOT NULL
'''.format(mlac_main, ','.join(laccols), ','.join(bfirn), ','.join(afirn), mcrcs_main)
al.call_no_return(q)

q = '''
DELETE 
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id
    FROM (
        SELECT lac.*, cin.cin_irn
        FROM {0} AS lac
        LEFT JOIN (
            SELECT hybrid_id, MAX(irn_e) AS cin_irn
            FROM {1}
            GROUP BY hybrid_id
        ) AS cin
        ON lac.unified_id = cin.hybrid_id
        WHERE lac.irn_e IS NULL
        AND lac.sys_id_changes = 0
        AND cin.cin_irn IS NOT NULL
    )
) AND irn_e IS NULL
'''.format(mlac_main, mcrcs_main)
al.call_no_return(q)

### Changed system IDs
q = '''
INSERT INTO {0}({1})

SELECT {2}, cin.cin_irn AS irn_e, {3}
FROM {0} AS lac
LEFT JOIN (
    SELECT DISTINCT hybrid_id AS chybrid_id, irn_e AS cin_irn
    FROM {4}
    WHERE irn_e IS NOT NULL
) AS cin
ON lac.unified_id = cin.chybrid_id
WHERE lac.irn_e IS NULL
AND lac.sys_id_changes = 1
AND cin.cin_irn IS NOT NULL
'''.format(mlac_main, ','.join(laccols), ','.join(bfirn), ','.join(afirn), mcrcs_main)
al.call_no_return(q)

q = '''
DELETE 
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id
    FROM (
        SELECT lac.*, cin.cin_irn
        FROM {0} AS lac
        LEFT JOIN (
            SELECT hybrid_id, MAX(irn_e) AS cin_irn
            FROM {1}
            GROUP BY hybrid_id
        ) AS cin
        ON lac.unified_id = cin.hybrid_id
        WHERE lac.irn_e IS NULL
        AND lac.sys_id_changes = 1
        AND cin.cin_irn IS NOT NULL
    )
) AND irn_e IS NULL
'''.format(mlac_main, mcrcs_main)
al.call_no_return(q)

# CORRECT MISSING WOB 
## Uing the crossfilled IRNs to correct missing WOB and gender 
def patch_row(pdct, uid):
    bstr = ''
    for k, v in pdct.items():
        bstr += '{0} = {1}, '.format(k, v)
    bstr = bstr[:-2]
    
    q = '''
    UPDATE {0}
    SET {1}
    WHERE unified_id = '{2}'
    '''.format(mlac_alf, bstr, uid)
    al.call_no_return(q)

q = '''
SELECT lac.unified_id, eduw_alf.*
FROM (
    SELECT alf.*, main.irn_e
    FROM {0} AS alf
    LEFT JOIN (
        SELECT unified_id, MAX(irn_e) AS irn_e
        FROM {1}
        GROUP BY unified_id
    ) AS main
    ON alf.unified_id = main.unified_id
    WHERE alf.wob IS NULL
    AND main.irn_e IS NOT NULL
) AS lac
LEFT JOIN {2} AS eduw_alf
ON lac.irn_e = eduw_alf.irn_e
'''.format(mlac_alf, mlac_main, eduw_alf)
to_patch = al.call(q)

for index, row in tqdm(to_patch.iterrows(), total=to_patch.shape[0]):
    pdct = {}
    pdct['wob'] = "'" + str(row['wob']) + "'"
    pdct['gndr_cd'] = row['gndr_cd']
    
    pdct['has_wob_been_corrected'] = 1
    pdct['lacwob_def_wrong'] = 1
    
    patch_row(pdct, row['unified_id'])

# BASIC MATCHING
# Boost CRCS via EDUW 
crcscols = gn.get_table_cols(mcrcs_alf)

# Boost CRCS via EDUW first
q = '''
INSERT INTO {0} ({1})

SELECT crcsalf.child_code_e, crcsalf.local_authority_code, eduw.alf_e, eduw.alf_mtch_pct, eduw.alf_sts_cd, wdsd.wob, eduw.gndr_cd, crcsalf.lsoa2011_cd,
    crcsalf.hybrid_id, 
    CASE
        WHEN crcsalf.wob != wdsd.wob THEN 1
        ELSE 0
    END AS has_wob_been_corrected, 
    crcsalf.original_crcs_wob
FROM {0} AS crcsalf
LEFT JOIN (
    SELECT DISTINCT hybrid_id, irn_e
    FROM 
    {2}
    WHERE irn_e IS NOT NULL
) AS crcsmain
ON crcsalf.hybrid_id = crcsmain.hybrid_id
LEFT JOIN (
    SELECT *
    FROM {3}
    WHERE alf_sts_cd IN (4, 39, 35)
    AND (alf_mtch_pct IS NULL OR alf_mtch_pct >= 0.8)
) AS eduw
ON crcsmain.irn_e = eduw.irn_e
LEFT JOIN {4} AS wdsd
ON eduw.alf_e = wdsd.alf_e
WHERE crcsalf.alf_e IS NULL
AND crcsmain.irn_e IS NOT NULL
AND eduw.alf_e IS NOT NULL
AND ABS(DAYS(wdsd.wob) - DAYS(crcsalf.wob)) < 365
'''.format(mcrcs_alf, ','.join(crcscols), mcrcs_main, eduw_alf, wdsdalf)
al.call_no_return(q)

### Drop the rows we've just populated
q = '''
DELETE
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id 
    FROM {0}
    WHERE alf_e IS NOT NULL
) 
AND alf_e IS NULL
'''.format(mcrcs_alf)
al.call_no_return(q)

### Check there's no weird dupe rows
q = '''
DELETE FROM 
(SELECT ROWNUMBER() OVER (PARTITION BY hybrid_id, alf_e, wob) AS rn
FROM {0}) AS A
WHERE rn > 1
'''.format(mcrcs_alf)
al.call_no_return(q)

## LACW via CRCS 
laccols = gn.get_table_cols(mlac_alf)

q = '''
INSERT INTO {0}({1})

SELECT lac.system_id_e, lac.local_authority_code, crcs.alf_e, crcs.alf_mtch_pct, crcs.alf_sts_cd, crcs.wob, crcs.gndr_cd, lac.hybrid_id, 
NULL as max_poss_wob, 0 AS potential_unknown_wob, crcs.has_wob_been_corrected, lac.original_lac_wob, 0 AS lacwob_def_wrong, lac.first_ep_start, 
0 AS missing_from_lacalf, 'CRCS' AS alf_source, lac.sys_id_changes, lac.sysid_changes_to, lac.sysid_changed_from, lac.unified_id
FROM {0} AS lac
LEFT JOIN {2} AS crcs
ON lac.unified_id = crcs.hybrid_id
WHERE lac.alf_e IS NULL
AND crcs.alf_e IS NOT NULL
AND ABS(DAYS(crcs.wob) - DAYS(lac.wob)) < 365
'''.format(mlac_alf, ','.join(laccols), mcrcs_alf)
al.call_no_return(q)

### Drop the rows we've just populated
q = '''
DELETE
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id 
    FROM {0}
    WHERE alf_e IS NOT NULL
) 
AND alf_e IS NULL
'''.format(mlac_alf)
al.call_no_return(q)

### Check there's no weird dupe rows
q = '''
DELETE FROM 
(SELECT ROWNUMBER() OVER (PARTITION BY hybrid_id, alf_e, wob) AS rn
FROM {0}) AS A
WHERE rn > 1
'''.format(mlac_alf)
al.call_no_return(q)

## CINW via EDUW 

cincols = gn.get_table_cols(mcin_alf)

### Boost CIN via EDUW now
q = '''
INSERT INTO {0} ({1})

SELECT cinwalf.child_code_e, cinwalf.local_authority_code, eduw.alf_e, eduw.alf_mtch_pct, eduw.alf_sts_cd, wdsd.wob, eduw.gndr_cd, cinwalf.lsoa2011_cd,
    cinwalf.hybrid_id, 
    CASE
        WHEN cinwalf.wob != wdsd.wob THEN 1
        ELSE 0
    END AS has_wob_been_corrected, 
    cinwalf.original_cin_wob
FROM {0} AS cinwalf
LEFT JOIN (
    SELECT DISTINCT hybrid_id, irn_e
    FROM 
    {2}
    WHERE irn_e IS NOT NULL
) AS cinwmain
ON cinwalf.hybrid_id = cinwmain.hybrid_id
LEFT JOIN (
    SELECT *
    FROM {3}
    WHERE alf_sts_cd IN (4, 39, 35)
    AND (alf_mtch_pct IS NULL OR alf_mtch_pct >= 0.8)
) AS eduw
ON cinwmain.irn_e = eduw.irn_e
LEFT JOIN {4} AS wdsd
ON eduw.alf_e = wdsd.alf_e
WHERE cinwalf.alf_e IS NULL
AND cinwmain.irn_e IS NOT NULL
AND eduw.alf_e IS NOT NULL
AND ABS(DAYS(wdsd.wob) - DAYS(cinwalf.wob)) < 365
'''.format(mcin_alf, ','.join(cincols), mcin_main, eduw_alf, wdsdalf)
al.call_no_return(q)

### Drop the rows we've just populated
q = '''
DELETE
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id 
    FROM {0}
    WHERE alf_e IS NOT NULL
) 
AND alf_e IS NULL
'''.format(mcin_alf)
al.call_no_return(q)

### Check there's no weird dupe rows
q = '''
DELETE FROM 
(SELECT ROWNUMBER() OVER (PARTITION BY hybrid_id, alf_e, wob) AS rn
FROM {0}) AS A
WHERE rn > 1
'''.format(mcin_alf)
al.call_no_return(q)

## LACW via CINW 
laccols = gn.get_table_cols(mlac_alf)

q = '''
INSERT INTO {0}({1})

SELECT lac.system_id_e, lac.local_authority_code, crcs.alf_e, crcs.alf_mtch_pct, crcs.alf_sts_cd, crcs.wob, crcs.gndr_cd, lac.hybrid_id, 
NULL as max_poss_wob, 0 AS potential_unknown_wob, crcs.has_wob_been_corrected, lac.original_lac_wob, 0 AS lacwob_def_wrong, lac.first_ep_start, 
0 AS missing_from_lacalf, 'CINW' AS alf_source, lac.sys_id_changes, lac.sysid_changes_to, lac.sysid_changed_from, lac.unified_id
FROM {0} AS lac
LEFT JOIN {2} AS crcs
ON lac.unified_id = crcs.hybrid_id
WHERE lac.alf_e IS NULL
AND crcs.alf_e IS NOT NULL
AND ABS(DAYS(crcs.wob) - DAYS(lac.wob)) < 365
'''.format(mlac_alf, ','.join(laccols), mcin_alf)
al.call_no_return(q)

# Drop the rows we've just populated
q = '''
DELETE
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id 
    FROM {0}
    WHERE alf_e IS NOT NULL
) 
AND alf_e IS NULL
'''.format(mlac_alf)
al.call_no_return(q)

# Check there's no weird dupe rows
q = '''
DELETE FROM 
(SELECT ROWNUMBER() OVER (PARTITION BY hybrid_id, alf_e, wob) AS rn
FROM {0}) AS A
WHERE rn > 1
'''.format(mlac_alf)
al.call_no_return(q)

## Overview
q = '''
SELECT COUNT(DISTINCT(unified_id)) AS ncinrcs
FROM {0}
WHERE alf_source IN ('CINW', 'CRCS')
'''.format(mlac_alf)
numf = al.call(q).iloc[0]['ncinrcs']

print('Number of ALFs found via CIN and CRCS: {0}'.format(numf))

#gn.lac_report(mlac_alf, lac_alf)

## LACW via EDUW 
q = '''
INSERT INTO {0}({1})

SELECT laca.system_id_e, laca.local_authority_code, eduw.alf_e, eduw.alf_mtch_pct, eduw.alf_sts_cd, eduw.wob, eduw.gndr_cd, laca.hybrid_id, NULL as max_poss_wob,
0 AS potential_unknown_wob, 
CASE
    WHEN laca.wob != eduw.wob THEN 1
    ELSE 0
END AS has_wob_been_corrected,
laca.original_lac_wob, laca.lacwob_def_wrong, laca.first_ep_start,  0 AS missing_from_lacalf, 'EDUW' AS alf_source, laca.sys_id_changes, laca.sysid_changes_to, 
laca.sysid_changed_from, laca.unified_id
FROM {0} AS laca
LEFT JOIN (
    SELECT DISTINCT unified_id, irn_e
    FROM {2}
    WHERE irn_e IS NOT NULL
) AS lacm
ON laca.unified_id = lacm.unified_id
LEFT JOIN (
    SELECT eduw.alf_e, eduw.alf_sts_cd, eduw.alf_mtch_pct, eduw.irn_e,
    CASE
        WHEN wdsd.wob IS NOT NULL THEN wdsd.wob
        ELSE eduw.wob
    END AS wob,
    CASE
        WHEN wdsd.gndr_cd IS NOT NULL THEN wdsd.gndr_cd
        ELSE eduw.gndr_cd
    END AS gndr_cd
    FROM {3} AS eduw
    LEFT JOIN {4} AS wdsd
    ON eduw.alf_e = wdsd.alf_e
    WHERE eduw.alf_sts_cd IN (4, 39, 35)
    AND (eduw.alf_mtch_pct IS NULL OR eduw.alf_mtch_pct >= 0.7)
) AS eduw
ON lacm.irn_e = eduw.irn_e
WHERE laca.alf_e IS NULL
AND lacm.irn_e IS NOT NULL
AND eduw.irn_e IS NOT NULL
AND ABS(DAYS(eduw.wob) - DAYS(laca.wob)) < 365
'''.format(mlac_alf, ','.join(laccols), mlac_main, eduw_alf, wdsdalf)
al.call_no_return(q)

# Drop the rows we've just populated
q = '''
DELETE
FROM {0}
WHERE hybrid_id IN (
    SELECT DISTINCT hybrid_id 
    FROM {0}
    WHERE alf_e IS NOT NULL
) 
AND alf_e IS NULL
'''.format(mlac_alf)
al.call_no_return(q)

# Check there's no weird dupe rows
q = '''
DELETE FROM 
(SELECT ROWNUMBER() OVER (PARTITION BY hybrid_id, alf_e, wob) AS rn
FROM {0}) AS A
WHERE rn > 1
'''.format(mlac_alf)
al.call_no_return(q)

## Overview
q = '''
SELECT COUNT(DISTINCT(unified_id)) AS n
FROM {0}
WHERE alf_source = 'EDUW'
'''.format(mlac_alf)
numf = al.call(q).iloc[0]['n']

print('Number of ALFs found via EDUW: {0}'.format(numf))

#gn.lac_report(mlac_alf, lac_alf)

## Total Status Report 
q = 'SELECT * FROM {0}'.format(mlac_alf)
df = al.call(q)

rpt = df.groupby(['alf_sts_cd', 'alf_source'])['hybrid_id'].count().to_frame()
rpt

# MAKE CLEANED EPI TABE 

q = '''
SELECT *, 
    CASE WHEN placement_localauth IS NULL AND placemnt_in_la = 1 THEN la_name
    ELSE placement_localauth
    END AS placement_la
FROM (
    SELECT 
        lem.unified_id,
        lem.sys_id_changes,
        lem.hybrid_id, 
        lem.EPISODE_START_DATE, 
        lem.EPISODE_END_DATE, 
        CASE WHEN upper(lem.REASON_EPISODE_STARTED_CODE) != 'L' THEN 1
            ELSE 0
        END AS is_placement_move,
        CASE WHEN lem.LSOA2011_PLACEMENT_POSTCODE IS NULL THEN 'X'
            ELSE lem.LSOA2011_PLACEMENT_POSTCODE
        END AS lsoa2011_placement_postcode, lem.la_name, 
        lem.placement_area, lem.placement_localauth,
        CASE WHEN UPPER(lem.PLACEMENT_TYPE_CODE) IN ('F1', 'F2', 'F3', 'H1', 'H3') THEN 1
            WHEN UPPER(lem.PLACEMENT_TYPE_CODE) IN ('F4', 'F5', 'F6', 'H21', 'H22', 'H4') THEN 0
            ELSE 2 END AS placemnt_in_la,
        CASE WHEN UPPER(lem.CATEGORY_OF_NEED_CODE) = 'N9' THEN 1
            ELSE 0 END AS adopt_disrupt,
        al.gndr_cd, al.wob 
    FROM {0} lem 
    LEFT JOIN
    {1} al
    ON lem.unified_id = al.unified_id
)'''.format(mlac_epi, mlac_alf)
cepidf = al.call(q)

## This takes a while to run 
cepidf['episode_start_date'] = gn.dt_type_fix(cepidf['episode_start_date'])
cepidf['episode_end_date'] = gn.dt_type_fix(cepidf['episode_end_date'])
cepidf['episode_end_date'].fillna(pd.Timestamp(2099,9,9), inplace=True)
cepidf['episode_end_date'] = gn.dt_type_fix(cepidf['episode_end_date'])

# cepidf['lsoa2011_placement_postcode'].fillna(0, inplace=True)
sc_epidf = cepidf.sort_values(['episode_start_date', 'episode_end_date', 
                          'lsoa2011_placement_postcode', 'placemnt_in_la'], ascending=[True, True, True, False])

cleanepi = sc_epidf.groupby(['episode_start_date', 'unified_id', 'gndr_cd',
              'wob']).agg({'episode_end_date':'first', 'is_placement_move':'first', 'lsoa2011_placement_postcode':'min',
                                 'placemnt_in_la':'first', 'la_name':'first', 'placement_area':'first', 'placement_la':'first', 'adopt_disrupt':'first'}).reset_index()

cleanepi['pers_row_num'] = cleanepi.groupby(['unified_id'])['unified_id'].cumcount()+1
cleanepi['la_name'] = cleanepi['la_name'].str.strip(' ')
cleanepi['placement_la'] = cleanepi['placement_la'].str.strip(' ')

pl = list(cleanepi['unified_id'].unique())
for person in tqdm(pl):
    epidx = list(cleanepi[cleanepi['unified_id'] == person].index)
    epidx.reverse()
    for i in range(len(epidx)-1):
        thisrow = cleanepi.iloc[epidx[i]]
        prevrow = cleanepi.iloc[epidx[i+1]]
        if thisrow['placement_la'] is None:
            placeholdfer = i
            # do NOTHING
        elif (thisrow['placemnt_in_la'] == 0) and (thisrow['placement_la'].strip(' ').lower() == thisrow['la_name'].strip(' ').lower()):
            cleanepi.at[epidx[i], 'placemnt_in_la'] = 3
        elif (thisrow['placemnt_in_la'] == 2) and (thisrow['placement_la'].strip(' ').lower() == thisrow['la_name'].strip(' ').lower()):
            cleanepi.at[epidx[i], 'placemnt_in_la'] = 1

        thisstart = thisrow['episode_start_date']
        thisend = thisrow['episode_end_date']
        thislsoa = thisrow['lsoa2011_placement_postcode']
        prevend = prevrow['episode_end_date']
        prevlsoa = prevrow['lsoa2011_placement_postcode']
        if (thisrow['is_placement_move'] == 0) and (thisstart == prevend):
            cleanepi.at[epidx[i+1], 'episode_end_date'] = thisend
            if thislsoa != "X" and prevlsoa == "X":
                cleanepi.at[epidx[i+1], 'lsoa2011_placement_postcode'] = thislsoa
                cleanepi.at[epidx[i+1], 'placement_area'] = thisrow['placement_area']
                cleanepi.at[epidx[i+1], 'placement_la'] = thisrow['placement_la']

# cleanepi = cleanepi[(cleanepi['is_placement_move'] == 1) | (cleanepi['pers_row_num'] == 1)]
cleanepi['time_placement'] = (pd.to_datetime(cleanepi['episode_end_date']) - pd.to_datetime(cleanepi['episode_start_date'])).dt.days

# throw out the rows where time in placement is less than 14 days
# cleanepi = cleanepi[(cleanepi['time_placement'] >= 14) | (cleanepi['pers_row_num'] == 1)]
cleanepi.reset_index(inplace=True)
cleanepi.drop(columns=['index'], inplace=True)

al.make_table(cleanepi, mlac_cepi.split('.')[0], mlac_cepi.split('.')[1])


# CAFCASS MATCH 

def check_twin(alfa, alfb):
    q = '''SELECT * FROM {0} WHERE alf_e = {1} OR alf_e = {2}'''.format(ncchd_births, alfa, alfb)
    try:
        tmp = al.call(q)
    except:
        return False
    if len(tmp) < 2:
        return False
    same_mother = (tmp['mat_alf_e'].iloc[0] == tmp['mat_alf_e'].iloc[1])
    nbirths = (tmp['tot_birth_num'].max() > 1)
    if same_mother and nbirths:
        return True
    # sometimes there's an issue where nbirths is incorrect so instead check there are diffs in birth_weight and birth_tm
    bweightdiff = (tmp['birth_weight'].iloc[0] != tmp['birth_weight'].iloc[1])
    btimediff = (tmp['birth_tm'].iloc[0] != tmp['birth_tm'].iloc[1])
    if bweightdiff and btimediff and same_mother:
        return True
    return False

## Get the LAC EPI 
q = '''SELECT MAX(cap_hearingdatetime) AS mhear FROM {0}'''.format(cafchear)
maxhear = al.call(q).iloc[0]['mhear']

q = '''
SELECT ep.unified_id, ep.local_authority_code, ep.la_name, ep.episode_start_date, 
CASE
    WHEN ep.legal_status_code = 'E1' THEN 'Placement Order'
    WHEN ep.legal_status_code = 'C2' THEN 'Care Order'
    ELSE ep.legal_status_code
END AS legal_status_code, ep.lsoa2011_placement_postcode,
main.lsoa2011_home_postcode, alf.wob, alf.gndr_cd, eduw.wob AS irn_wob
FROM {0} AS ep
LEFT JOIN {1} AS main
ON ep.unified_id = main.unified_id
AND ep.year_code = main.year_code
LEFT JOIN (
    SELECT DISTINCT alf_e, unified_id, wob, gndr_cd
    FROM 
    {2} 
) AS alf
ON ep.unified_id = alf.unified_id
LEFT JOIN {3} AS eduw
ON main.irn_e = eduw.irn_e
WHERE 
(
(ep.episode_end_date IS NOT NULL AND ep.year_code != 202021)
OR ep.year_code = 202021
)
AND ep.reason_episode_started_code IN ('B', 'L')
AND ep.episode_start_date >= '2012-01-01'
AND ep.legal_status_code IN ('C2','E1')
AND alf.alf_e IS NULL
AND ep.local_authority_code NOT IN (522, 516)
AND ep.episode_start_date < '{4}'
'''.format(mlac_epi, mlac_main, mlac_alf, eduw_alf, maxhear)
edf = al.call(q)

q = '''
SELECT DISTINCT unified_id, irn_e
FROM {0}
'''.format(mlac_main)
mdf = al.call(q)

## Sort out the CAFCASS table 
q = '''
SELECT alf.system_id_e, alf.alf_e, alf.alf_sts_cd, alf.alf_mtch_pct, alf.wob, alf.gndr_cd, alf.lsoa2011_cd,
hear.cap_hearingdatetime, hear.cap_courtidname, ho.cap_hearingoutcomefinalname, ho.cap_hearingoutcometypeidname,
app.cap_dateofcompletion, app.cap_issuedate, app.cap_receiptdate, app.cap_primaryapplicationtypename
FROM {0} AS hear
INNER JOIN {1} AS ho
ON hear.cap_hearingid_e = ho.cap_hearingid_e
AND hear.cap_caseid_e = ho.cap_caseid_e
INNER JOIN {2} AS hoa
ON hear.cap_caseid_e = hoa.cap_caseid_e
AND ho.cap_hearingoutcomeid_e = hoa.cap_hearingoutcomeid_e
INNER JOIN {3} AS app
ON hoa.cap_applicationid_e = app.cap_applicationid_e
AND hoa.cap_caseid_e = app.cap_incidentid_e
INNER JOIN (
    SELECT * 
    FROM {4}
    WHERE UPPER(record2roleidname) = 'SUBJECT'
) AS asu
ON app.cap_applicationid_e = asu.record1id_e
INNER JOIN {5} AS alf
ON asu.record2id_e = alf.system_id_e
AND app.cap_lawtypename = 'Public'
AND ho.cap_hearingoutcomefinalname = 'Yes'
LEFT JOIN {6} AS lac
ON alf.alf_e = lac.alf_e
WHERE lac.alf_e IS NULL
'''.format(cafchear, cafcho, cafchoa, cafcapp, cafcasu, cafcalf, mlac_alf)
cafdf = al.call(q)

## FInd the matches 
### Using recordlinkage on the wob

indexer = recordlinkage.Index()
### we want to match on: wob, gender
indexer.block(['wob', 'gndr_cd'])
candidate_links = indexer.index(edf, cafdf)

compare_cl = recordlinkage.Compare()
compare_cl.exact('wob', 'wob', label='wob')
compare_cl.exact('gndr_cd', 'gndr_cd', label='gndr_cd')
compare_cl.exact('episode_start_date', 'cap_hearingdatetime', label='hearing_date')
compare_cl.exact('legal_status_code', 'cap_hearingoutcometypeidname', label='hearing_outcome')
features = compare_cl.compute(candidate_links, edf, cafdf)

matches = features[features.sum(axis=1) == 4].reset_index()

#### just get the people who only have 1 match
mergematch = matches.merge(edf, left_on='level_0', right_index=True).merge(cafdf, left_on='level_1', right_index=True)
mr = mergematch.groupby(['unified_id'])['system_id_e'].count().to_frame().reset_index()
keepers = list(mr[mr['system_id_e'] == 1]['unified_id'].unique())
fmatches = mergematch[mergematch['unified_id'].isin(keepers)]

### do a second attempt using the wob from the IRN, removing those who we've already matches with both groups
edff = edf[~edf['unified_id'].isin(list(fmatches['unified_id'].unique()))].reset_index().drop(columns=['index'])
cafdff = cafdf[~cafdf['system_id_e'].isin(list(fmatches['system_id_e'].unique()))].reset_index().drop(columns=['index'])

indexer = recordlinkage.Index()
#### we want to match on: wob, gender
indexer.block(['gndr_cd'])
candidate_links = indexer.index(edff, cafdff)

compare_cl = recordlinkage.Compare()
compare_cl.exact('irn_wob', 'wob', label='wob')
compare_cl.exact('gndr_cd', 'gndr_cd', label='gndr_cd')
compare_cl.exact('episode_start_date', 'cap_hearingdatetime', label='hearing_date')
compare_cl.exact('legal_status_code', 'cap_hearingoutcometypeidname', label='hearing_outcome')
features = compare_cl.compute(candidate_links, edff, cafdff)

matches = features[features.sum(axis=1) == 4].reset_index()

#### just get the people who only have 1 match
mergematch = matches.merge(edff, left_on='level_0', right_index=True).merge(cafdff, left_on='level_1', right_index=True)
mr = mergematch.groupby(['unified_id'])['system_id_e'].count().to_frame().reset_index()
keepers = list(mr[mr['system_id_e'] == 1]['unified_id'].unique())
fmatchess = mergematch[mergematch['unified_id'].isin(keepers)]

final_match = pd.concat([fmatches, fmatchess])

## Tidy up and get ready to insert the rows into the database
final_match = final_match.reset_index().drop(columns=['index', 'level_0', 'level_1', 'wob_x', 'gndr_cd_x', 'hearing_date', 'hearing_outcome', 'la_name', 'episode_start_date', 'legal_status_code',
                                        'lsoa2011_placement_postcode', 'lsoa2011_home_postcode', 'wob_y', 'gndr_cd_y', 'irn_wob', 'lsoa2011_cd', 
                                        'cap_hearingdatetime', 'cap_courtidname', 'cap_hearingoutcomefinalname', 'cap_hearingoutcometypeidname', 'cap_dateofcompletion',
                                        'cap_issuedate', 'cap_receiptdate', 'cap_primaryapplicationtypename'])

## For the kids in the final match without an ALF, can we derive an ALF from their parent information?

q = '''
SELECT
    palf.system_id_e AS parent_system_id,
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
FROM
    {0} as rel
    INNER JOIN 
        {1} as palf
    ON 
        rel.record1id_e = palf.system_id_e
    INNER JOIN
        {1} AS chalf
    ON
        rel.record2id_e = chalf.system_id_e
WHERE
    UPPER(rel.record1roleidname) = 'PARENT'
    AND
    UPPER(rel.record2roleidname) = 'CHILD'
'''.format(cafcrel, cafcalf)
cafp = al.call(q)

missalf = final_match[final_match['alf_e'].isna()]

i = 0
for index, row in tqdm(missalf.iterrows(), total=missalf.shape[0]):
    sid = row['system_id_e']
    par = cafp[(cafp['child_system_id'] == sid) & (cafp['parent_gndr'] == 2) & (~cafp['parent_alf'].isna())]
    if len(par) == 0:
        continue
    q = '''SELECT DISTINCT nch.alf_e, nch.wob, nch.birth_weight, nch.birth_tm, nch.gndr_cd,
    CASE WHEN wds.alf_e IS NULL THEN 0 ELSE 1 END AS in_wdsd, CASE WHEN caf.alf_e IS NULL THEN 0 ELSE 1 END AS in_caf,
    CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS in_lac
    FROM {0} AS nch
    LEFT JOIN {1} AS wds
    ON nch.alf_e = wds.alf_e
    LEFT JOIN {2} AS caf
    ON nch.alf_e = caf.alf_e
    LEFT JOIN {5} AS lac
    ON nch.alf_e = lac.alf_e
    WHERE nch.mat_alf_e = {3} AND nch.wob = '{4}' AND caf.alf_e IS NULL 
    '''.format(ncchd_births, wdsdalf, cafcalf, par['parent_alf'].iloc[0], row['wob'], mlac_alf)
    try:
        child_res = al.call(q)
    except ValueError as e:
        pass
        continue
    if len(child_res[child_res['in_lac'] == 1]) > 0:
        continue
    ### get the index of the child in the final match df
    cidx = final_match[final_match['system_id_e'] == sid].index[0]
    final_match.at[cidx, 'alf_e'] = child_res['alf_e'].iloc[0]
    final_match.at[cidx, 'alf_sts_cd'] = 1

### this is how many matches with ALFs we've found
len(final_match[final_match['alf_sts_cd'] != 99])

## Actually put them in the database now 
for index, row in tqdm(final_match[final_match['alf_sts_cd'] != 99].iterrows(), total=len(final_match[final_match['alf_sts_cd'] != 99])):
    alfmtch = row['alf_mtch_pct']
    if np.isnan(row['alf_mtch_pct']) or not row['alf_mtch_pct']:
        alfmtch = 'NULL'
        
    q = '''
    UPDATE {0}
    SET alf_e = {1}, alf_mtch_pct = {2}, alf_sts_cd = {3}, alf_source = 'CAFW'
    WHERE unified_id = '{4}'
    '''.format(mlac_alf, int(row['alf_e']), alfmtch, row['alf_sts_cd'], row['unified_id'])
    
    al.call_no_return(q)

## Report 
q = '''
SELECT COUNT(DISTINCT(unified_id)) AS ncinrcs
FROM {0}
WHERE alf_source = 'CAFW'
'''.format(mlac_alf)
numf = al.call(q).iloc[0]['ncinrcs']

print('Number of ALFs found via Cafcass: {0}'.format(numf))

#gn.lac_report(mlac_alf, lac_alf)

# NCCHD MATCHING 
## Get the LAC tables
adf, mdf, edf, cedf = gn.get_refreshed_lac_tables(mlac_alf, mlac_main, mlac_epi, la_codes, mlac_cepi, True)

## read in the GP read code reference file
f = open('./helper_files/gp_read_codes.json')
read_codes = json.load(f)

child_codes = list(set(list(read_codes['adoption child'].keys()) + 
    list(read_codes['care'].keys()) + 
    list(read_codes['narrow care'].keys()) +
    list(read_codes['pre care'].keys())))

adult_codes = list(set(list(read_codes['adoption'].keys()) + 
                       list(read_codes['parent of LAC'].keys()) + 
                       list(read_codes['foster parent'].keys()) +
                       list(read_codes['adoptive parent'].keys())))

## We can only apply this to kids who:
## - enter LAC at less than 6 months old;
## - who have a home postcode;

## Make a CAFCASS parent table to reference 
###2001 lsoa codes changed to 2011 
if not gn.check_table_exists(mcafcpar):
    q = '''
        CREATE TABLE {0} AS (
        SELECT
            palf.system_id_e AS parent_system_id,
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
            CASE 
                WHEN chalf.system_id_e IN (SELECT DISTINCT system_id_e
                                            FROM {2} AS alf
                                            LEFT JOIN (
                                                SELECT DISTINCT record2id_e 
                                                FROM {3} AS asu
                                                LEFT JOIN (
                                                    SELECT DISTINCT cap_applicationid_e
                                                    FROM {4}
                                                    WHERE cap_lawtypename = 'Public'
                                                ) AS app
                                                ON asu.record1id_e = app.cap_applicationid_e
                                                WHERE record2roleidname = 'Subject'
                                                AND app.cap_applicationid_e IS NOT NULL
                                            ) AS appsu
                                            ON alf.system_id_e = appsu.record2id_e
                                            WHERE appsu.record2id_e IS NOT NULL) THEN 1
                ELSE 0
            END AS child_subj_public,
            CASE 
                WHEN palf.system_id_e IN (SELECT DISTINCT system_id_e
                                            FROM {2} AS alf
                                            LEFT JOIN (
                                                SELECT DISTINCT record2id_e 
                                                FROM {3} AS asu
                                                LEFT JOIN (
                                                    SELECT DISTINCT cap_applicationid_e
                                                    FROM {4}
                                                    WHERE cap_lawtypename = 'Public'
                                                ) AS app
                                                ON asu.record1id_e = app.cap_applicationid_e
                                                WHERE record2roleidname = 'Respondent'
                                                AND app.cap_applicationid_e IS NOT NULL
                                            ) AS appsu
                                            ON alf.system_id_e = appsu.record2id_e
                                            WHERE appsu.record2id_e IS NOT NULL) THEN 1
                ELSE 0
            END AS parent_resp_public
        FROM
            {1} as rel
            INNER JOIN 
                {2} as palf
            ON 
                rel.record1id_e = palf.system_id_e
            INNER JOIN
                {2} AS chalf
            ON
                rel.record2id_e = chalf.system_id_e
        WHERE
            UPPER(rel.record1roleidname) = 'PARENT'
            AND
            UPPER(rel.record2roleidname) = 'CHILD'
        ) WITH DATA
     '''.format(mcafcpar, cafcrel, cafcalf, cafcasu, cafcapp)
    al.call_no_return(q)

## Patch the CAFCASS parent table to fill in missing ALFs 
def ulist(ser):
    return list(ser.unique())

def patch_row(pdct, wdct, tab):
    bstr = ''
    for k, v in pdct.items():
        bstr += '{0} = {1}, '.format(k, v)
    bstr = bstr[:-2]
    
    wstr = ''
    for k, v in wdct.items():
        wstr += '{0} = {1} AND '.format(k, v)
    wstr = wstr[:-4]
    
    q = '''
    UPDATE {0}
    SET {1}
    WHERE {2}
    '''.format(tab, bstr, wstr)
    al.call_no_return(q)

def check_multi(mat_alf, wob):
    q = '''
    SELECT alf_e, child_id_e, wob, CASE WHEN gndr_cd = 'M' THEN 1 ELSE 2 END AS gndr_cd,
    birth_weight, birth_tm, birth_order 
    FROM {0} 
    WHERE mat_alf_e = {1}
    AND wob = '{2}'
    '''.format(ncchd_births, mat_alf, wob)
    return al.call(q)

def check_any(child_alf, wob):
    q = '''
    SELECT DISTINCT parent_alf
    FROM {0}
    WHERE child_alf = {1}
    AND parent_gndr = 2
    '''.format(mcafcpar, child_alf)
    mat_alf = al.call(q)['parent_alf'].iloc[0]
    
    if not mat_alf:
        return True
    
    q = '''
    SELECT COUNT(DISTINCT(alf_e)) AS num
    FROM {0}
    WHERE mat_alf_e = {1}
    AND wob = '{2}'
    '''.format(ncchd_births, mat_alf, wob)
    res = al.call(q)['num'].iloc[0]
    
    if res == 0:
        return True
    return False

q = '''
SELECT DISTINCT cafc.*, ncch.*,
CASE WHEN twins.ch1_alf IS NOT NULL THEN 1 ELSE 0 END AS twin,
CASE WHEN cchild.child_alf IS NOT NULL THEN 1 ELSE 0 END AS alf_already_caf,
CASE WHEN dcheck.child_alf IS NOT NULL THEN 1 ELSE 0 END AS alf_caf_same_mother
FROM (
	SELECT parent_system_id, PARENT_ALF, parent_wob, child_system_id, child_wob, child_gndr 
	FROM {0}
	WHERE CHILD_ALF IS NULL 
	AND PARENT_ALF IS NOT NULL 
	AND PARENT_GNDR = 2
) AS cafc
LEFT JOIN (
	SELECT alf_e, wob, birth_weight, birth_tm, 
	CASE WHEN gndr_cd = 'M' THEN 1
	WHEN gndr_cd = 'F' THEN 2
	ELSE 9 END AS gndr_cd, mat_alf_e, lsoa_cd_birth
	FROM {1}
) AS ncch
ON cafc.parent_alf = ncch.MAT_alf_e 
AND cafc.child_wob = ncch.wob
AND cafc.child_gndr = ncch.gndr_cd
LEFT JOIN (
    SELECT lf.alf_e AS ch1_alf, lf.CHILD_ID_e AS ch1_id, lf.wob, lf.BIRTH_WEIGHT AS ch1_bw, lf.birth_tm AS ch1_bt, lf.gndr_cd AS ch1_gndr, 
    rt.alf_e AS ch2_alf, rt.CHILD_ID_e AS ch2_id, rt.BIRTH_WEIGHT AS ch2_bw, rt.BIRTH_TM AS ch2_bt, rt.GNDR_CD AS ch2_gndr
    FROM {1} AS lf
    INNER JOIN {1} AS rt
    ON lf.MAT_alf_e = rt.MAT_alf_e 
    AND lf.wob = rt.wob
    AND lf.BIRTH_ORDER != rt.BIRTH_ORDER 
    AND lf.BIRTH_TM != rt.BIRTH_TM 
) AS twins
ON ncch.alf_e = twins.ch1_alf
OR ncch.alf_e = twins.ch2_alf
LEFT JOIN {0} AS cchild
ON ncch.alf_e = cchild.child_alf
LEFT JOIN {0} AS dcheck
ON ncch.alf_e = dcheck.child_alf
AND ncch.mat_alf_e = dcheck.parent_alf
'''.format(mcafcpar, ncchd_births)
cafp = al.call(q)

all_alfs = ulist(cafp[cafp['twin'] == 0]['alf_e'])

for alf in tqdm(all_alfs):    
    pdct = {}
    wdct = {}
    row = cafp[cafp['alf_e'] == alf]
    if np.isnan(alf) or len(row) > 1:
        continue
    
    if max(row['alf_already_caf']) == 0:
        row = row.iloc[0]

    elif max(row['alf_already_caf']) == 1 and max(row['alf_caf_same_mother']) == 1:
        row = row.iloc[0]

    elif max(row['alf_already_caf']) == 1 and max(row['alf_caf_same_mother']) == 0:
        chk = check_any(alf, row['wob'].iloc[0])
        if check_any:
            row = row.iloc[0]
        else:
            continue
    
    pdct['child_alf'] = int(row['alf_e'])
    wdct['child_system_id'] = int(row['child_system_id'])
    patch_row(pdct, wdct, mcafcpar)

## Match using NCCHD births
entry_under6m = list(edf[edf['age_start'] <= 0.6]['unified_id'].unique())
have_homepc = list(mdf[~mdf['lsoa2011_home_postcode'].isna()]['unified_id'].unique())

eligible_children = list(mdf[(mdf['unified_id'].isin(entry_under6m)) &
                           (mdf['unified_id'].isin(have_homepc))]['unified_id'].unique())

def get_base(pers):
    gndr_map = { 1:'M', 2:'F' }
    
    tep = edf[edf['unified_id'] == pers]
    tmn = mdf[mdf['unified_id'] == pers]
    wob = tep['wob'].iloc[0]
    gndr_num = int(tmn['gndr_cd'].iloc[0])
    gndr = gndr_map[gndr_num]
    irn = tmn['irn_e'].iloc[0]
    fhp = tmn['lsoa2011_home_postcode'].iloc[0]
    plocs = [x for x in ulist(tep['lsoa2011_placement_postcode']) if x is not None]
    
    return tep, tmn, wob, gndr, gndr_num, irn, fhp, plocs

def query_ncchd(wob, gndr, fhp, gndr_n, plocs):
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
    '''.format(ncchd_births, mlac_alf, mcafcpar, wob, gndr, fhp, gndr_n, wdsdralf, 
              pquery, gpncare, gpadopt, gpparent, pcase)
    return al.call(q)

def add_alf(nalf, alf_mtch, alf_sts, uid):
    alfmtch = alf_mtch
    if not alf_mtch:
        alfmtch = 'NULL'
    
    q = '''
    UPDATE {0}
    SET alf_e = {1}, alf_mtch_pct = {2}, alf_sts_cd = {3}, alf_source = 'NCCHD_BIRTHS'
    WHERE unified_id = '{4}'
    '''.format(mlac_alf, int(nalf), alfmtch, alf_sts, uid)
    
    al.call_no_return(q)

for pers in tqdm(eligible_children):
    tep, tmn, wob, gndr, gndr_n, irn, fhp, plocs = get_base(pers)

    if fhp is None or fhp[0] != 'W' or ulist(tep['legal_status_code']) == ['V2']:
        continue
    try:
        tnch = query_ncchd(wob, gndr, fhp, gndr_n, plocs)
    except ValueError:
        pass
        continue
    tnch['sum'] = tnch[['mat_alf_in_caf', 'child_alf_in_caf', 'caf_wg_match_from_matalf', 'lsoa_nnull', 'wdsd_fhp_match', 'wdsd_ppc_match',
                       'child_care_ev', 'child_adopt_ev', 'parent_lac_ev', 'child_subj_public', 'parent_resp_public']].sum(axis=1)
    
    if tnch['alf_e'].nunique() == 1:
        add_alf(tnch.iloc[0]['alf_e'], None, 1, pers)
        continue
    
    if tnch['alf_e'].nunique() > 1 and tnch['sum'].max() > 2:
        # pick the row with the best match
        pick = tnch[tnch['sum'] == tnch['sum'].max()]
        if pick['alf_e'].nunique() == 1:
            add_alf(pick.iloc[0]['alf_e'], None, 1, pers)
            continue
        
        # check the row with the best match when there are two equal rows         
        if pick['alf_e'].nunique() == 2 and pick['mat_alf_e'].nunique() == 1:
            # basically happens when there are twins, so just pick one of them
            add_alf(pick.iloc[0]['alf_e'], None, 1, pers)
            continue
        
        # if there are multiple best matches we prefer the row where the birth lsoa matches the LAC census
        pick = pick[pick['lsoa_nnull'] == 1]
        if pick['alf_e'].nunique() == 1:
            add_alf(pick.iloc[0]['alf_e'], None, 1, pers)
            continue
        
        # if there are still multiple best matches, go for the best match between the two
        pick['sum_2'] = pick[['child_alf_in_caf', 'lsoa_nnull', 'wdsd_fhp_match', 'wdsd_ppc_match', 
                      'child_care_ev', 'child_adopt_ev', 'parent_lac_ev']].sum(axis=1)
        npick = pick[pick['sum_2'] == pick['sum_2'].max()]
        if npick['alf_e'].nunique() == 1:
            add_alf(npick.iloc[0]['alf_e'], None, 1, pers)
            continue

## Match from parent event via NCCHD
def emerg_check(wob, gender, lsoa, start, end, mpw):
    wstr = '''alf.wob = '{0}' AND alf.gndr_cd = {1}'''.format(wob, gender)
    if str(wob) == 'NaT':
        wstr = '''alf.wob <= '{0}' '''.format(mpw)
    q = '''
    SELECT COUNT(DISTINCT(wdsd.alf_e)) AS num
    FROM {0} AS wdsd
    LEFT JOIN {1} AS alf
    ON wdsd.alf_e = alf.alf_e
    WHERE {2}
    AND wdsd.lsoa2011_cd = '{4}'
    AND (wdsd.start_date, wdsd.end_date) OVERLAPS (DATE('{5}'), DATE('{6}'))
    '''.format(wdsdralf, wdsdalf, wstr, gender, lsoa, start, end)
    res = al.call(q)['num'].iloc[0]
    if res == 0:
        return False
    return True

def add_alf(nalf, alf_mtch, alf_sts, uid):
    alfmtch = alf_mtch
    if not alf_mtch:
        alfmtch = 'NULL'
    
    q = '''
    UPDATE {0}
    SET alf_e = {1}, alf_mtch_pct = {2}, alf_sts_cd = {3}, alf_source = 'NCCHD_PARENT_GP_EVENT'
    WHERE unified_id = '{4}'
    '''.format(mlac_alf, int(nalf), alfmtch, alf_sts, uid)
    
    al.call_no_return(q)
    
def person_ev_type(alf, evtype):
    rcs = read_codes[evtype]
    
    q = '''SELECT DISTINCT gp.alf_e, gp.lsoa_cd, la.lad20nm, gp.event_cd, gp.event_dt, dc.pref_term_30 FROM {0} AS gp
    LEFT JOIN {1} AS dc
    ON gp.event_cd = dc.READ_CODE
    LEFT JOIN {2} AS la
    ON gp.lsoa_cd = la.lsoa11cd
    WHERE gp.alf_e = {3}
    AND gp.event_cd IN ({4})
    '''.format(gpevent, readcode, lsoa_decode, alf, ','.join(["'"+x+"'" for x in rcs.keys()]))
    return al.call(q)

def ulist(ser):
    return list(ser.unique())

def get_base(alf):
    rows = ncchd_conf[ncchd_conf['alf_e'] == alf]
    wob = rows['wob'].iloc[0]
    gndr = rows['gndr_cd'].iloc[0]
    
    mat_alf = ulist(rows['mat_alf_e'])
    # get the LAs for the mother -- one of these will be the looking after LA 
    mat_locs = all_plocs[(all_plocs['alf_e'].isin(mat_alf)) & (all_plocs['end_date'] >= wob)]
    # get all LSOAs that the child has lived in     
    chi_locs = all_clocs[all_clocs['alf_e'] == int(alf)]
    clocs = ulist(chi_locs['lsoa2011_cd'])
    
    local_auths = ulist(mat_locs['la_id'])
    local_auths += ulist(rows['la_id'])
    local_auths += ulist(chi_locs['la_id'])
    local_auths = list(set([int(x) for x in local_auths if str(x) != 'nan']))
    
    event_dates = ulist(rows['event_dt'])
    
    ex_mlocs = person_ev_type(mat_alf[0], 'parent of LAC')
    try:
        ex_clocs = person_ev_type(alf, 'care')
    except ValueError as e:
        pass
        ex_clocs = pd.DataFrame(data=[], columns=ex_mlocs.columns)
    
    return wob, gndr, mat_locs, local_auths, event_dates, clocs, rows, mat_alf, ex_mlocs, ex_clocs

parentoflac = read_codes['parent of LAC'].keys()

q = '''
SELECT DISTINCT ncchd.alf_e, ncchd.wob, birth_weight, birth_tm, 
CASE WHEN ncchd.gndr_cd = 'M' THEN 1 WHEN ncchd.gndr_cd = 'F' THEN 2 ELSE 9 END AS gndr_cd, CASE WHEN lac.alf_e IS NOT NULL THEN 1 ELSE 0 END AS alf_in_lac,
mat_alf_e, fromgp.event_dt, rf.start_date, rf.end_date, rf.ralf_e, rf.lsoa2011_cd, ls.lad20nm, lacd.la_id, 
CASE WHEN childgp.alf_e IS NOT NULL THEN 1 ELSE 0 END AS child_ncare_ev,
CASE WHEN childadop.alf_e IS NOT NULL THEN 1 ELSE 0 END AS child_adopt_ev,
CASE WHEN childanycare.alf_e IS NOT NULL THEN 1 ELSE 0 END AS child_anycare_ev,
CASE WHEN paradopt.alf_e IS NOT NULL THEN 1 ELSE 0 END AS parent_adoptev
FROM {0} AS ncchd
INNER JOIN (
    SELECT DISTINCT alf_e, event_dt
    FROM {1}
    WHERE gndr_cd = 2
    AND event_cd IN ({2})
) AS fromgp
ON ncchd.mat_alf_e = fromgp.alf_e
LEFT JOIN (
    SELECT DISTINCT alf_e
    FROM {1}
    WHERE event_cd = '13I81'
) AS paradopt
ON ncchd.mat_alf_e = paradopt.alf_e
LEFT JOIN {3} AS lac
ON ncchd.alf_e = lac.alf_e
LEFT JOIN {4} AS rf
ON fromgp.alf_e = rf.alf_e
LEFT JOIN {5} AS ls
ON rf.lsoa2011_cd = ls.lsoa11cd
LEFT JOIN {6} AS lacd
ON ls.lad20nm = lacd.la_name
LEFT JOIN {7} AS childgp
ON ncchd.alf_e = childgp.alf_e 
LEFT JOIN {8} AS childadop
ON ncchd.alf_e = childadop.alf_e 
LEFT JOIN {9} AS childanycare
ON ncchd.alf_e = childanycare.alf_e 
WHERE lac.alf_e IS NULL
AND (start_date, end_date) OVERLAPS (fromgp.event_dt, fromgp.event_dt)
AND ncchd.alf_e IS NOT NULL
'''.format(ncchd_births, gpevent, ','.join(["'"+x+"'" for x in parentoflac]), mlac_alf, wdsdralf, lsoa_decode, la_codes, gpncare, gpadopt, gpcare)
ncchd_par = al.call(q)

q = '''
SELECT ralf.alf_e, ralf.start_date, ralf.end_date, ralf.ralf_e, ralf.lsoa2011_cd, fromgp.event_dt, lsoa.lad20nm, laid.la_id
FROM {0} AS ralf
INNER JOIN (
    SELECT DISTINCT alf_e, event_dt
        FROM {1}
        WHERE gndr_cd = 2
        AND event_cd IN ({2})
) AS fromgp
ON ralf.alf_e = fromgp.alf_e
LEFT JOIN {3} AS lsoa
ON ralf.lsoa2011_cd = lsoa.lsoa11cd
LEFT JOIN {4} AS laid
ON lsoa.lad20nm = laid.la_name
'''.format(wdsdralf, gpevent, ','.join(["'"+x+"'" for x in parentoflac]), lsoa_decode, la_codes)
all_plocs = al.call(q)

q = '''
SELECT DISTINCT ralf.alf_e, ralf.start_date, ralf.end_date, ralf.ralf_e, ralf.lsoa2011_cd, lsoa.lad20nm, laid.la_id, 
CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac
FROM {0} AS ralf
LEFT JOIN {1} AS lsoa
ON ralf.lsoa2011_cd = lsoa.lsoa11cd
LEFT JOIN {2} AS laid
ON lsoa.lad20nm = laid.la_name
LEFT JOIN {6} AS lac
ON ralf.alf_e = lac.alf_e
WHERE ralf.alf_e IN ({3})

UNION

SELECT DISTINCT reg.alf_e, reg.start_date, reg.end_date, NULL AS ralf_e, evnt.lsoa_cd AS lsoa2011_cd, lsoa.lad20nm, laid.la_id,
CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac
FROM {4} AS reg
INNER JOIN {5} AS evnt
ON reg.alf_e = evnt.alf_e 
AND reg.prac_cd_e = evnt.prac_cd_e
AND (reg.start_date, reg.end_date) OVERLAPS (evnt.event_dt, evnt.event_dt)
LEFT JOIN {1} AS lsoa
ON evnt.lsoa_cd = lsoa.lsoa11cd
LEFT JOIN {2} AS laid
ON lsoa.lad20nm = laid.la_name
LEFT JOIN {6} AS lac
ON reg.alf_e = lac.alf_e
WHERE reg.alf_e IN ({3})

UNION 

SELECT DISTINCT ev.alf_e, NULL as start_date, NULL AS end_date, NULL AS ralf_e, ev.lsoa_cd AS lsoa2011_cd, lsoa.lad20nm, laid.la_id,
CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac
FROM {5} AS ev
LEFT JOIN {1} AS lsoa
ON ev.lsoa_cd = lsoa.lsoa11cd
LEFT JOIN {2} AS laid
ON lsoa.lad20nm = laid.la_name
LEFT JOIN {6} AS lac
ON ev.alf_e = lac.alf_e
WHERE ev.alf_e IN ({3})
'''.format(wdsdralf, lsoa_decode, la_codes, 
           ','.join([str(int(x)) for x in ncchd_par[~ncchd_par['alf_e'].isna()]['alf_e'].to_list()]), gpreg, gpevent, mlac_alf)
all_clocs = al.call(q)

confirmed = []
unconfirmed = []
# Before we start, check that the child is down as living in different places to the parent
for index, row in tqdm(ncchd_par.iterrows(), total=ncchd_par.shape[0]):
    if row['child_ncare_ev'] == 1 or row['child_adopt_ev'] == 1:
        confirmed.append(row)
        continue
        
    child_locs = all_clocs[(all_clocs['alf_e'] == row['alf_e']) & (~all_clocs['start_date'].isna())].sort_values('start_date')
    parent_locs = all_plocs[(all_plocs['alf_e'] == row['mat_alf_e']) & (all_plocs['end_date'] >= row['wob']) 
                    & (~all_plocs['start_date'].isna())].sort_values('start_date')
    tmp = pd.merge(child_locs, parent_locs, how='inner', on=['ralf_e'], indicator=True)
    if len(tmp) == 0:
        confirmed.append(row)
        continue
    res = tmp[tmp.apply(lambda x: DateTimeRange(x['start_date_x'], x['end_date_x']).is_intersection(DateTimeRange(x['start_date_y'], x['end_date_y'])), axis=1)]
    if len(res) >= len(child_locs):
        unconfirmed.append(row)
        continue
    else:
        confirmed.append(row)
        continue

ncchd_conf = pd.DataFrame(confirmed)
ncchd_uconf = pd.DataFrame(unconfirmed)

adopted_ids = ulist(edf[edf['reason_episode_finished_code'].str[0:2] == 'E1']['unified_id'])

alfs_to_apply = list(ncchd_conf['alf_e'].unique())

for alf in tqdm(alfs_to_apply):
    wob, gndr, mat_locs, local_auths, event_dates, clocs, rows, mat_alf, ex_mlocs, ex_clocs = get_base(alf)
    
    if max(event_dates) < datetime.date(2002,1,1):
        continue
        
    # get people in LAC who match on wob, gender, and local auth     
    matchw = ulist(adf[(adf['wob'] == wob) & 
                       (adf['gndr_cd'] == gndr) & 
                       (adf['local_authority_code'].isin(local_auths))]['unified_id'])
 
    # and also get people in LAC who are missing WOBs     
    matchn = list(adf[(adf['wob'].isna()) & 
                      (adf['first_ep_start'] >= row['wob'] - datetime.timedelta(days=90)) & 
                      (adf['local_authority_code'].isin(local_auths))]['unified_id'].unique())
    
    if rows['child_adopt_ev'].max() == 1 or rows['parent_adoptev'].max() == 1:
        matchw = ulist(adf[(adf['wob'] == wob) & 
                       (adf['gndr_cd'] == gndr) & 
                       (adf['local_authority_code'].isin(local_auths)) &
                       (adf['unified_id'].isin(adopted_ids))]['unified_id']) 
        
        matchn = list(adf[(adf['wob'].isna()) & 
                      (adf['first_ep_start'] >= row['wob'] - datetime.timedelta(days=90)) & 
                      (adf['local_authority_code'].isin(local_auths)) &
                      (adf['unified_id'].isin(adopted_ids))]['unified_id'].unique())

    matchlist = matchw + matchn
    matchls = mdf[(mdf['unified_id'].isin(matchlist))]
    mother_lsoas = list(set(mat_locs['lsoa2011_cd'].to_list()))
    mother_lsoas += ulist(ex_mlocs['lsoa_cd'])
    matchlsp = matchls[matchls['lsoa2011_home_postcode'].isin(mother_lsoas)]

    if matchlsp['unified_id'].nunique() == 1:
        add_alf(alf, None, 1, ulist(matchlsp['unified_id'])[0])
        continue

    if matchlsp['unified_id'].nunique() > 1:
        epls = edf[edf['unified_id'].isin(ulist(matchlsp['unified_id']))]
    else:
        epls = edf[edf['unified_id'].isin(matchlist)]
    
    child_lsoas = clocs + ulist(ex_clocs['lsoa_cd'])
    eplm = epls[epls['lsoa2011_placement_postcode'].isin(child_lsoas)]

    if eplm['unified_id'].nunique() == 1:
        add_alf(alf, None, 1, ulist(eplm['unified_id'])[0])
        continue
     
    if rows['child_adopt_ev'].max() == 1 or rows['parent_adoptev'].max() == 1:
        # last chance if the child is adopted, maybe the WOB is wrong
        eplam = edf[(edf['unified_id'].isin(adopted_ids)) &
                (edf['lsoa2011_placement_postcode'].isin(clocs)) &
                (edf['age_start'] < 0) & 
                (edf['episode_start_date'] >= wob) &
                (edf['gndr_cd'] == gndr)]

        if eplam['unified_id'].nunique() == 1:
            add_alf(alf, None, 1, ulist(eplam['unified_id'])[0])
            continue
            
    wobtest = adf[(adf['wob'] == wob) &  (adf['gndr_cd'] == gndr) & (adf['local_authority_code'].isin(local_auths))]
    if rows['child_adopt_ev'].max() == 1 or rows['parent_adoptev'].max() == 1:
        wobtest = wobtest[wobtest['unified_id'].isin(adopted_ids)]
    
    # are there ANY kids in this LA with this gender and wob?     
    if len(wobtest) == 0:
        # if not, wob is probably wrong 
        if rows['child_adopt_ev'].max() == 1:
            adoptdt = min(ulist(person_ev_type(alf, 'adoption child')['event_dt']))
            try:
                incaredt = min(ulist(person_ev_type(alf, 'narrow care')['event_dt']))
            except ValueError as e:
                pass
                incaredt = adoptdt
            matchl = ulist(adf[(adf['local_authority_code'].isin(local_auths)) & 
                                (adf['first_ep_start'] <= incaredt) &
                                (adf['first_ep_start'] >= wob) &
                                (adf['gndr_cd'] == gndr) &
                                (adf['unified_id'].isin(adopted_ids))]['unified_id'])
            hpm = set(ulist(mdf[(mdf['unified_id'].isin(matchl)) & (mdf['lsoa2011_home_postcode'].isin(mother_lsoas))]['unified_id']))
            epm = set(ulist(edf[(edf['unified_id'].isin(matchl)) & (edf['lsoa2011_placement_postcode'].isin(child_lsoas))]['unified_id']))
            pmatch = hpm.intersection(epm)
            if len(pmatch) == 1:
                add_alf(alf, None, 1, list(pmatch)[0])
                continue
            
        if rows['child_ncare_ev'].max() == 1:
            in_care_dt = ulist(person_ev_type(alf, 'narrow care')['event_dt'])
            in_care_mindt = min(in_care_dt)
            matchl = ulist(adf[(adf['local_authority_code'].isin(local_auths)) & 
                (adf['first_ep_start'] <= in_care_mindt) &
                (adf['first_ep_start'] >= wob) &
                (adf['gndr_cd'] == gndr)]['unified_id'])
            hpm = set(ulist(mdf[(mdf['unified_id'].isin(matchl)) & (mdf['lsoa2011_home_postcode'].isin(mother_lsoas))]['unified_id']))
            epm = set(ulist(edf[(edf['unified_id'].isin(matchl)) & (edf['lsoa2011_placement_postcode'].isin(child_lsoas))]['unified_id']))
            pmatch = hpm.intersection(epm)
            if len(pmatch) == 1:
                add_alf(alf, None, 1, list(pmatch)[0])
                continue
        
        if rows['parent_adoptev'].max() == 1 and rows['child_adopt_ev'].max() == 0:
            adopdf = person_ev_type(mat_alf[0], 'parent of LAC')
            adoptdt = adopdf[adopdf['event_cd'] == '13I81']['event_dt'].min()
            matchl = ulist(adf[(adf['local_authority_code'].isin(local_auths)) & 
                                (adf['first_ep_start'] <= adoptdt) &
                                (adf['first_ep_start'] >= wob) &
                                (adf['gndr_cd'] == gndr) &
                                (adf['unified_id'].isin(adopted_ids))]['unified_id'])
            hpm = set(ulist(mdf[(mdf['unified_id'].isin(matchl)) & (mdf['lsoa2011_home_postcode'].isin(mother_lsoas))]['unified_id']))
            epm = set(ulist(edf[(edf['unified_id'].isin(matchl)) & (edf['lsoa2011_placement_postcode'].isin(child_lsoas))]['unified_id']))
            pmatch = hpm.intersection(epm)
            if len(pmatch) == 1:
                add_alf(alf, None, 1, list(pmatch)[0])
                continue

## Report 
q = '''
SELECT COUNT(DISTINCT(unified_id)) AS ncinrcs
FROM {0}
WHERE alf_source = 'NCCHD_BIRTHS' OR alf_source = 'NCCHD_PARENT_GP_EVENT'
'''.format(mlac_alf)
numf = al.call(q).iloc[0]['ncinrcs']

print('Number of ALFs found via NCCHD: {0}'.format(numf))

#gn.lac_report(mlac_alf, lac_alf)


# GP events

## Get the LAC tables
adf, mdf, edf, cedf = gn.get_refreshed_lac_tables(mlac_alf, mlac_main, mlac_epi, la_codes, mlac_cepi, True)

## read in the GP read code reference file
f = open('./helper_files/gp_read_codes.json')
read_codes = json.load(f)

child_codes = list(set(list(read_codes['adoption child'].keys()) + 
    list(read_codes['care'].keys()) + 
    list(read_codes['narrow care'].keys()) +
    list(read_codes['pre care'].keys())))

adult_codes = list(set(list(read_codes['adoption'].keys()) + 
                       list(read_codes['parent of LAC'].keys()) + 
                       list(read_codes['foster parent'].keys()) +
                       list(read_codes['adoptive parent'].keys())))

## Pull into memory 
### Grab the datasets into the memory we have 
lacmaxwob = adf[~adf['wob'].isna()]['wob'].max()
lacminwob = adf[~adf['wob'].isna()]['wob'].min()

q = '''
SELECT gp.alf_e, MAX(gp.end_date) AS mend
FROM {0} AS gp
LEFT JOIN {1} AS alf
ON gp.alf_e = alf.alf_e
WHERE alf.wob >= '{2}' AND alf.wob <= '{3}'
GROUP BY gp.alf_e
'''.format(gpreg, gpalf, lacminwob, lacmaxwob)

gpreg_pers = al.call(q)

q = '''SELECT DISTINCT gp.*, CASE WHEN lac.alf_e IS NOT NULL THEN 1 ELSE 0 END AS alf_in_lac
FROM (
    SELECT DISTINCT reg.alf_e, alf.wob, alf.gndr_cd, 
    FIRST_VALUE(start_date) OVER (PARTITION BY reg.alf_e ORDER BY reg.start_date) AS first_start,
    FIRST_VALUE(end_date) OVER (PARTITION BY reg.alf_e ORDER BY reg.end_date DESC) AS final_end
    FROM {0} AS reg
    INNER JOIN (
        SELECT DISTINCT alf_e, wob, gndr_cd
        FROM {1}
        WHERE wob >= '{2}' AND wob <= '{3}'
    ) AS alf
    ON reg.alf_e = alf.alf_e
) AS gp
LEFT JOIN {4} AS lac
ON gp.alf_e = lac.alf_e '''.format(gpreg, gpalf, lacminwob, lacmaxwob, mlac_alf)

gpreg_minmax = al.call(q)

q = '''
SELECT gp.alf_e, alf.wob, alf.gndr_cd, gp.start_date, gp.end_date, alf.lsoa_cd, CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac,
CASE WHEN ncare.alf_e IS NULL THEN 0 ELSE 1 END AS narrow_care_ev,
CASE WHEN pcare.alf_e IS NULL THEN 0 ELSE 1 END AS precare_ev,
CASE WHEN adopt.alf_e IS NULL THEN 0 ELSE 1 END AS adopt_ev,
CASE WHEN gpparent.alf_e IS NULL THEN 0 ELSE 1 END AS parent_ev
FROM {0} AS gp
LEFT JOIN {1} AS alf
ON gp.alf_e = alf.alf_e
LEFT JOIN (
    SELECT DISTINCT alf_e 
    FROM {2} 
) AS lac
ON alf.alf_e = lac.alf_e
LEFT JOIN {5} AS ncare
ON alf.alf_e = ncare.alf_e
LEFT JOIN {6} AS pcare
ON alf.alf_e = pcare.alf_e
LEFT JOIN {7} AS adopt
ON alf.alf_e = adopt.alf_e
LEFT JOIN (
    SELECT DISTINCT ncchd_births.alf_e, ncchd_births.mat_alf_e 
    FROM {8} AS ncchd_births
    INNER JOIN {9} AS pgpev
    ON ncchd_births.mat_alf_e = pgpev.alf_e
) AS gpparent
ON alf.alf_e = gpparent.alf_e
WHERE alf.wob >= '{3}'
AND alf.wob <= '{4}'
ORDER BY start_date
'''.format(gpreg, gpalf, mlac_alf, lacminwob, lacmaxwob, gpncare, gppcare, gpadopt, ncchd_births, gpparent)
gprs = al.call(q)

gprs['sum'] = gprs[['narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev']].sum(axis=1)

q = '''
SELECT wdsdralf.alf_e, alf.wob, alf.gndr_cd, wdsdralf.start_date, wdsdralf.end_date, wdsdralf.ralf_e, wdsdralf.lsoa2011_cd, 
CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac,
CASE WHEN ncare.alf_e IS NULL THEN 0 ELSE 1 END AS narrow_care_ev,
CASE WHEN pcare.alf_e IS NULL THEN 0 ELSE 1 END AS precare_ev,
CASE WHEN adopt.alf_e IS NULL THEN 0 ELSE 1 END AS adopt_ev, 
CASE WHEN gpparent.alf_e IS NULL THEN 0 ELSE 1 END AS parent_ev
FROM {0} AS wdsdralf
LEFT JOIN {1} AS alf
ON wdsdralf.alf_e = alf.alf_e
LEFT JOIN (
    SELECT DISTINCT alf_e 
    FROM {2} 
) AS lac
ON wdsdralf.alf_e = lac.alf_e
LEFT JOIN {5} AS ncare
ON alf.alf_e = ncare.alf_e
LEFT JOIN {6} AS pcare
ON alf.alf_e = pcare.alf_e
LEFT JOIN {7} AS adopt
ON alf.alf_e = adopt.alf_e
LEFT JOIN (
    SELECT DISTINCT ncchd_births.alf_e, ncchd_births.mat_alf_e 
    FROM {8} AS ncchd_births
    INNER JOIN {9} AS pgpev
    ON ncchd_births.mat_alf_e = pgpev.alf_e
) AS gpparent
ON alf.alf_e = gpparent.alf_e
WHERE alf.wob >= '{3}'
AND alf.wob <= '{4}'
ORDER BY start_date
'''.format(wdsdralf, wdsdalf, mlac_alf, lacminwob, lacmaxwob, gpncare, gppcare, gpadopt, ncchd_births, gpparent)
wdsdrs = al.call(q)

wdsdrs['sum'] = wdsdrs[['narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev']].sum(axis=1)

q = '''SELECT DISTINCT gp.alf_e, gp.wob, gp.gndr_cd, gp.lsoa_cd, la.lad20nm, gp.event_cd, 
CASE WHEN gp.event_dt = '0001-01-01' THEN '2099-01-01' ELSE gp.event_dt END AS event_dt, dc.pref_term_30,
CASE WHEN lac.alf_e IS NOT NULL THEN 1
ELSE 0
END AS alf_in_lac
FROM {0} AS gp
LEFT JOIN {1} AS dc
ON gp.event_cd = dc.READ_CODE
LEFT JOIN {2} AS la
ON gp.lsoa_cd = la.lsoa11cd
LEFT JOIN {3} AS lac
ON gp.alf_e = lac.alf_e
WHERE gp.wob >= '{4}'
AND gp.wob <= '{5}'
AND gp.event_cd IN ({6})
AND gp.alf_e IS NOT NULL
'''.format(gpevent, readcode, lsoa_decode, mlac_alf, lacminwob, lacmaxwob, ','.join(["'"+x+"'" for x in child_codes]))

gpev_child = al.call(q)
gpev_child['event_dt'] = gn.dttime_fix(gpev_child['event_dt'])
gpev_child['wob'] = gn.dt_type_fix(gpev_child['wob'])

q = '''SELECT DISTINCT gp.alf_e, gp.wob, gp.gndr_cd, gp.lsoa_cd, la.lad20nm, gp.event_cd, 
CASE WHEN gp.event_dt = '0001-01-01' THEN '2099-01-01' ELSE gp.event_dt END AS event_dt, dc.pref_term_30
FROM {0} AS gp
LEFT JOIN {1} AS dc
ON gp.event_cd = dc.READ_CODE
LEFT JOIN {2} AS la
ON gp.lsoa_cd = la.lsoa11cd
WHERE gp.event_cd IN ({3})
AND gp.alf_e IS NOT NULL
'''.format(gpevent, readcode, lsoa_decode, ','.join(["'"+x+"'" for x in adult_codes]))

gpev_adult = al.call(q)
gpev_adult['event_dt'] = gn.dttime_fix(gpev_adult['event_dt'])
gpev_adult['wob'] = gn.dt_type_fix(gpev_adult['wob'])

## Matching 
def person_ev_all(alf):
    q = '''SELECT DISTINCT gp.alf_e, gp.lsoa_cd, la.lad20nm, gp.event_cd, gp.event_dt, dc.pref_term_30 FROM {0} AS gp
    LEFT JOIN {1} AS dc
    ON gp.event_cd = dc.READ_CODE
    LEFT JOIN {2} AS la
    ON gp.lsoa_cd = la.lsoa11cd
    WHERE gp.alf_e = {3}
    ORDER BY event_dt
    '''.format(gpevent, readcode, lsoa_decode, alf)
    return al.call(q)

def person_ev_type(alf, evtype):
    rcs = read_codes[evtype]
    
    q = '''SELECT DISTINCT gp.alf_e, gp.lsoa_cd, la.lad20nm, gp.event_cd, gp.event_dt, dc.pref_term_30 FROM {0} AS gp
    LEFT JOIN {1} AS dc
    ON gp.event_cd = dc.READ_CODE
    LEFT JOIN {2} AS la
    ON gp.lsoa_cd = la.lsoa11cd
    WHERE gp.alf_e = {3}
    AND gp.event_cd IN ({4})
    '''.format(gpevent, readcode, lsoa_decode, alf, ','.join(["'"+x+"'" for x in rcs.keys()]))
    return al.call(q)

def alf_against_lsoas(alfs, lsoas):
    q = '''
    SELECT * 
    FROM {0} AS ls
    LEFT JOIN {1} AS alf
    ON ls.pers_id_e = alf.pers_id_e
    WHERE alf.alf_e IN ({2})
    AND ls.lsoa2001_cd IN ({3})
    '''.format(wdsdlsoa, wdsdalf, ','.join([str(x) for x in alfs]), ','.join(["'"+x+"'" for x in lsoas]))
    
    return al.call(q)

def wide_ev_type(wob, gender, evtype):
    rcs = list(read_codes[evtype].keys())
    
    gdf = gpev_child[(gpev_child['wob'] == wob) & (gpev_child['gndr_cd'] == str(gender)) &
                     (gpev_child['event_cd'].isin(rcs))]
    
    return gdf

def person_ev_type(alf, evtype):
    rcs = list(read_codes[evtype].keys())
    
    gdf = gpev_child[(gpev_child['alf_e'] == alf) &
                     (gpev_child['event_cd'].isin(rcs))]
    
    return gdf


def people_ev_type(alfs, evtype):
    rcs = list(read_codes[evtype].keys())
    
    gdf = gpev_child[(gpev_child['alf_e'].isin(alfs)) &
                     (gpev_child['event_cd'].isin(rcs))]
    
    return gdf


def get_datebounded_gpreg(wob, gndr, apdt, llac, dttype):
    if dttype == 'first':
        return gpreg_minmax[(gpreg_minmax['wob'] == wob) & (gpreg_minmax['gndr_cd'] == str(gndr)) &
                 (gpreg_minmax['first_start'] >= apdt) & (gpreg_minmax['first_start'] <= llac)]
    else:
        return gpreg_minmax[(gpreg_minmax['wob'] == wob) & (gpreg_minmax['gndr_cd'] == str(gndr)) &
                 (gpreg_minmax['final_end'] >= apdt) & (gpreg_minmax['final_end'] <= llac)]


def get_gpreg_pers(alf):
    if type(alf) != list:
        alf = [alf]
    return gpreg_pers[gpreg_pers['alf_e'].isin(alf)]


def add_alf(nalf, alf_mtch, alf_sts, uid):
    alfmtch = alf_mtch
    if not alf_mtch:
        alfmtch = 'NULL'
    
    q = '''
    UPDATE {0}
    SET alf_e = {1}, alf_mtch_pct = {2}, alf_sts_cd = {3}, alf_source = 'GP_EVENT'
    WHERE unified_id = '{4}'
    '''.format(mlac_alf, int(nalf), alfmtch, alf_sts, uid)
    
    al.call_no_return(q)

def inplace_udate_alf(alf):
    idxs = gpev_child[gpev_child['alf_e'] == alf].index.to_list()
    for idx in idxs:
        gpev_child.at[idx, 'alf_in_lac'] = 1

    idxs = wdsdrs[wdsdrs['alf_e'] == alf].index.to_list()
    for idx in idxs:
        wdsdrs.at[idx, 'alf_in_lac'] = 1

    idxs = gprs[gprs['alf_e'] == alf].index.to_list()
    for idx in idxs:
        gprs.at[idx, 'alf_in_lac'] = 1

def get_base(pers):
    
    tep = edf[edf['unified_id'] == pers]
    tcep = cedf[cedf['unified_id'] == pers]
    tcep['time_placement'] = (pd.to_datetime(tcep['episode_end_date']) - pd.to_datetime(tcep['episode_start_date'])).dt.days
    tmn = mdf[mdf['unified_id'] == pers]
    wob = tep['wob'].iloc[0]
    gndr = int(tmn['gndr_cd'].iloc[0])
    lanm = tep['la_name'].iloc[0]
    all_la = list(set(list([x for x in tep['placement_localauth'].unique() if x is not None]) + [lanm]))
    flac = tep['episode_start_date'].min()
    oflac = flac
    llac = tcep['episode_end_date'].max()
    plsoa = [x for x in list(tep['lsoa2011_placement_postcode'].unique()) if x is not None]
    hlsoa = [x for x in list(tmn['lsoa2011_home_postcode'].unique()) if x is not None]
    alsoa = plsoa + hlsoa
    adopt = tep['reason_episode_finished_code'].isin(['E1','E11','E12']).any()
    
    return tep, tcep, tmn, wob, gndr, lanm, all_la, flac, oflac, llac, plsoa, alsoa, hlsoa, adopt

def ulist(ser):
    return list(ser.unique())

### remove kids who are only ever on respite care, and who are only ever placed with their parents
fedf = edf[(edf['legal_status_code'] != 'V1') & (edf['placement_type_code'] != 'P1') & (~edf['wob'].isna())]

tmp = fedf.groupby(['unified_id'])['age_start'].min().reset_index()
tmp = tmp[tmp['age_start'] < 0]
earlystart = list(tmp['unified_id'].unique())

fedf = fedf[~fedf['unified_id'].isin(earlystart)]
to_match = fedf['unified_id'].unique()

for pers in tqdm(to_match):
    tep, tcep, tmn, wob, gndr, lanm, all_la, flac, oflac, llac, plsoa, alsoa, hlsoa, adopt = get_base(pers)
    
    if tep['episode_start_date'].min() < datetime.date(2002,1,1):
        continue
    
    # if they're never in a placement for more than 3 weeks (21 days) then skip, can't match
    if tcep['time_placement'].max() < 21:
        continue
        
    # get all care-related GP events for people with this wob and gender
    gplac = wide_ev_type(wob, gndr, 'narrow care')
    gplac = gplac[gplac['alf_in_lac'] == 0]
    gprec = wide_ev_type(wob, gndr, 'pre care')
    gprec['event_dt'] = gn.dt_type_fix(gprec['event_dt'])
    gprec = gprec[gprec['alf_in_lac'] == 0]
    gprec = gprec[gprec['event_dt'] <= flac]
    
    gpall = wide_ev_type(wob, gndr, 'care')
    gpall = gpall[gpall['alf_in_lac'] == 0]
    gpall['event_dt'] = gn.dt_type_fix(gpall['event_dt'])
    gpall = gpall[gpall['event_dt'] <= llac]
    
    gplpre = gprec[gprec['lsoa_cd'].isin(hlsoa)]
    gpldur = gpall[gpall['lsoa_cd'].isin(plsoa)]
    
    gploc = pd.concat([gplpre, gpldur])
    
    if len(plsoa) == 0 or len(gploc) == 0:
        gploc = gpall[gpall['lad20nm'] == lanm]
        
    intersec = set(ulist(gplac['alf_e'])).intersection(set(ulist(gprec['alf_e']))).intersection(set(ulist(gploc['alf_e'])))
    
    if adopt and len(intersec) > 1:
        gpapt = wide_ev_type(wob, gndr, 'adoption child')
        intersec = intersec.intersection(ulist(gpapt['alf_e']))
    
    if len(intersec) == 1:
        add_alf(list(intersec)[0], 'NULL', 1, pers)
        inplace_udate_alf(list(intersec)[0])
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
    
    intersec = set(ulist(wdhm['alf_e'])).intersection(set(ulist(wdpl['alf_e'])))
    
    check = wdpl[wdpl['alf_e'].isin(list(intersec))][
                    ['alf_e', 'alf_in_lac', 'narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev', 'sum']
                ].drop_duplicates(keep='first')
    if adopt:        
        checkev = check[check['adopt_ev'] == 1]
    else:
        check = check[(check['adopt_ev'] == 0) & (check['sum'] > 0)]
        checkev = check[check['sum'] == check['sum'].max()]
    
    if checkev['alf_e'].nunique() == 1:
        add_alf(checkev['alf_e'].iloc[0], 'NULL', 1, pers)
        inplace_udate_alf(checkev['alf_e'].iloc[0])
        continue
        
    flt = gprs[(gprs['wob'] == wob) & (gprs['gndr_cd'] == str(gndr)) &
       (gprs['lsoa_cd'].isin(plsoa)) & (gprs['alf_in_lac'] == 0)]

    gppl = flt[(flt.apply(lambda x: DateTimeRange(flac, llac).is_intersection(DateTimeRange(x['start_date'], x['end_date'])), axis=1))]
    
    flt = gprs[(gprs['wob'] == wob) & (gprs['gndr_cd'] == str(gndr)) &
       (gprs['lsoa_cd'].isin(hlsoa)) & (gprs['alf_in_lac'] == 0)]

    gphm = flt[(flt['start_date'] < flac) & (flt['end_date'] < llac)]
    
    intersec = set(ulist(gppl['alf_e'])).intersection(set(ulist(gphm['alf_e'])))
    
    check = wdpl[wdpl['alf_e'].isin(list(intersec))][
                    ['alf_e', 'alf_in_lac', 'narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev', 'sum']
                ].drop_duplicates(keep='first')
    
    if adopt:        
        checkev = check[check['adopt_ev'] == 1]
    else:
        check = check[(check['adopt_ev'] == 0) & (check['sum'] > 0)]
        checkev = check[check['sum'] == check['sum'].max()]
    
    if checkev['alf_e'].nunique() == 1:
        add_alf(checkev['alf_e'].iloc[0], 'NULL', 1, pers)
        inplace_udate_alf(checkev['alf_e'].iloc[0])
        continue

## Report
q = '''
SELECT COUNT(DISTINCT(unified_id)) AS ncinrcs
FROM {0}
WHERE alf_source = 'GP_EVENT'
'''.format(mlac_alf)
numf = al.call(q).iloc[0]['ncinrcs']

print('Number of ALFs found via GP Events and LSOA matches: {0}'.format(numf))


# GP events

## Get the LAC tables
adf, mdf, edf, cedf = gn.get_refreshed_lac_tables(mlac_alf, mlac_main, mlac_epi, la_codes, mlac_cepi, True)

## read in the GP read code reference file
f = open('./helper_files/gp_read_codes.json')
read_codes = json.load(f)

child_codes = list(set(list(read_codes['adoption child'].keys()) + 
    list(read_codes['care'].keys()) + 
    list(read_codes['narrow care'].keys()) +
    list(read_codes['pre care'].keys())))

adult_codes = list(set(list(read_codes['adoption'].keys()) + 
                       list(read_codes['parent of LAC'].keys()) + 
                       list(read_codes['foster parent'].keys()) +
                       list(read_codes['adoptive parent'].keys())))

## Pull into memory 
### Grab the datasets into the memory we have 
lacmaxwob = adf[~adf['wob'].isna()]['wob'].max()
lacminwob = adf[~adf['wob'].isna()]['wob'].min()

q = '''
SELECT gp.alf_e, MAX(gp.end_date) AS mend
FROM {0} AS gp
LEFT JOIN {1} AS alf
ON gp.alf_e = alf.alf_e
WHERE alf.wob >= '{2}' AND alf.wob <= '{3}'
GROUP BY gp.alf_e
'''.format(gpreg, gpalf, lacminwob, lacmaxwob)

gpreg_pers = al.call(q)

q = '''SELECT DISTINCT gp.*, CASE WHEN lac.alf_e IS NOT NULL THEN 1 ELSE 0 END AS alf_in_lac
FROM (
    SELECT DISTINCT reg.alf_e, alf.wob, alf.gndr_cd, 
    FIRST_VALUE(start_date) OVER (PARTITION BY reg.alf_e ORDER BY reg.start_date) AS first_start,
    FIRST_VALUE(end_date) OVER (PARTITION BY reg.alf_e ORDER BY reg.end_date DESC) AS final_end
    FROM {0} AS reg
    INNER JOIN (
        SELECT DISTINCT alf_e, wob, gndr_cd
        FROM {1}
        WHERE wob >= '{2}' AND wob <= '{3}'
    ) AS alf
    ON reg.alf_e = alf.alf_e
) AS gp
LEFT JOIN {4} AS lac
ON gp.alf_e = lac.alf_e '''.format(gpreg, gpalf, lacminwob, lacmaxwob, mlac_alf)

gpreg_minmax = al.call(q)

q = '''
SELECT gp.alf_e, alf.wob, alf.gndr_cd, gp.start_date, gp.end_date, alf.lsoa_cd, CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac,
CASE WHEN ncare.alf_e IS NULL THEN 0 ELSE 1 END AS narrow_care_ev,
CASE WHEN pcare.alf_e IS NULL THEN 0 ELSE 1 END AS precare_ev,
CASE WHEN adopt.alf_e IS NULL THEN 0 ELSE 1 END AS adopt_ev,
CASE WHEN gpparent.alf_e IS NULL THEN 0 ELSE 1 END AS parent_ev
FROM {0} AS gp
LEFT JOIN {1} AS alf
ON gp.alf_e = alf.alf_e
LEFT JOIN (
    SELECT DISTINCT alf_e 
    FROM {2} 
) AS lac
ON alf.alf_e = lac.alf_e
LEFT JOIN {5} AS ncare
ON alf.alf_e = ncare.alf_e
LEFT JOIN {6} AS pcare
ON alf.alf_e = pcare.alf_e
LEFT JOIN {7} AS adopt
ON alf.alf_e = adopt.alf_e
LEFT JOIN (
    SELECT DISTINCT ncchd_births.alf_e, ncchd_births.mat_alf_e 
    FROM {8} AS ncchd_births
    INNER JOIN {9} AS pgpev
    ON ncchd_births.mat_alf_e = pgpev.alf_e
) AS gpparent
ON alf.alf_e = gpparent.alf_e
WHERE alf.wob >= '{3}'
AND alf.wob <= '{4}'
ORDER BY start_date
'''.format(gpreg, gpalf, mlac_alf, lacminwob, lacmaxwob, gpncare, gppcare, gpadopt, ncchd_births, gpparent)
gprs = al.call(q)

gprs['sum'] = gprs[['narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev']].sum(axis=1)

q = '''
SELECT wdsdralf.alf_e, alf.wob, alf.gndr_cd, wdsdralf.start_date, wdsdralf.end_date, wdsdralf.ralf_e, wdsdralf.lsoa2011_cd, 
CASE WHEN lac.alf_e IS NULL THEN 0 ELSE 1 END AS alf_in_lac,
CASE WHEN ncare.alf_e IS NULL THEN 0 ELSE 1 END AS narrow_care_ev,
CASE WHEN pcare.alf_e IS NULL THEN 0 ELSE 1 END AS precare_ev,
CASE WHEN adopt.alf_e IS NULL THEN 0 ELSE 1 END AS adopt_ev, 
CASE WHEN gpparent.alf_e IS NULL THEN 0 ELSE 1 END AS parent_ev
FROM {0} AS wdsdralf
LEFT JOIN {1} AS alf
ON wdsdralf.alf_e = alf.alf_e
LEFT JOIN (
    SELECT DISTINCT alf_e 
    FROM {2} 
) AS lac
ON wdsdralf.alf_e = lac.alf_e
LEFT JOIN {5} AS ncare
ON alf.alf_e = ncare.alf_e
LEFT JOIN {6} AS pcare
ON alf.alf_e = pcare.alf_e
LEFT JOIN {7} AS adopt
ON alf.alf_e = adopt.alf_e
LEFT JOIN (
    SELECT DISTINCT ncchd_births.alf_e, ncchd_births.mat_alf_e 
    FROM {8} AS ncchd_births
    INNER JOIN {9} AS pgpev
    ON ncchd_births.mat_alf_e = pgpev.alf_e
) AS gpparent
ON alf.alf_e = gpparent.alf_e
WHERE alf.wob >= '{3}'
AND alf.wob <= '{4}'
ORDER BY start_date
'''.format(wdsdralf, wdsdalf, mlac_alf, lacminwob, lacmaxwob, gpncare, gppcare, gpadopt, ncchd_births, gpparent)
wdsdrs = al.call(q)

wdsdrs['sum'] = wdsdrs[['narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev']].sum(axis=1)

q = '''SELECT DISTINCT gp.alf_e, gp.wob, gp.gndr_cd, gp.lsoa_cd, la.lad20nm, gp.event_cd, 
CASE WHEN gp.event_dt = '0001-01-01' THEN '2099-01-01' ELSE gp.event_dt END AS event_dt, dc.pref_term_30,
CASE WHEN lac.alf_e IS NOT NULL THEN 1
ELSE 0
END AS alf_in_lac
FROM {0} AS gp
LEFT JOIN {1} AS dc
ON gp.event_cd = dc.READ_CODE
LEFT JOIN {2} AS la
ON gp.lsoa_cd = la.lsoa11cd
LEFT JOIN {3} AS lac
ON gp.alf_e = lac.alf_e
WHERE gp.wob >= '{4}'
AND gp.wob <= '{5}'
AND gp.event_cd IN ({6})
AND gp.alf_e IS NOT NULL
'''.format(gpevent, readcode, lsoa_decode, mlac_alf, lacminwob, lacmaxwob, ','.join(["'"+x+"'" for x in child_codes]))

gpev_child = al.call(q)
gpev_child['event_dt'] = gn.dttime_fix(gpev_child['event_dt'])
gpev_child['wob'] = gn.dt_type_fix(gpev_child['wob'])

q = '''SELECT DISTINCT gp.alf_e, gp.wob, gp.gndr_cd, gp.lsoa_cd, la.lad20nm, gp.event_cd, 
CASE WHEN gp.event_dt = '0001-01-01' THEN '2099-01-01' ELSE gp.event_dt END AS event_dt, dc.pref_term_30
FROM {0} AS gp
LEFT JOIN {1} AS dc
ON gp.event_cd = dc.READ_CODE
LEFT JOIN {2} AS la
ON gp.lsoa_cd = la.lsoa11cd
WHERE gp.event_cd IN ({3})
AND gp.alf_e IS NOT NULL
'''.format(gpevent, readcode, lsoa_decode, ','.join(["'"+x+"'" for x in adult_codes]))

gpev_adult = al.call(q)
gpev_adult['event_dt'] = gn.dttime_fix(gpev_adult['event_dt'])
gpev_adult['wob'] = gn.dt_type_fix(gpev_adult['wob'])

## Matching 
def person_ev_all(alf):
    q = '''SELECT DISTINCT gp.alf_e, gp.lsoa_cd, la.lad20nm, gp.event_cd, gp.event_dt, dc.pref_term_30 FROM {0} AS gp
    LEFT JOIN {1} AS dc
    ON gp.event_cd = dc.READ_CODE
    LEFT JOIN {2} AS la
    ON gp.lsoa_cd = la.lsoa11cd
    WHERE gp.alf_e = {3}
    ORDER BY event_dt
    '''.format(gpevent, readcode, lsoa_decode, alf)
    return al.call(q)

def person_ev_type(alf, evtype):
    rcs = read_codes[evtype]
    
    q = '''SELECT DISTINCT gp.alf_e, gp.lsoa_cd, la.lad20nm, gp.event_cd, gp.event_dt, dc.pref_term_30 FROM {0} AS gp
    LEFT JOIN {1} AS dc
    ON gp.event_cd = dc.READ_CODE
    LEFT JOIN {2} AS la
    ON gp.lsoa_cd = la.lsoa11cd
    WHERE gp.alf_e = {3}
    AND gp.event_cd IN ({4})
    '''.format(gpevent, readcode, lsoa_decode, alf, ','.join(["'"+x+"'" for x in rcs.keys()]))
    return al.call(q)

def alf_against_lsoas(alfs, lsoas):
    q = '''
    SELECT * 
    FROM {0} AS ls
    LEFT JOIN {1} AS alf
    ON ls.pers_id_e = alf.pers_id_e
    WHERE alf.alf_e IN ({2})
    AND ls.lsoa2001_cd IN ({3})
    '''.format(wdsdlsoa, wdsdalf, ','.join([str(x) for x in alfs]), ','.join(["'"+x+"'" for x in lsoas]))
    
    return al.call(q)

def wide_ev_type(wob, gender, evtype):
    rcs = list(read_codes[evtype].keys())
    
    gdf = gpev_child[(gpev_child['wob'] == wob) & (gpev_child['gndr_cd'] == str(gender)) &
                     (gpev_child['event_cd'].isin(rcs))]
    
    return gdf

def person_ev_type(alf, evtype):
    rcs = list(read_codes[evtype].keys())
    
    gdf = gpev_child[(gpev_child['alf_e'] == alf) &
                     (gpev_child['event_cd'].isin(rcs))]
    
    return gdf


def people_ev_type(alfs, evtype):
    rcs = list(read_codes[evtype].keys())
    
    gdf = gpev_child[(gpev_child['alf_e'].isin(alfs)) &
                     (gpev_child['event_cd'].isin(rcs))]
    
    return gdf


def get_datebounded_gpreg(wob, gndr, apdt, llac, dttype):
    if dttype == 'first':
        return gpreg_minmax[(gpreg_minmax['wob'] == wob) & (gpreg_minmax['gndr_cd'] == str(gndr)) &
                 (gpreg_minmax['first_start'] >= apdt) & (gpreg_minmax['first_start'] <= llac)]
    else:
        return gpreg_minmax[(gpreg_minmax['wob'] == wob) & (gpreg_minmax['gndr_cd'] == str(gndr)) &
                 (gpreg_minmax['final_end'] >= apdt) & (gpreg_minmax['final_end'] <= llac)]


def get_gpreg_pers(alf):
    if type(alf) != list:
        alf = [alf]
    return gpreg_pers[gpreg_pers['alf_e'].isin(alf)]


def add_alf(nalf, alf_mtch, alf_sts, uid):
    alfmtch = alf_mtch
    if not alf_mtch:
        alfmtch = 'NULL'
    
    q = '''
    UPDATE {0}
    SET alf_e = {1}, alf_mtch_pct = {2}, alf_sts_cd = {3}, alf_source = 'GP_EVENT'
    WHERE unified_id = '{4}'
    '''.format(mlac_alf, int(nalf), alfmtch, alf_sts, uid)
    
    al.call_no_return(q)

def inplace_udate_alf(alf):
    idxs = gpev_child[gpev_child['alf_e'] == alf].index.to_list()
    for idx in idxs:
        gpev_child.at[idx, 'alf_in_lac'] = 1

    idxs = wdsdrs[wdsdrs['alf_e'] == alf].index.to_list()
    for idx in idxs:
        wdsdrs.at[idx, 'alf_in_lac'] = 1

    idxs = gprs[gprs['alf_e'] == alf].index.to_list()
    for idx in idxs:
        gprs.at[idx, 'alf_in_lac'] = 1

def get_base(pers):
    
    tep = edf[edf['unified_id'] == pers]
    tcep = cedf[cedf['unified_id'] == pers]
    #tcep['time_placement'] = (pd.to_datetime(tcep['episode_end_date']) - pd.to_datetime(tcep['episode_start_date'])).dt.days
    #tcep['time_placement'] = tcep.apply(lambda row: ((pd.to_datetime(row.episode_end_date) - pd.to_datetime(row.episode_start_date)).days), axis=1)
    time_placement = (pd.to_datetime(tcep['episode_end_date']) - pd.to_datetime(tcep['episode_start_date'])).dt.days
    tcep = tcep.copy()
    tcep['time_placement'] = (pd.to_datetime(tcep['episode_end_date']) - pd.to_datetime(tcep['episode_start_date'])).dt.days
    tmn = mdf[mdf['unified_id'] == pers]
    wob = tep['wob'].iloc[0]
    gndr = int(tmn['gndr_cd'].iloc[0])
    lanm = tep['la_name'].iloc[0]
    all_la = list(set(list([x for x in tep['placement_localauth'].unique() if x is not None]) + [lanm]))
    flac = tep['episode_start_date'].min()
    oflac = flac
    llac = tcep['episode_end_date'].max()
    plsoa = [x for x in list(tep['lsoa2011_placement_postcode'].unique()) if x is not None]
    hlsoa = [x for x in list(tmn['lsoa2011_home_postcode'].unique()) if x is not None]
    alsoa = plsoa + hlsoa
    adopt = tep['reason_episode_finished_code'].isin(['E1','E11','E12']).any()
    
    return tep, tcep, tmn, wob, gndr, lanm, all_la, flac, oflac, llac, plsoa, alsoa, hlsoa, adopt

def ulist(ser):
    return list(ser.unique())

### remove kids who are only ever on respite care, and who are only ever placed with their parents
fedf = edf[(edf['legal_status_code'] != 'V1') & (edf['placement_type_code'] != 'P1') & (~edf['wob'].isna())]

tmp = fedf.groupby(['unified_id'])['age_start'].min().reset_index()
tmp = tmp[tmp['age_start'] < 0]
earlystart = list(tmp['unified_id'].unique())

fedf = fedf[~fedf['unified_id'].isin(earlystart)]
to_match = fedf['unified_id'].unique()

for pers in tqdm(to_match):
    tep, tcep, tmn, wob, gndr, lanm, all_la, flac, oflac, llac, plsoa, alsoa, hlsoa, adopt = get_base(pers)
    
    if tep['episode_start_date'].min() < datetime.date(2002,1,1):
        continue
    
    # if they're never in a placement for more than 3 weeks (21 days) then skip, can't match
    if tcep['time_placement'].max() < 21:
        continue
        
    # get all care-related GP events for people with this wob and gender
    gplac = wide_ev_type(wob, gndr, 'narrow care')
    gplac = gplac[gplac['alf_in_lac'] == 0]
    gprec = wide_ev_type(wob, gndr, 'pre care')
    gprec['event_dt'] = gn.dt_type_fix(gprec['event_dt'])
    gprec = gprec[gprec['alf_in_lac'] == 0]
    gprec = gprec[gprec['event_dt'] <= flac]
    
    gpall = wide_ev_type(wob, gndr, 'care')
    gpall = gpall[gpall['alf_in_lac'] == 0]
    gpall['event_dt'] = gn.dt_type_fix(gpall['event_dt'])
    gpall = gpall[gpall['event_dt'] <= llac]
    
    gplpre = gprec[gprec['lsoa_cd'].isin(hlsoa)]
    gpldur = gpall[gpall['lsoa_cd'].isin(plsoa)]
    
    gploc = pd.concat([gplpre, gpldur])
    
    if len(plsoa) == 0 or len(gploc) == 0:
        gploc = gpall[gpall['lad20nm'] == lanm]
        
    intersec = set(ulist(gplac['alf_e'])).intersection(set(ulist(gprec['alf_e']))).intersection(set(ulist(gploc['alf_e'])))
    
    if adopt and len(intersec) > 1:
        gpapt = wide_ev_type(wob, gndr, 'adoption child')
        intersec = intersec.intersection(ulist(gpapt['alf_e']))
    
    if len(intersec) == 1:
        add_alf(list(intersec)[0], 'NULL', 1, pers)
        inplace_udate_alf(list(intersec)[0])
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
    
    intersec = set(ulist(wdhm['alf_e'])).intersection(set(ulist(wdpl['alf_e'])))
    
    check = wdpl[wdpl['alf_e'].isin(list(intersec))][
                    ['alf_e', 'alf_in_lac', 'narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev', 'sum']
                ].drop_duplicates(keep='first')
    if adopt:        
        checkev = check[check['adopt_ev'] == 1]
    else:
        check = check[(check['adopt_ev'] == 0) & (check['sum'] > 0)]
        checkev = check[check['sum'] == check['sum'].max()]
    
    if checkev['alf_e'].nunique() == 1:
        add_alf(checkev['alf_e'].iloc[0], 'NULL', 1, pers)
        inplace_udate_alf(checkev['alf_e'].iloc[0])
        continue
        
    flt = gprs[(gprs['wob'] == wob) & (gprs['gndr_cd'] == str(gndr)) &
       (gprs['lsoa_cd'].isin(plsoa)) & (gprs['alf_in_lac'] == 0)]

    gppl = flt[(flt.apply(lambda x: DateTimeRange(flac, llac).is_intersection(DateTimeRange(x['start_date'], x['end_date'])), axis=1))]
    
    flt = gprs[(gprs['wob'] == wob) & (gprs['gndr_cd'] == str(gndr)) &
       (gprs['lsoa_cd'].isin(hlsoa)) & (gprs['alf_in_lac'] == 0)]

    gphm = flt[(flt['start_date'] < flac) & (flt['end_date'] < llac)]
    
    intersec = set(ulist(gppl['alf_e'])).intersection(set(ulist(gphm['alf_e'])))
    
    check = wdpl[wdpl['alf_e'].isin(list(intersec))][
                    ['alf_e', 'alf_in_lac', 'narrow_care_ev', 'precare_ev', 'adopt_ev', 'parent_ev', 'sum']
                ].drop_duplicates(keep='first')
    
    if adopt:        
        checkev = check[check['adopt_ev'] == 1]
    else:
        check = check[(check['adopt_ev'] == 0) & (check['sum'] > 0)]
        checkev = check[check['sum'] == check['sum'].max()]
    
    if checkev['alf_e'].nunique() == 1:
        add_alf(checkev['alf_e'].iloc[0], 'NULL', 1, pers)
        inplace_udate_alf(checkev['alf_e'].iloc[0])
        continue

## Report
q = '''
SELECT COUNT(DISTINCT(unified_id)) AS ncinrcs
FROM {0}
WHERE alf_source = 'GP_EVENT'
'''.format(mlac_alf)
numf = al.call(q).iloc[0]['ncinrcs']

print('Number of ALFs found via GP Events and LSOA matches: {0}'.format(numf))

gn.lac_report(mlac_alf, lac_alf)