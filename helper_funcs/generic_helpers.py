from . import db_conn as al
import pandas as pd
import numpy as np
import json

def get_refreshed_lac_tables(mlac_alf, mlac_main, mlac_epi, la_ref, mlac_cepi=None, cleaned=False):
    alfq = '''
    SELECT alf.*, ref.la_name FROM {0} AS alf
    LEFT JOIN {1} AS ref
    ON alf.local_authority_code = ref.la_id
    WHERE alf_e IS NULL
    '''.format(mlac_alf, la_ref)
    alfdf = al.call(alfq)

    mainq = '''SELECT DISTINCT 
        main.*,
        alf.wob,
        alf.gndr_cd
    FROM {0} AS main
    LEFT JOIN {1} AS alf
    ON main.hybrid_id = alf.hybrid_id
    WHERE alf.alf_e IS NULL
    ORDER BY year_code
    '''.format(mlac_main, mlac_alf, mlac_epi)
    maindf = al.call(mainq)

    epiq = '''
    SELECT 
        epi.*,
        alf.wob,
        alf.gndr_cd,
        alf.potential_unknown_wob,
        alf.max_poss_wob
    FROM {0} AS epi
    LEFT JOIN {1} AS alf
    ON epi.hybrid_id = alf.hybrid_id
    WHERE alf.alf_e IS NULL
    ORDER BY year_code, episode_number
    '''.format(mlac_epi, mlac_alf)
    epidf = al.call(epiq)

    # epidf['episode_end_date'] = epidf['episode_end_date'].apply(
    #     lambda x: str(x) if str(x) != 'None' else x
    # )
    # epidf['episode_end_date'] = epidf['episode_end_date'].replace(
    #     '9999-12-31', '2099-01-01'
    # )
    
    maindf['collection_corrected'] = maindf['year_code'].astype(str).str[:2] + maindf['year_code'].astype(str).str[-2:] + '-03-31'
    maindf['collection_corrected'] = dt_type_fix(maindf['collection_corrected'])
    maindf['dtwob'] = dt_type_fix(maindf['wob'])
    maindf['age_at_collection'] = (pd.to_datetime(maindf['collection_corrected']) - pd.to_datetime(maindf['dtwob'])).dt.days/365.25
    
    epidf['wob'] = dt_type_fix(epidf['wob'])
    epidf['episode_start_date'] = dt_type_fix(epidf['episode_start_date'])
    epidf['episode_end_date'] = dt_type_fix(epidf['episode_end_date'])
    
    epidf['age_start'] = (pd.to_datetime(epidf['episode_start_date']) - pd.to_datetime(epidf['wob'])).dt.days / 365.25
    epidf['age_end'] = np.where(epidf['episode_end_date'] is not None, 
                                (pd.to_datetime(epidf['episode_end_date']) - pd.to_datetime(epidf['wob'])).dt.days / 365.25, None)
    if cleaned == False:
        return alfdf, maindf, epidf
    
    cepiq = '''
    SELECT
        *
    FROM {0}
    ORDER BY episode_start_date'''.format(mlac_cepi)
    cepidf = al.call(cepiq)

    # cepidf['episode_end_date'] = cepidf['episode_end_date'].apply(
    #     lambda x: str(x) if str(x) != 'None' else x
    # )
    # cepidf['episode_end_date'] = cepidf['episode_end_date'].replace(
    #     '9999-12-31', '2099-01-01'
    # )

    cepidf['wob'] = dt_type_fix(cepidf['wob'])
    cepidf['episode_start_date'] = dt_type_fix(cepidf['episode_start_date'])
    cepidf['episode_end_date'] = dt_type_fix(cepidf['episode_end_date'])

    cepidf['age_start'] = (pd.to_datetime(cepidf['episode_start_date']) - pd.to_datetime(cepidf['wob'])).dt.days / 365.25
    cepidf['age_end'] = np.where(cepidf['episode_end_date'] is not None, 
                                (pd.to_datetime(cepidf['episode_end_date']) - pd.to_datetime(cepidf['wob'])).dt.days / 365.25, None)
    
    return alfdf, maindf, epidf, cepidf

def dt_type_fix(series):
    return pd.to_datetime(series).dt.date
