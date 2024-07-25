
from ibm_db_dbi import OperationalError
from sqlalchemy import *
from sqlalchemy import types
from decimal import Decimal
import pandas as pd
import ibm_db as ibm
import sqlalchemy as db
import ibm_db_sa

def set_me_up(username, password):
    global engine
    engine = database_connector(username, password)
    return engine

def database_connector(uname, pwd):
    connection_string = 'ibm_db_sa://{0}:{1}@/PR_SAIL'.format(uname, pwd)
    try:
        engine = db.create_engine(connection_string)
        connection = engine.connect()
        inspector = inspect(engine)
    except Exception as exx:
        pass
        print(exx)
        print("Your login credentials might be wrong, or DB2 might be down.")
    return engine

def call(query):
    if engine:
        ResultProxy = engine.execute(query)
        results = ResultProxy.fetchall()
        dataframe = pd.DataFrame(results, columns=ResultProxy.keys())
        # dataframe.columns = ResultProxy.keys()
        return dataframe
    else:
        return "There's a problem with the connection"

def test_call(query):
    if engine:
        ResultProxy = engine.execute(query)
        results = ResultProxy.fetchall()
        if results:
            dataframe = pd.DataFrame(results)
            dataframe.columns = ResultProxy.keys()
            return dataframe
        else:
            return False
    return "You need to set this up before using it. Call al_db_conn.core.set_me_up('your username', 'your password') to get setup."

def call_no_return(query):
    if engine:
        ResultProxy = engine.execute(query)

def call_commit(query):
    if engine:
        with engine.begin() as conn:
            conn.execute(query)

def rename_helper(df):
    df.rename(columns = {0:'TABSCHEMA', 1:'TABNAME', 2:'COLNAME', 3:'COLNO', 4:'TYPESCHEMA', 5:'TYPENAME', 6:'LEN (BYTES)', 7:'SCALE', 8:'TYPESTRINGUNITS', 9:'STRINGUNITSLENGTH', 
                        10:'DEFAULT', 11:'NULLS', 12:'CODEPAGE', 13:'COLLATIONSCHEMA', 14:'COLLATIONNAME', 15:'LOGGED', 16:'COMPACT', 17:'COLCARD', 18:'HIGH2KEY', 19:'LOW2KEY',
                        20:'AVGCOLLEN', 21:'KEYSEQ', 22:'PARTSKEYSEQ', 23:'NQUANTILES', 24:'NMOSTFREQ', 25:'NUMNULLS', 26:'TARGET_TYPESCHEMA', 27:'TARGET_TYPENAME', 28:'SCOPE_TABSCHEMA',
                        29:'SCOPE_TABNAME', 30:'SOURCE_TABSCHEMA', 31:'SOURCE_TABNAME', 32:'DL_FEATURES', 33:'SPECIAL_PROPS', 34:'HIDDEN', 35:'INLINE_LENGTH', 36:'PCTINLINED', 37:'IDENTITY',
                        38:'ROWCHANGETIMESTAMP', 39:'GENERATED', 40:'TEXT', 41:'COMPRESS', 42:'AVGDISTINCTPERPAGE', 43:'PAGEVARIANCERATION', 44:'SUB_COUNT', 45:'SUB_DELIM_LENGTH',
                        46:'AVGCOLLENCHAR', 47:'IMPLICITVALUE', 48:'SECLABELNAME', 49:'ROWBEGIN', 50:'ROWEND', 51:'TRANSACTIONSTARTID', 52:'RANDDISTKEY', 53:'PCTENCODED', 
                        54:'AVGENCODEDCOLLEN', 55:'QUALIFIER', 56:'FUNC_PATH', 57:'REMARKS'}, inplace=True)
    return df

def makeCat(items, target_df):
    for item in items:
        target_df[item] = target_df[item].astype('category')
    return target_df

def makeDate(items, target_df):
    for item in items:
        target_df[item] = pd.to_datetime(target_df[item])
    return target_df

def makeFloat(items, target_df):
    for item in items:
        target_df[item] = pd.to_numeric(target_df[item], errors='coerce')
    return target_df

def dropDupeColumns(frame, keep=[]):
    cols = list(frame.columns)
    drop_nums = []
    always_drop = ['extract_dt', 'field_1', 'field_2', 'field_3', 'field_4', 'avail_from_dt', 'processed_date']
    cols_info = getDuplicatesWithInfo(cols)
    for col, value in cols_info.items():
        if col in always_drop:
            drop_nums.append(value[1][:])
        elif value[0] > 1:
            drop_nums.append(value[1][1:])
    flat_drop_nums = [item for sublist in drop_nums for item in sublist]
    flat_drop_nums = sorted(flat_drop_nums)
    filtered_drops = [x for x in flat_drop_nums if x not in keep]
    col_nums = [x for x in range(frame.shape[1])]
    col_nums = [x for x in col_nums if x not in filtered_drops]
    return frame.iloc[:, col_nums]
    
        
def getDuplicatesWithInfo(listOfElems):
    ''' Get duplicate element in a list along with thier indices in list
    and frequency count'''
    dictOfElems = dict()
    index = 0
    # Iterate over each element in list and keep track of index
    for elem in listOfElems:
        # If element exists in dict then keep its index in lisr & increment its frequency
        if elem in dictOfElems:
            dictOfElems[elem][0] += 1
            dictOfElems[elem][1].append(index)
        else:
            # Add a new entry in dictionary 
            dictOfElems[elem] = [1, [index]]
        index += 1    

    dictOfElems = { key:value for key, value in dictOfElems.items() }
    return dictOfElems

def get_creds():
    return '', ''

def tryReconnect():
    start('', '')

def start(username, pwd):
    global engine
    engine = None
    if len(username) > 0:
        try:
            engine = database_connector(username, pwd)
            print("Connected to DB2")
        except:
            pass
            print("Could not connect to DB2, it might down or your password might be wrong.")
    else:
        print("This hasn't been set up yet. Run set_me_up or check the docs to get started.")
        
def make_table(df, schema, table_name, del_existing=False, coerce_types=True, type_dict={}):
    # check if this table exists in this schema
    q = '''SELECT COUNT(*) FROM syscat.tables WHERE 
        TABSCHEMA = '{0}'
        AND
        TABNAME = '{1}' '''.format(schema, table_name)
    test_exist = call(q)
    if (test_exist.iloc[0,0] != 0) and (del_existing == False):
        err_string = "DATA NOT INSERTED INTO TABLE. Table {0} already exists in schema {1}, and you didn't indicate that you want to delete it. If you want to delete it then call this method again, adding del_existing=True to the parameters. Alternatively, you might have typed the schema incorrectly.".format(table_name, schema)
        return print(err_string)
    obj_type_cols = df.dtypes[df.dtypes=='object'].index
    if not coerce_types and len(type_dict) == 0 and len(obj_type_cols) > 0:
        err_string = "You have chosen to not coerce types and have also not sent a dictionary with column type mappings. Your dataframe contains columns of object type and this will fail to be put into DB2 as DB2 doesn't recognise object type columns. You can either set the types of the problem columns in the dataframe and call this again, or use the parameter coerce_types=True, or send in a dictionary of column type mappings with the parameter type_dict. \n The problem columns are: {0}".format(obj_type_cols)
        return print(err_string)
    if not coerce_types and len(type_dict) > 0 and len(obj_type_cols) > 0:
        if not all(name in type_dict for name in obj_type_cols):
            problem_cols = (set(obj_type_cols) - type_dict.keys())
            err_string = "You have sent a dictionary with column types but this doesn't include all columns of object type. You can either re-run this with coerce_types=True, add the missing column types to the type_dict, or set the types of the problem columns directly in the dataframe. \n The problem columns are: {0}".format(problem_cols)
            return print(err_string)
    ac = list(df.columns)
    mapper_dict = {'int64':types.BigInteger(), 'float64':types.Float(), 'datetime64[ns]':types.VARCHAR(length=255), 'int32':types.Integer()}
    type_dict = {}
    if coerce_types:
        for col in ac:
            ctype = df[col].dtype
            if ctype == 'object':
                sa_type = coerce_object(df, col)
            else:
                sa_type = mapper_dict[str(ctype)]
            type_dict[col] = sa_type

    if (test_exist.iloc[0,0] != 0) and (del_existing):
        q = '''DROP TABLE {0}.{1} '''.format(schema, table_name)
        call_no_return(q)

    df.to_sql(name=table_name, con=engine, schema=schema, dtype=type_dict, index=False, method='multi', chunksize=10)

def coerce_object(df, col_name):
    fval = df.loc[0, col_name]
    plain_type = types.VARCHAR(length=255)
    return plain_type
