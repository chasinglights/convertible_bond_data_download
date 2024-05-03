# from bson import ObjectId
from datetime import datetime
from collections import namedtuple
from pymongo import MongoClient

URL = 'set your mongodb url'

CLIENT = MongoClient(host=URL)
DB = CLIENT['kzz_data']

def drop_collections():
    names = DB.list_collection_names()
    for name in names:

        print(names)
        DB[name].drop()


def list_database():
    database_list = CLIENT.list_database_names()
    print(database_list)


def is_data_mongodb_storable(df):
    """
    Check if the data inside a pandas DataFrame can be stored in MongoDB.
    
    :param df: pandas DataFrame
    :return: bool, indicating if the DataFrame is storable in MongoDB
    """
    def is_bson_compatible(value):
        """
        Check if the value is BSON compatible.
        """
        bson_compatible_types = (int, float, str, dict, list, tuple, bool, datetime, ObjectId, type(None))
        return isinstance(value, bson_compatible_types)

    for column in df.columns:
        for value in df[column]:
            # Handle cases where value could be NaN or other NumPy types
            if isinstance(value, (np.generic, np.ndarray)):
                value = value.item()   # Convert NumPy value to native Python type
                
            if not is_bson_compatible(value):
                print(f"Value '{value}' in column '{column}' is not BSON/MongoDB compatible.")
                return False
    return True



def mongo_collection_inserter(coll_name, df):
    """insert a dataframe into a specified collection."""
    dict_from_df = df.to_dict(orient='records')
    DB[coll_name].insert_many(dict_from_df)


def mongo_collection_fieldvalue_checker(coll_name, field, value_to_be_checked):
    """query if a colletion's field has the value."""
    query = {
        field: value_to_be_checked
    }

    num = DB[coll_name].count_documents(query)
    return num

def query_stock_code_list() -> list:

    stock_list = [i['stk_code'] for i in DB['cb_basic'].find({}, {"stk_code":1, "_id":0}) if i['stk_code'] is not None]
    return stock_list

def query_bond_stock_map() -> dict:
    map_dict = {i['ts_code']: i['stk_code'] for i in DB['cb_basic'].find({},
     {'ts_code':1, "stk_code":1, "_id":0}) if i['stk_code'] is not None}
    return map_dict

def query_bond_code_endpoints() -> list:
    """query bond_codes' start_date and end_date from cb_k_line """
    # query from mongodb
    pipeline = [
    {
        '$group': {
            '_id': '$ts_code', 
            'start_date': {
                '$min': '$trade_date'
            }, 
            'end_date': {
                '$max': '$trade_date'
            }
        }
    }
]
    results = DB['cb_k_line'].aggregate(pipeline)

    # data structure
    result_list = []
    BondInfo = namedtuple('DateEndpoints', ['bond_code', 'start_date', 'end_date'])
    for result in results:
        result_list.append(BondInfo(result['_id'], result['start_date'], result['end_date']))
    
    return result_list


def delete_redundant_stock_documents(stock_code, start_date, end_date):
    coll_name = 'stock_k_line'
    query = {
        'ts_code': stock_code, 
        '$or': [
            {
                'trade_date': {
                    '$gt': end_date
                }
            }, {
                'trade_date': {
                    '$lt': start_date
                }
            }
        ]
    }
    result = DB[coll_name].delete_many(query)
    if result.acknowledged:
        return result.deleted_count
    else:
        return "not acknowledged"


def update_one_document(coll_name, query_filter, update_dict, upsert=True):

    result = DB[coll_name].update_one(query_filter, update_dict, upsert)
    return result


if __name__ == '__main__':
    
    # coll_name = 'cb_k_line'
    # field = 'date'
    # value_to_be_checked = 1
    # a = mongo_collection_fieldvalue_checker(coll_name, field, value_to_be_checked)
    # print("result's ", a)
    # list_database()
    stock_code, start_date, end_date = '000001.SZ', datetime(2016,2,19), datetime(2019,9,20)
    a = delete_redundant_stock_documents(stock_code, start_date, end_date)
    print(a)
    


