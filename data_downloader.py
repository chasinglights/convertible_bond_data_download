import os
import pandas as pd
import logging.config
from datetime import datetime, timedelta
from tushare_utils import get_cb_k_line_from_tushare,get_cb_basic_from_tushare, get_stock_k_line_from_tushare
from mongo_utils import *

# print(os.getcwd())

logging.config.fileConfig('logging.conf', defaults={'file_name': 'data_downloader'})
LOGGER = logging.getLogger('data_downloader')

STOCK_TRACEBACK_DAYS_BEFORE_BOND = -365 * 2


def cb_k_line_downloader(trade_date):
    """download cb_k_line from tushare to mongodb day by day.
    :params trade_date: datetime type.
    """
    coll_name = 'cb_k_line'
    num_already_exists = mongo_collection_fieldvalue_checker(coll_name, "trade_date", trade_date)
    if num_already_exists:
        LOGGER.info(f"date: {trade_date}; notes: {num_already_exists} documents already in.")
        return 
    cb_k_line_i = get_cb_k_line_from_tushare(trade_date) 
    if cb_k_line_i.empty:
        LOGGER.info(f"date: {trade_date}; notes: not trade date.")
        return
    mongo_collection_inserter(coll_name, cb_k_line_i)
    LOGGER.info(f"date: {trade_date}; {len(cb_k_line_i)} documents downloaded.")

def cb_k_line_downloader_batch(start_date, end_date):
    date_list = pd.date_range(start_date, end_date)
    for _date in date_list:
        cb_k_line_downloader(_date)

def cb_basic_downloader(daily_refresh=True):
    coll_name = 'cb_basic'

    cb_basic = get_cb_basic_from_tushare()
    if daily_refresh:
        for index, row in cb_basic.iterrows():
            bond_code = row['ts_code']
            query_filter = {
                'ts_code': bond_code
            }
            update_dict = row.to_dict()
            del update_dict['ts_code']
            update_one_document(coll_name, query_filter, update_dict, upsert=True)
        LOGGER.info(f"cb_basic_data updated.")
        return

    mongo_collection_inserter(coll_name, cb_basic)
    LOGGER.info(f"cb_basic_data created.")


def stock_date_endpoints() -> pd.DataFrame:
        """columns->[start_date, end_date], index-> stock_code"""
        # 获取数据
        cb_endpoint_list = query_bond_code_endpoints()
        map_dict = query_bond_stock_map()
        # 多对一：转债多，股票一
        stock_end_point_df = pd.DataFrame(cb_endpoint_list)
        stock_end_point_df['stock_code'] = stock_end_point_df['bond_code'].apply(lambda x: map_dict[x])
        stock_end_point_df = stock_end_point_df.groupby('stock_code').agg({'start_date': 'min', 'end_date': 'max'})
        stock_end_point_df['start_date'] = stock_end_point_df['start_date'].apply(lambda x: x + timedelta(days=STOCK_TRACEBACK_DAYS_BEFORE_BOND))
        # stock_list = stock_end_point_df[(stock_end_point_df['start_date'] <= trade_date) & (stock_end_point_df['end_date'] >= trade_date)].index.to_list() 
        return stock_end_point_df

def stock_k_line_dowloader(trade_date, stock_list='query'):
    """
    :params trade_date: datetime type.
    :params stock_list: "query", which is much slower and maily for daily updates, or list type, faster and maily for batch download. 
    """
    coll_name = 'stock_k_line'
    num_already_exists = mongo_collection_fieldvalue_checker(coll_name, "trade_date", trade_date)
    if num_already_exists:
        LOGGER.info(f"date: {trade_date}; notes: {num_already_exists} documents already in.")
        return 
    stock_k_line_i = get_stock_k_line_from_tushare(trade_date) 

    if stock_list == 'query':
        stock_end_point_df = stock_date_endpoints()
        stock_list = stock_end_point_df[(stock_end_point_df['start_date'] <= trade_date) & (stock_end_point_df['end_date'] >= trade_date)].index.to_list() 
    elif not isinstance(stock_list, list):
        raise TypeError("stock_list should be list type object.")

    stock_k_line_i = stock_k_line_i[stock_k_line_i['ts_code'].isin(stock_list)]

    if stock_k_line_i.empty:
        LOGGER.info(f"date: {trade_date}; notes: not trade date.")
        return
    
    mongo_collection_inserter(coll_name, stock_k_line_i)
    LOGGER.info(f"date: {trade_date}; {len(stock_k_line_i)} documents downloaded.")


def stock_k_line_dowloader_batch(start_date, end_date):
    # stock_list = query_stock_code_list()

    stock_end_point_df = stock_date_endpoints()
    
    date_list = pd.date_range(start_date, end_date)
    for _date in date_list:
        stock_list = stock_end_point_df[(stock_end_point_df['start_date'] <= _date) & (stock_end_point_df['end_date'] >= _date)].index.to_list() 
        stock_k_line_dowloader(_date, stock_list=stock_list)




def stock_k_line_trimmer():
    """delete redundant stock k line due to storage limitation of mongodb atlas"""

    stock_end_point_df = stock_date_endpoints()

    for end_point in stock_end_point_df.itertuples():
        stock_code = end_point.Index
        start_date = end_point.start_date 
        end_date = end_point.end_date
        deleted_num = delete_redundant_stock_documents(stock_code, start_date, end_date)
        LOGGER.info(f"stock_code: {stock_code}; before: {start_date}; after: {end_date}; deleted_num: {deleted_num}")



if __name__ == '__main__':
    # # 批量下载k线数据
    # start_date = datetime(2010, 1, 1)
    # end_date = datetime(2024, 4, 30)
    # cb_k_line_downloader_batch(start_date, end_date)

    # # 更新cb_basic 
    # cb_basic_downloader()

    # # 下载stock k线数据
    # stock_k_line_dowloader(datetime(2024, 4, 30))

    # # 批量下载stock k线数据
    # start_date = datetime(2000, 1, 1)
    # end_date = datetime(2024, 4, 30)
    # stock_k_line_dowloader_batch(start_date, end_date)

    # 删除多余stock k线数据
    # stock_k_line_trimmer()

    # 批量下载stock k线数据
    # start_date = datetime(2020, 5, 13)
    # end_date = datetime(2024, 4, 30)
    # stock_k_line_dowloader_batch(start_date, end_date)

    # once again: pymongo.errors.OperationFailure: you are over your space quota, using 512 MB of 512 MB, full error: {'ok': 0, 'errmsg': 'you are over your space quota, using 512 MB of 512 MB', 'code': 8000, 'codeName': 'AtlasError'}
    # 2023-01-19 00:00:00; 879 documents downloaded.
    # 删除多余stock k线数据
    # stock_k_line_trimmer()
    # 删除出错
    # stock_date_endpoints()


    # drop stock_k_line后重新批量下载stock k线数据
    start_date = datetime(2007, 1, 1)
    end_date = datetime(2024, 4, 30)
    # stock_k_line_dowloader_batch(start_date, end_date)







