import os
import pandas as pd
import tushare as ts
from datetime import datetime, timedelta


def set_token(token):

    ts.set_token(token)


def get_cb_k_line_from_tushare(trade_date=datetime.now()):
    # 从两个数据库获取数据
    pro = ts.pro_api()
    df = pro.cb_daily(trade_date=trade_date.strftime('%Y%m%d'), fields=[
        "ts_code",
        "trade_date",
        "pre_close",
        "open",
        "high",
        "low",
        "close",
        "change",
        "pct_chg",
        "vol",
        "amount",
        "bond_value",
        "bond_over_rate",
        "cb_value",
        "cb_over_rate"])
    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    return df


def get_cb_basic_from_tushare():
    pro = ts.pro_api()
    df = pro.cb_basic(fields=[
    "ts_code",
    "bond_full_name",
    "bond_short_name",
    "cb_code",
    "stk_code",
    "stk_short_name",
    "maturity",
    "par",
    "issue_price",
    "issue_size",
    "remain_size",
    "value_date",
    "maturity_date",
    "rate_type",
    "coupon_rate",
    "add_rate",
    "pay_per_year",
    "list_date",
    "delist_date",
    "exchange",
    "conv_start_date",
    "conv_end_date",
    "conv_stop_date",
    "first_conv_price",
    "conv_price",
    "rate_clause",
    "put_clause",
    "maturity_put_price",
    "call_clause",
    "reset_clause",
    "conv_clause",
    "guarantor",
    "guarantee_type",
    "issue_rating",
    "newest_rating",
    "rating_comp"
])
    return df

def get_stock_k_line_from_tushare(trade_date=datetime.now()):

    pro = ts.pro_api()
    str_date = trade_date.strftime('%Y%m%d')
    stock_k_line_i = pro.daily(trade_date=str_date)

    adj_factor_df = pro.query('adj_factor',  trade_date=str_date)

    stock_k_line_i = stock_k_line_i.merge(adj_factor_df.drop('trade_date', axis=1), on='ts_code')
    stock_k_line_i['trade_date'] = pd.to_datetime(stock_k_line_i['trade_date'], format='%Y%m%d')
    return stock_k_line_i

if __name__ == '__main__':
    _date = datetime.now() + timedelta(days=-3)
    a = get_stock_k_line_from_tushare(_date)
    # print(a)
    # a = get_cb_basic_from_tushare()
    # a = a.convert_dtypes()

    print(a)
    print(a.dtypes)
    # a.to_excel("cb_basic.xlsx")






