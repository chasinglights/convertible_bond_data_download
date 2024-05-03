import schedule
import time as t
import logging.config
import data_downloader
from tushare_utils import set_token
from datetime import datetime, timedelta, time

logging.config.fileConfig('logging.conf', defaults={'file_name': 'schedule_logfile'})
LOGGER = logging.getLogger('data_downloader')


def initial_set_up():
    start_date = datetime(2010, 1, 1)
    end_date = datetime(2024, 4, 30)
    token = 'set your token'
    set_token(token= token)
    # 三个数据库
    data_downloader.cb_k_line_downloader_batch(start_date, end_date)  
    data_downloader.cb_basic_downloader(daily_refresh=False)
    data_downloader.stock_k_line_dowloader_batch(start_date + timedelta(days=-365 * 3), end_date)


def index_updater():
    try:
        trade_date = datetime.combine(datetime.now().date(), time(0, 0, 0))
        data_downloader.cb_k_line_downloader(trade_date)
        data_downloader.cb_basic_downloader()
        data_downloader.stock_k_line_dowloader(trade_date, stock_list='query')
        LOGGER.info(f"today's data has been successfully downloaded.")
    except Exception as e:
        LOGGER.exception(e)

def daily_schedule():

    schedule.every().day.at("10:00").do(index_updater)
    # schedule.every(1).minutes.do(index_updater)
    while True:
        schedule.run_pending()
        t.sleep(1)



if __name__ == '__main__':
    # daily_schedule()
    print('hello world')


