from pandas import Series
import numpy as np
from datetime import datetime
import logging
#from ta.momentum import MACD

from sqlalchemy.orm import sessionmaker

import sys
sys.path.append("../config")  
from logging_config import configure_logging

sys.path.append("..")  
from models import StockUS, StockUSOHLC, StockUsMACD, Base, engine


# get setting logging info
configure_logging()

Session = sessionmaker(bind=engine)
session = Session()

## get ohlc information of tickers
def get_us_macd_of_ticker():
    
    ## get ticker list
    stocks = session.query(StockUS).all()
    for index, stock in enumerate(stocks, start=1):
        
        ############
        logging.error(f"=============== START MACD [code: {stock.stock_code}, sid: {stock.sid}, {index}/{len(stocks)}] ===============")     
        print(f"=============== START MACD [code: {stock.stock_code}, sid: {stock.sid}, {index}/{len(stocks)}] ===============")     
        
        ## get ohlc date of a ticker
        ohlc_data = session.query(StockUSOHLC).filter_by(sid=stock.sid).all()       
        
        # MACD 계산 및 저장
        short_period = 12
        long_period = 26
        signal_period = 9

        ema12 = 0
        ema26 = 0
        macd9 = 0
        
        # Insert StockUsMACD 데이터 삽입
        macd_list = []
        records = []
        for i in range(len(ohlc_data)):
            close_price = ohlc_data[i].close
    
            if i >= long_period:
                if i == long_period:
                    ema12 = sum([data.close for data in ohlc_data[i - short_period + 1:i + 1]]) / short_period
                    ema26 = sum([data.close for data in ohlc_data[i - long_period + 1:i + 1]]) / long_period
                else:
                    ema12 = (close_price * (2 / (short_period + 1))) + (ema12 * (1 - (2 / (short_period + 1))))
                    ema26 = (close_price * (2 / (long_period + 1))) + (ema26 * (1 - (2 / (long_period + 1))))
                
                macd = ema12 - ema26
                if i >= long_period + signal_period - 1:
                    macd9 = sum([data.macd for data in macd_list[i - signal_period + 1:i + 1]]) / signal_period
                    macd_list.append(macd)
                    macd_record = StockUsMACD(
                        sid=1,
                        macd9=macd9,
                        macd12=ema12,
                        macd26=ema26,
                        macd_date=ohlc_data[i].ohlc_dt
                    )
                    records.append(macd_record)
                    #session.commit()
                            
        session.bulk_save_objects(records)        
        session.commit()                

    session.close()
    
    
get_us_macd_of_ticker()