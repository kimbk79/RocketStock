from pandas import Series
import numpy as np
from datetime import datetime
import logging
from ta.momentum import rsi

from sqlalchemy.orm import sessionmaker

import sys
sys.path.append("../config")  
from logging_config import configure_logging

sys.path.append("..")  
from models import StockUS, StockUSOHLC, StockUsRSI, Base, engine


# get setting logging info
configure_logging()

Session = sessionmaker(bind=engine)
session = Session()

## get ohlc information of tickers
def get_us_rsi_of_ticker():
    
    ## get ticker list
    stocks = session.query(StockUS).all()
    for index, stock in enumerate(stocks, start=1):
        
        ############
        logging.error(f"=============== START RSI [code: {stock.stock_code}, sid: {stock.sid}, {index}/{len(stocks)}] ===============")     
        print(f"=============== START RSI [code: {stock.stock_code}, sid: {stock.sid}, {index}/{len(stocks)}] ===============")     
        
        ## get ohlc date of a ticker
        ohlc_data = session.query(StockUSOHLC).filter_by(sid=stock.sid).all()        
        
        # 가격 데이터 추출
        price_data = [(data.ohlc_dt, data.close) for data in ohlc_data]
        
        # compute RSI 
        price_close = [data[1] for data in price_data]
        rsi_values_7 = rsi(Series(price_close), window=7)
        rsi_values_14 = rsi(Series(price_close), window=14)
        rsi_values_24 = rsi(Series(price_close), window=24)
        
        # Insert StockUsRSI에 데이터 삽입
        records = []
        for i, data in enumerate(price_data):
            if np.isnan(rsi_values_7[i]) == False and np.isnan(rsi_values_14[i]) == False and np.isnan(rsi_values_24[i]) == False:
                #print(f"{stock.sid}, {data[0]}, {rsi_values_7[i]}, {rsi_values_14[i]}, {rsi_values_24[i]}")
                rsi_record = StockUsRSI(
                    sid=stock.sid,
                    rsi_date=data[0],
                    rsi7=float(rsi_values_7[i]),
                    rsi14=float(rsi_values_14[i]),
                    rsi24=float(rsi_values_24[i])
                )
                records.append(rsi_record)                
                
        session.bulk_save_objects(records)        
        session.commit()                

    session.close()
    
    
get_us_rsi_of_ticker()