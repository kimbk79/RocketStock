import pandas as pd
import time
import numpy as np
from datetime import datetime
import logging

import yfinance as yf
from sqlalchemy.orm import sessionmaker

import sys
sys.path.append("../config")  
from logging_config import configure_logging

sys.path.append("..")  
from models import StockUS, StockUSOHLC, Base, engine


# get setting logging info
configure_logging()

Session = sessionmaker(bind=engine)
session = Session()

## get ohlc information of tickers
def get_us_ohlc_of_ticker():
    
    stocks = session.query(StockUS).all()
    for index, stock in enumerate(stocks, start=1):
        '''
        최근 조회 된 ohlc 날짜 조회
        - 조회 된 날짜가 없을 경우 초기 데이터 부터 읽어오기
        - 조회 되었을 경우 마지막 날짜 이후 부터 읽어 오기
        - 존재하면 update, 없을 경우 insert
        '''
        latest_ohlc = session.query(StockUSOHLC).filter_by(sid=stock.sid).order_by(StockUSOHLC.reg_date.desc()).first()
        
        
        # 시작일, 종료일        
        start_str = "1970-01-01"
        
        # 오늘 날짜를 얻어옵니다.
        today = datetime.now()
        end_str = today.strftime("%Y-%m-%d")
        
        index_of_space = stock.stock_code.find(' ')  # 첫 번째 공백의 인덱스 찾기
        if index_of_space != -1:
            stock.stock_code = stock.stock_code[:index_of_space]  # 공백 이전까지의 문자만 선택
            
        if latest_ohlc is not None:
            start_str = latest_ohlc.ohlc_dt            
            
        tohlcs = yf.download(stock.stock_code, start=start_str, end=end_str)
            
        logging.error(f"=============== START code: {stock.stock_code}, sid: {stock.sid}, {index}/{len(stocks)} ===============")     
        
                           
        records = []      
        is_latenty = False
        for index, row in tohlcs.iterrows():
            date = index.strftime("%Y-%m-%d")
            open_price = row['Open']
            
            row.fillna(0.0)
            logging.info(f"code: {stock.sid}, date: {date}, open_price: {open_price} close_price: {row['Close']}")   
            
            ## nan 값이 발생 할 경우
            if (row['Open'] == float('nan')) or (row['Close'] == float('nan')):
                continue                
            
            if is_latenty == False:
                exits_ohlc = session.query(StockUSOHLC).filter_by(sid=stock.sid, ohlc_dt=index.strftime("%Y-%m-%d")).order_by(StockUSOHLC.reg_date.desc()).first()

            if exits_ohlc is None:
                is_latenty = True
                                
                per_change = 0.0                
                ## open 0.0 일 경우 per 계산 오류 예외처리
                if float(row['Open']) != 0.0:                
                    per_change = (row['Close'] - row['Open']) / row['Open'] * 100
                    
                if  row['Open'] != 0.0:
                    new_record = StockUSOHLC( sid = stock.sid
                                            , ohlc_dt = index.strftime("%Y-%m-%d")
                                            , open = float(row['Open'])
                                            , close = float(row['Close'])
                                            , high = float(row['High'])
                                            , low = float(row['Low'])
                                            , volume = int(row['Volume'])
                                            , change = float(row['Close'] - row['Open'])
                                            , per_change=float(per_change) )
                    
                    records.append(new_record)
            else:
                logging.info(f"-- Already Insert {stock.sid}, {index.strftime('%Y-%m-%d')}")
                
        session.bulk_save_objects(records)        
        session.commit()        

    session.close()
    
    
get_us_ohlc_of_ticker()