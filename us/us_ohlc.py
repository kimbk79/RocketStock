import pandas as pd
import time
import numpy as np
from datetime import datetime
import logging
import sys

import yfinance as yf
from sqlalchemy.orm import sessionmaker

sys.path.append("..")  # 상위 폴더 추가
from models import StockUS, StockUSOHLC, Base, engine

Session = sessionmaker(bind=engine)
session = Session()

def get_us_stock_list():
    stocks = session.query(StockUS).all()
    for stock in stocks:
        '''
        최근 조회 된 ohlc 날짜 조회
        - 조회 된 날짜가 없을 경우 초기 데이터 부터 읽어오기
        - 조회 되었을 경우 마지막 날짜 이후 부터 읽어 오기
        - 존재하면 update, 없을 경우 insert
        '''
        latest_ohlc = session.query(StockUSOHLC).filter_by(sid=stock.sid).order_by(StockUSOHLC.reg_date.desc()).first()
        
        
        # 시작일, 종료일        
        start_str = "1980-01-01"
        
        # 오늘 날짜를 얻어옵니다.
        today = datetime.now()
        end_str = today.strftime("%Y-%m-%d")
        
        if latest_ohlc is None:
            start_str = "1980-01-01"
            tohlcs = yf.download(stock.stock_code, start=start_str, end=end_str)
            
            print(f"========================================>>> STARTStock Code: {stock.stock_code}")            
            
            records = []            
            for index, row in tohlcs.iterrows():
                date = index.strftime("%Y-%m-%d")
                open_price = row['Open']
                print(f"Stock Code: {stock.sid}, date: {date}, open_price: {open_price}")
                
                
                exits_ohlc = session.query(StockUSOHLC).filter_by(sid=stock.sid, ohlc_dt=index.strftime("%Y-%m-%d")).order_by(StockUSOHLC.reg_date.desc()).first()

                if exits_ohlc is None:
                    
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
                        #session.add(new_record)
                        #session.commit()
                        records.append(new_record)
                else:
                    print(f"-------------- already insert {stock.sid}, {index.strftime('%Y-%m-%d')}")
            
            session.bulk_save_objects(records)
            session.commit()

get_us_stock_list()