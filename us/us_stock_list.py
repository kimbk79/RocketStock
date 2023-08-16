import FinanceDataReader as fdr
import pandas as pd
import schedule
import time
import numpy as np

import logging

from sqlalchemy.orm import sessionmaker

import sys
sys.path.append("../config")  # 상위 폴더 추가
from logging_config import configure_logging

sys.path.append("..")  # 상위 폴더 추가
from models import StockUS, StockId, Base, engine

# get setting logging info
configure_logging()

Session = sessionmaker(bind=engine)
session = Session()


## get us sockets
def get_lately_us_stock_list(market):
    try:
        logging.info(f"주식 종목 리스트 조회 : {market}")
        # 미국 주식 시장 종목 리스트 조회
        us_stock_list = fdr.StockListing(market)
        
        return us_stock_list
            
    except Exception as e:
        logging.error(f"us 종목 조회 실패: {e}")
        return None


def us_stock_porcee():
    """_summary_
    미국 주식 데이터 조회 및 조회 데이터 DB 저장
    """
    logging.info(f"START 미국 주식 종목 리스트 업데이트")
    uslist = get_lately_us_stock_list('NYSE')    
    db_insert_us_stock_list(uslist=uslist, market=21)

    uslist = get_lately_us_stock_list('NASDAQ')
    db_insert_us_stock_list(uslist=uslist, market=22)
    
    logging.info(f"END 미국 주식 종목 리스트 업데이트")


def db_insert_us_stock_list(uslist, market):    
    """_summary_
    최신 데이터 Insert
    Args:
        uslist (_type_): _description_
        market (_type_): _description_
    """
    for index, row in uslist.iterrows():       
        time.sleep(0.01)
        
        session = Session()
        
        try:
            new_sid = insert_stock_id(session=session, stock_code=row['Symbol'], country=market/10)
            
            industryCode = ''
            industry = ''
            if pd.isna(row['Industry']) == False:
                industry = row['Industry']

            if pd.isna(row['IndustryCode']) == False:
                industryCode = row['IndustryCode']

            if new_sid != None:
                logging.info(f"add stock info ------- {new_sid}, {row['Symbol']}, {row['Name']}, {row['Industry']}, {row['IndustryCode']}")
                insert_stock_us( session=session,
                                sid=new_sid,
                                stock_code=row['Symbol'],  
                                stock_name=row['Name'], 
                                industry=industry, 
                                industry_code=industryCode, 
                                market=21)   
                
        except Exception as e:
            logging.error(f"error db_insert_us_stock_list {e}")
            session.rollback()
        
        session.close()
            
    

def insert_stock_id(session, stock_code, country, is_service=1, del_date=None, up_date=None):
    
    tstock_code = session.query(StockId).filter_by(stock_code=stock_code).first()
    if tstock_code is None:
        stock_id = StockId(stock_code=stock_code, country=country, is_service=is_service, del_date=del_date, up_date=up_date)
        session.add(stock_id)
        session.commit()
        new_sid = stock_id.sid                
        return new_sid
    else:        
        return None

def insert_stock_us(session, sid, stock_code, stock_name, industry=None, industry_code=None, market=None, reg_date=None, del_date=None, up_date=None):
    stock_us = StockUS(
        sid=sid,
        stock_code=stock_code,
        stock_name=stock_name,
        industry=industry,
        industry_code=industry_code,
        market=market,
        reg_date=reg_date,
        del_date=del_date,
        up_date=up_date
    )
    session.add(stock_us)
    session.commit()
    

us_stock_porcee()

## 종목 기존 종목과 비교해서 INSERT, UPDATE, DELETE
 ## insert 시 stock_id 생성

## DB GET 종목 상세 정보가 없는 종목 리스트  

## 종목 상세 정보 조회

## 

