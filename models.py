from sqlalchemy import create_engine, Column, Integer, String, DateTime, SmallInteger, func, BigInteger, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from db_config_info import db_config

engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
Session = sessionmaker(bind=engine)
Base = declarative_base()

class StockId(Base):
    __tablename__ = 'tb_stock_id'

    sid = Column(BigInteger, primary_key=True, autoincrement=True)
    stock_code = Column(String(10), nullable=False)
    country = Column(SmallInteger, nullable=False)
    is_service = Column(SmallInteger, default=1)
    del_date = Column(DateTime)
    reg_date = Column(DateTime, server_default=func.now())
    up_date = Column(DateTime)
    
  
class StockUS(Base):
    __tablename__ = 'tb_stock_us'

    sid = Column(BigInteger, primary_key=True, unique=True, nullable=False)
    stock_code = Column(String(7), nullable=False)
    stock_name = Column(String(100), nullable=False)
    industry = Column(String(100))
    industry_code = Column(String(100))
    market = Column(SmallInteger, nullable=False)
    reg_date = Column(DateTime, server_default=func.now())
    del_date = Column(DateTime)
    up_date = Column(DateTime)

    
class StockUSOHLC(Base):
    __tablename__ = 'tb_stock_us_ohlc'

    ohlcid = Column(BigInteger, primary_key=True, autoincrement=True)
    sid = Column(BigInteger, nullable=False)
    ohlc_dt = Column(String(14))
    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(BigInteger)
    change = Column(Float)
    per_change = Column(Float)
    reg_date = Column(DateTime, server_default=func.now())