import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

def configure_logging():
    # 로그 설정
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # TimedRotatingFileHandler를 사용하여 날짜별로 로그 파일을 생성하고 관리
    log_file = "app.log"
    handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8")
    handler.suffix = "%Y%m%d"
    handler.setFormatter(log_formatter)
    
    # 콘솔 핸들러 추가
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.ERROR)
    root_logger.addHandler(handler)
    root_logger.addHandler(console_handler)
