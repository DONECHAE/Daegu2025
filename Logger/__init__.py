# Logger/__init__.py
import logging
import threading
from datetime import datetime, timezone, timedelta

# 요청마다 UUID를 저장할 쓰레드 로컬
request_context = threading.local()

# UUID 필터 정의
class UUIDFilter(logging.Filter):
    def filter(self, record):
        record.request_id = getattr(request_context, "request_id", "NO-UUID")
        return True
    
# KST 기준 시간 컨버터 정의
def kst_time(*args):
    kst_tz = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst_tz)
    return now_kst.timetuple()

# 커스텀 Formatter 정의(request_id 필드가 없을 경우 NO-UUID로 대체)
class OptionalRequestIDFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.converter = kst_time  # 시간 변환기를 KST로 설정
        
    def format(self, record):
        if not hasattr(record, "request_id"):
            record.request_id = "NO-UUID"
        return super().format(record)

# 커스텀 Formatter 생성
formatter = OptionalRequestIDFormatter(
    fmt='[%(request_id)s] %(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 공통 필터
uuid_filter = UUIDFilter()

def setup_logger(name: str, file: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 파일 핸들러
    file_handler = logging.FileHandler(file)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(uuid_filter)

    # 콘솔 핸들러
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(uuid_filter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

# File Handler
file_handler = logging.FileHandler("application.log")
file_handler.setFormatter(formatter)

# Stream Handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

# 루트 로거 가져오기 및 레벨 설정
Logger = logging.getLogger()
Logger.setLevel(logging.INFO)

# # 핸들러 등록
Logger.addHandler(file_handler)
Logger.addHandler(stream_handler)

# UUIDFilter 인스턴스 생성 및 루트 로거에 추가
uuid_filter = UUIDFilter()
Logger.addFilter(uuid_filter)

# httpx / apscheduler 로거에도 동일한 필터 적용
logging.getLogger("httpx").addFilter(uuid_filter)
logging.getLogger("apscheduler").addFilter(uuid_filter)

logger = logging.getLogger()  # 루트 로거 별칭
# request_context는 그대로 외부에서 쓰게 노출
__all__ = ["logger", "request_context"]