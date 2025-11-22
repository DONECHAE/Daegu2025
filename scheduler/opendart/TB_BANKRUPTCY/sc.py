from setting.database_orm import SessionLocal
from Logger import logger ,request_context
from uuid import uuid4
from infrastructure.opendart.api.service import opendart_bankruptcy_api
from infrastructure.queryFactory.TB_COMPANY.queryFactory import TBCompanyQueryFactory
from db.public.models import TB_BANKRUPTCY
from setting.inject import provision_inject_orm
from datetime import datetime
from infrastructure.queryFactory.base_orm import BaseQueryFactory
from error.opendart.errors import OPENDART_ERROR_MESSAGES
from error.email.email_logger import attach_error_email_handler

"""
    * TB_BANKRUPTCY(부도발생)
        - 부도발생 정보를 DB에 저장하는 스케줄러
        - 스케줄러 주기 : 매일
"""

class SchedulerServiceTBBankruptcy:
    def __init__(self):
        # logger request_context내 UUID 직접 할당
        request_context.request_id = str(uuid4())
        self.provision = provision_inject_orm()
        self.openDart_api_key = self.provision.OPENDART_API_KEY
        attach_error_email_handler(logger, service_name='WEB:BANKRUPTCY 스케줄러')
        
    def run(self):
        logger.info("[TB_BANKRUPTCY : 부도발생] -----> 스케줄러 시작")
        result = []
        log_errors = set()

        with SessionLocal() as conn:
            company_query_factory = TBCompanyQueryFactory(conn)
            base_query_factory = BaseQueryFactory(conn, TB_BANKRUPTCY)
            corp_codes = [code[0] for code in company_query_factory.corp_code()]
            # 중복 체크
            existing_keys = {(row.CORP_CODE, row.RCEPT_NO) for row in base_query_factory.find_all()}

            for code in corp_codes:
                json_data = opendart_bankruptcy_api(api_key=self.openDart_api_key, corp_code=code)
                status = json_data.get("status", "900") # (column, default_value)
                
                if status == "000":
                    result.extend(json_data.get("list"))

                # API 요청 횟수가 초과된 경우 종료
                elif status == "020":
                    logger.error(f"[TB_BANKRUPTCY : 부도발생] -----> ERROR : {OPENDART_ERROR_MESSAGES[status]}", exc_info=True)
                    break             

                # API가 정상적으로 호출되지 않은 경우
                else:
                    msg = OPENDART_ERROR_MESSAGES.get(status, "Unknown error")

                    # 📌 여기에 이메일 발송 여부 구분 코드 삽입
                    if status in {"900", "999"}:   # 메일 보내고 싶은 에러 코드
                        logger.error(f"[TB_BANKRUPTCY : 부도발생] -----> ERROR : {msg}", exc_info=True)
                    else:
                        logger.warning(f"[TB_BANKRUPTCY : 부도발생] -----> WARNING : {msg}")
                
            instances = []

            for item in result:
                key = (item['corp_code'], item['rcept_no'])
                if key in existing_keys:
                    logger.info(f"Skipping duplicate: {key}")
                    continue

                dfd_value = item['dfd']
                
                dfd = None
                if dfd_value not in ('-', ''):
                    try:
                        dfd = datetime.strptime(dfd_value, '%Y년 %m월 %d일').date()
                    except ValueError:
                        logger.warning(f"Invalid date format for {key}: {dfd_value}")

                instance = {
                    "CORP_CODE": item['corp_code'],
                    "RCEPT_NO": item['rcept_no'],
                    "CORP_CLS": item['corp_cls'],
                    "CORP_NAME": item['corp_name'],
                    "DF_CN": item['df_cn'],
                    "DF_AMT": item['df_amt'],
                    "DF_BNK": item['df_bnk'],
                    "DFD": dfd,
                    "DF_RS": item['df_rs'],
                }

                instances.append(TB_BANKRUPTCY(**instance))

            if instances:
                try:
                    base_query_factory.insert_multi_row(instances)
                    logger.info(f"[TB_BANKRUPTCY : 부도발생] -----> 삽입 : {len(instances)}건 적재 완료")

                except Exception as e:
                    logger.error(f"[TB_BANKRUPTCY : 부도발생] -----> ERROR : Insert failed: {e}", exc_info=True)
                    raise
            else:
                logger.info("[TB_BANKRUPTCY : 부도발생] -----> 조회된 데이터가 없습니다")

        logger.info("[TB_BANKRUPTCY : 부도발생] -----> 스케줄러 종료")