from setting.database_orm import SessionLocal
from Logger import logger ,request_context
from uuid import uuid4
from db.public.models import TB_COMPANY
from infrastructure.queryFactory.base_orm import BaseQueryFactory
from error.opendart.errors import OPENDART_ERROR_MESSAGES
from setting.inject import provision_inject_orm
from infrastructure.opendart.api.service import opendart_corp_code, opendart_company_api
import pandas as pd
import re
from error.email.email_logger import attach_error_email_handler

"""
    * TB_COMPANY(기업개황)
        - 기업개황 정보를 DB에 저장하는 스케줄러
        - 스케줄러 주기: 매일
        - 금융업, 공사, 스팩 기업의 경우 지표산출시 사용하지 않는 기업목록으로 IS_CALCULATE 칼럼의 값을 FALSE로 관리
        - 지표산출시 제외해야할 종목코드 정리    
            * 661 : 금융업 지원 서비스업
            * 642 : 신탁업 및 집합 투자업
            * 641 : 은행 및 저축기관
            * 649 : 기타 금융업, 64992(지주회사는 해당되지 않음)
            * 651 : 보험업
"""


class SchedulerServiceTBCompany:
    def __init__(self):
        # logger request_context내 UUID 직접 할당
        request_context.request_id = str(uuid4())
        self.provision = provision_inject_orm()
        attach_error_email_handler(logger, service_name='WEB:TB_COMPANY 스케줄러')
    def run(self):
        logger.info("[TB_COMPANY : 기업정보] -----> 스케줄러 시작")
        log_errors = set()

        # 고유번호 조회 API
        corpCodes = opendart_corp_code(self.provision.OPENDART_API_KEY)

        # 기업 개황 조회 API
        company_df = []
        for code in corpCodes.corp_code:
            result = opendart_company_api(self.provision.OPENDART_API_KEY, code)
            status = result['status']

            if result['status'] == '000':
                company_df.append(result)

            # API KEY 최대 요청 횟수 넘을시 중단
            elif result['status'] == '020':
                logger.error(f"[TB_COMPANY : 기업정보] -----> ERROR : {OPENDART_ERROR_MESSAGES[status]}", exc_info=True)
                break
            # 동일한 에러가 중복으로 기록되는 것을 방지
            else:
                if status not in log_errors:
                    logger.error(f"[TB_COMPANY : 기업정보] -----> ERROR : {OPENDART_ERROR_MESSAGES[status]}", exc_info=True)
                    log_errors.add(status)

        company_df = pd.DataFrame(company_df)

        with SessionLocal() as conn:
            base_query_factory = BaseQueryFactory(conn, TB_COMPANY)
            balance_sheets = base_query_factory.find_all()
            company_tb = pd.DataFrame([row.__dict__ for row in balance_sheets])

            diff = set(company_df.corp_code) - set(company_tb.CORP_CODE) 

            insert_df = company_df.loc[company_df['corp_code'].isin(diff)].reset_index(drop=True)

            # 지표산출시 제외해야할 산업코드 정의
            induty_codeList = r'^(661|642|641|649(?!92)|651)'

            instances = []
            for _, row in insert_df.iterrows():

                is_calculate = True
                # 기업 이름에 "공사"가 포함되는 경우 지표산출시 해당 기업 제외
                if re.match(induty_codeList, row.induty_code) or row.corp_name.endswith("공사") or "금융" in row.corp_name:
                    is_calculate = False

                instance = {
                "STOCK_CODE": row['stock_code'],
                "CORP_CODE": row['corp_code'],
                "CORP_NAME": row['corp_name'],
                "CORP_NAME_ENG": row['corp_name_eng'],
                "CORP_CLS": row['corp_cls'],
                "CEO_NM": row['ceo_nm'],    
                "JURIR_NO": row['jurir_no'],
                "BIZR_NO": row['bizr_no'],  
                "ADRES": row['adres'],
                "PHN_NO": row['phn_no'],
                "INDUTY_CODE": row['induty_code'],
                "EST_DT": row['est_dt'],
                "ACC_MT": row['acc_mt'],
                "IS_ACTIVE": True,
                "IS_CALCULATE" : is_calculate
                }

                instances.append(TB_COMPANY(**instance))

            if instances:
                try:
                    base_query_factory.insert_multi_row(instances)
                    logger.info(f"[TB_COMPANY : 기업정보] -----> 삽입 : {len(instances)}건 적재 완료")
                except Exception as e:
                    logger.error(f"[TB_COMPANY : 기업정보] -----> ERROR : Insert failed: {e}", exc_info=True)
                    raise
            else:
                logger.info("[TB_COMPANY : 기업정보] -----> 조회된 데이터가 없습니다.")

        logger.info("[TB_COMPANY : 기업정보] -----> 스케줄러 종료")      
