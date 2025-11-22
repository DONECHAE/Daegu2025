from infrastructure.opendart.api.service import opendart_disclosure_api
from setting.inject import provision_inject_orm
from infrastructure.queryFactory.TB_COMPANY.queryFactory import TBCompanyQueryFactory
from setting.database_orm import SessionLocal
from infrastructure.queryFactory.base_orm import BaseQueryFactory
from db.public.models import TB_DISCLOSURE_INFORMATION
from Logger import logger, request_context
from error.opendart.errors import OPENDART_ERROR_MESSAGES
from uuid import uuid4
from error.email.email_logger import attach_error_email_handler
from datetime import datetime, timedelta

"""
    * TB_DISCLOSURE_INFOMATION(공시정보)
        - 공시정보 정보를 DB에 저장하는 스케줄러
        - 스케줄러 주기 : 매일
"""

class SchedulerServiceTBDisclosure:
    def __init__(self, from_date: str | None = None, to_date: str | None = None, days: int = 3):
        # logger request_context내 UUID 직접 할당
        request_context.request_id = str(uuid4())
        self.provision = provision_inject_orm()
        self.openDart_api_key = self.provision.OPENDART_API_KEY5

        # 날짜 범위 옵션 (기본: 오늘 기준 3일 전 ~ 오늘)
        self.from_date = from_date  # 'YYYYMMDD' 또는 None
        self.to_date = to_date      # 'YYYYMMDD' 또는 None
        self.days = days            # from_date/to_date 미지정 시 사용할 기본 일수

        attach_error_email_handler(logger, service_name='WEB:TB_DISCLOSURE_INFOMATION 스케줄러')

    def _resolve_date_range(self) -> tuple[str, str]:
        """
        날짜 파라미터를 최종 확정한다.
        - 둘 다 None이면: 오늘 기준 (days)일 전 ~ 오늘
        - from_date만 있으면: from_date ~ 오늘
        - to_date만 있으면: (오늘 - days) ~ to_date
        - 둘 다 있으면: 그대로 사용
        반환은 ('YYYYMMDD', 'YYYYMMDD') 형태
        """
        today = datetime.today()
        if self.from_date is None and self.to_date is None:
            bgn = (today - timedelta(days=self.days)).strftime('%Y%m%d')
            end = today.strftime('%Y%m%d')
            return bgn, end
        if self.from_date and not self.to_date:
            end = today.strftime('%Y%m%d')
            return self.from_date, end
        if self.to_date and not self.from_date:
            bgn = (today - timedelta(days=self.days)).strftime('%Y%m%d')
            return bgn, self.to_date
        return self.from_date, self.to_date

    def run(self):
        logger.info("[TB_DISCLOSURE_INFOMATION : 공시검색] -----> 스케줄러 시작")

        # 검색 날짜 범위 확정 (기본: 최근 3일)
        from_date, to_date = self._resolve_date_range()
        logger.info(f"[TB_DISCLOSURE_INFOMATION : 공시검색] -----> 날짜범위: {from_date} ~ {to_date}")

        result = []
        error_codes: set[str] = set()  # ⬅ 에러코드 수집(메일 한 통만)

        with SessionLocal() as conn:
            company_query_factory = TBCompanyQueryFactory(conn)
            base_query_factory = BaseQueryFactory(conn, TB_DISCLOSURE_INFORMATION)

            corp_codes = [code[0] for code in company_query_factory.corp_code()]

            for corp_code in corp_codes:
                json_data = opendart_disclosure_api(
                    api_key=self.openDart_api_key,
                    corp_code=corp_code,
                    bgn_de=from_date,
                    end_de=to_date
                )
                status = json_data.get("status", "900")  # 기본 "900" (정의 외/예외적 상황)

                if status == "000":
                    result.extend(json_data.get("list", []))

                elif status == "013":
                    # 데이터 없음: 정보성 로그 (메일 X)
                    logger.info(f"[TB_DISCLOSURE_INFORMATION : 공시검색] -----> {OPENDART_ERROR_MESSAGES[status]} (corp={corp_code})")

                elif status == "020":
                    # 요청 한도 초과: 경고만 찍고 코드 수집 → 루프 종료(더 호출해도 의미 없음)
                    error_codes.add(status)
                    logger.warning(f"[TB_DISCLOSURE_INFORMATION : 공시검색] -----> {OPENDART_ERROR_MESSAGES[status]} (corp={corp_code})")
                    break

                else:
                    # 그 외 에러: 경고 로그 + 코드 수집 (여러 번 떠도 메일은 마지막에 한 번)
                    error_codes.add(status)
                    logger.warning(
                        f"[TB_DISCLOSURE_INFORMATION : 공시검색] -----> ERROR : "
                        f"{OPENDART_ERROR_MESSAGES.get(status, '정의되지 않은 오류')} (corp={corp_code})"
                    )

            # 접수번호(unique)를 기준 추출
            rcept_no_list = [item['rcept_no'] for item in result if 'rcept_no' in item]
            balance_sheets = base_query_factory.find_all_in("RCEPT_NO", rcept_no_list)
            rcept_rows = {row.RCEPT_NO for row in balance_sheets}

            # DB에서 조회되지 않은 정보들만 추출
            insert_data = [item for item in result if item.get('rcept_no') not in rcept_rows]

            instances = [
                TB_DISCLOSURE_INFORMATION(
                    STOCK_CODE=item.get("stock_code"),
                    CORP_CODE=item.get("corp_code"),
                    CORP_NAME=item.get("corp_name"),
                    CORP_CLS=item.get("corp_cls"),
                    REPORT_NM=item.get("report_nm"),
                    RCEPT_NO=item.get("rcept_no"),
                    FLR_NM=item.get("flr_nm"),
                    RCEPT_DT=item.get("rcept_dt"),
                    RM=item.get("rm")
                )
                for item in insert_data
            ]

            if instances:
                try:
                    base_query_factory.insert_multi_row(instances)
                    logger.info(f"[TB_DISCLOSURE_INFORMATION : 공시검색] -----> 삽입 : {len(instances)}건 적재 완료")
                except Exception as e:
                    # DB 삽입 실패는 실제 에러: 코드를 수집하고 마지막에 메일 1통
                    error_codes.add("DB_INSERT")
                    logger.warning(f"[TB_DISCLOSURE_INFORMATION : 공시검색] -----> Insert failed: {e}")

            else:
                logger.info("[TB_DISCLOSURE_INFORMATION : 공시검색] -----> 조회된 데이터가 없습니다")

        if error_codes:
            logger.error(
                f"[TB_DISCLOSURE_INFORMATION : 공시검색] -----> 에러 코드 요약: {', '.join(sorted(error_codes))}"
            )

        logger.info("[TB_DISCLOSURE_INFORMATION : 공시검색] -----> 스케줄러 종료")