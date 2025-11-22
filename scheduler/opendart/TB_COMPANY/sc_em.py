
import time
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple

from infrastructure.opendart.api.service import dart_report
from error.opendart.errors import OPENDART_ERROR_MESSAGES

from infrastructure.queryFactory.base_orm import BaseQueryFactory
from infrastructure.queryFactory.TB_COMPANY.queryFactory import TBCompanyQueryFactory
from setting.database_orm import SessionLocal
from setting.inject import provision_inject_orm
from db.public.models import TB_COMPANY
from Logger import logger 
from error.email.email_logger import attach_error_email_handler
"""
    * TB_COMPANY(직원수)
        - OpenDART 보고서에서 '직원' 키워드 데이터를 조회하여 TB_COMPANY.EMPLOYEE 업데이트
        - 스케줄러 주기: 
"""

class SchedulerServiceTBCompanyEmployee:
    def __init__(self, target_year: Optional[int] = None):
        self.provision = provision_inject_orm()
        self.api_key = self.provision.OPENDART_API_KEY2

        self.year = target_year or datetime.today().year
        self.month = datetime.today().month
        self.reprt_code = self._decide_reprt_code_for_month(self.month)
        attach_error_email_handler(logger, service_name='WEB:TB_COMPANY_EMPLOYEE 스케줄러')
        
    def _decide_reprt_code_for_month(self, month: int) -> str:
        if 1 <= month <= 3:
            return "11011"  
        if 4 <= month <= 6:
            return "11013"  
        if 7 <= month <= 9:
            return "11012"  
        return "11014"      

    def _previous_report(self, year: int, reprt_code: str) -> Tuple[int, str]:
        if reprt_code == "11013":      
            return year - 1, "11011"
        elif reprt_code == "11012":    
            return year, "11013"
        elif reprt_code == "11014":    
            return year, "11012"
        elif reprt_code == "11011":    
            return year, "11014"
        return year, reprt_code

    def _parse_sm_sum(self, df: pd.DataFrame) -> Optional[int]:
        if df is None or df.empty or "sm" not in df.columns:
            return None

        values = []
        for v in df["sm"]:
            try:
                values.append(int(str(v).replace(",", "")))
            except Exception:
                continue

        if not values:
            return None

        if len(values) >= 3:
            last_two = sum(values[-2:])
            rest = sum(values[:-2])
            return last_two if last_two == rest else sum(values)
        return sum(values)

    def _fetch_active_corp_codes(self):
        with SessionLocal() as session:
            factory = TBCompanyQueryFactory(conn=session)
            rows = factory.corp_code() or []
            return [r[0] for r in rows]

    def _process_one_corp(self, corp_code: str, year: int, reprt_code: str, retry: bool = False):
        try:
            jo = dart_report(
                api_key=self.api_key,
                corp_code=corp_code,
                key_word="직원",
                bsns_year=str(year),
                reprt_code=reprt_code,
            )

            status = str(jo.get("status", ""))
            if status != "000" or "list" not in jo:
                msg = OPENDART_ERROR_MESSAGES.get(status, jo.get("message", "Unknown error"))
                logger.info(f"[EMP] {corp_code} {year}-{reprt_code} 데이터 없음/에러: {msg} (status={status})")
                return "NO_DATA"

            df = pd.DataFrame(jo["list"])
            if df.empty:
                logger.info(f"[EMP] {corp_code} {year}-{reprt_code} 데이터 없음")
                return "NO_DATA"

            emp_val = self._parse_sm_sum(df)
            if emp_val is None:
                logger.warning(f"[EMP] {corp_code} {year}-{reprt_code} sm 파싱 실패")
                return "INVALID_VALUE"

            with SessionLocal() as session:
                factory = BaseQueryFactory(conn=session, model=TB_COMPANY)
                company = factory.find_one(CORP_CODE=corp_code)

                if company is None:
                    factory.insert_single_row(CORP_CODE=corp_code, EMPLOYEE=emp_val)
                    logger.info(f"[EMP] [{corp_code}][{year}] 신규 삽입: {emp_val}명")
                else:
                    if company.EMPLOYEE in [None, "", "-"]:
                        factory.update(company, EMPLOYEE=emp_val)
                        logger.info(f"[EMP] [{corp_code}][{year}] 업데이트(무효값→{emp_val})")
                    else:
                        try:
                            old_val = int(str(company.EMPLOYEE).replace(",", ""))
                        except Exception:
                            old_val = None

                        if old_val is None or old_val != emp_val:
                            factory.update(company, EMPLOYEE=emp_val)
                            logger.info(f"[EMP] [{corp_code}][{year}] 변경: {old_val} → {emp_val}")
                        else:
                            logger.info(f"[EMP] {corp_code} 직원 수 동일 → 건너뜀")
                            return "SKIP"

                if (emp_val in [None, 0]) and (not retry):
                    py, pr = self._previous_report(year, reprt_code)
                    logger.info(f"[EMP] {corp_code} {year}-{reprt_code} 유효치 미확보 → 이전 분기 재시도: {py}-{pr}")
                    return self._process_one_corp(corp_code, py, pr, retry=True)

            return "SUCCESS"

        except Exception as e:
            err_code = getattr(e, "code", None)
            if err_code is not None and str(err_code) in OPENDART_ERROR_MESSAGES:
                err_msg = OPENDART_ERROR_MESSAGES[str(err_code)]
                logger.error(f"[EMP] {corp_code} {year}-{reprt_code} 조회 실패: {err_msg} (code={err_code})", exc_info=True)
            else:
                logger.error(f"[EMP] {corp_code} {year}-{reprt_code} 조회 실패: {e}", exc_info=True)

            if not retry:
                try:
                    py, pr = self._previous_report(year, reprt_code)
                    logger.info(f"[EMP] {corp_code} 예외 → 이전 분기 재시도: {py}-{pr}")
                    return self._process_one_corp(corp_code, py, pr, retry=True)
                except Exception as e2:
                    logger.error(f"[EMP] {corp_code} 이전 분기 재시도 실패: {e2}", exc_info=True)
            return "FAIL"

    def run(self, throttle_every: int = 1000, sleep_sec: int = 60, call_cap: int = 20000):
        logger.info(f"[EMP] 스케줄러 시작: year={self.year}, month={self.month}, reprt_code={self.reprt_code}")

        corp_codes = self._fetch_active_corp_codes()
        logger.info(f"[EMP] 대상 기업 수: {len(corp_codes)}")

        total_calls = 0
        for corp in corp_codes:
            if total_calls >= call_cap:
                logger.error(f"[EMP] API 호출 상한({call_cap}) 도달 → 중단", exc_info=True)
                break

            _ = self._process_one_corp(corp, self.year, self.reprt_code)
            total_calls += 1

            if throttle_every and (total_calls % throttle_every == 0):
                logger.info(f"[EMP] {throttle_every}건 처리 완료 → {sleep_sec}s 대기")
                time.sleep(sleep_sec)

        logger.info("[EMP] 스케줄러 완료")