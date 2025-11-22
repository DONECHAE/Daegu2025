from infrastructure.opendart.api.service import opendart_financial_api
from setting.inject import provision_inject_orm
from infrastructure.queryFactory.TB_COMPANY.queryFactory import TBCompanyQueryFactory
from infrastructure.queryFactory.TB_FINANCIAL_VARIABLE.queryFactory import TBFINANCIALQueryFactory
from setting.database_orm import SessionLocal
from infrastructure.queryFactory.base_orm import BaseQueryFactory
from db.public.models import *
from Logger import logger , request_context
from error.opendart.errors import OPENDART_ERROR_MESSAGES
from uuid import uuid4
from datetime import timedelta, datetime
import requests
import time
import pandas as pd
from infrastructure.opendart.financial.opendart_pre import FinancialDataProcessor
from error.email.email_logger import attach_error_email_handler

def choose_report_by_acc_mt(acc_mt: int | None, today: datetime) -> tuple[str, str]:
    try:
        if acc_mt is None:
            acc = 12
        elif isinstance(acc_mt, str):
            acc = int(acc_mt.strip() or 12)
        else:
            acc = int(acc_mt)
    except Exception:
        acc = 12
    if acc < 1 or acc > 12:
        acc = 12
    month = getattr(today, "month", None) or datetime.today().month
    rel = ((month - acc - 1) % 12) + 1
    if month > acc:
        year_of_last_fy_end = today.year
        year_of_current_fy_end = today.year + 1
    else:
        year_of_last_fy_end = today.year - 1
        year_of_current_fy_end = today.year
    if 1 <= rel <= 3:
        return "11011", str(year_of_last_fy_end)
    elif 4 <= rel <= 6:
        return "11013", str(year_of_current_fy_end)
    elif 7 <= rel <= 9:
        return "11012", str(year_of_current_fy_end)
    else:
        return "11014", str(year_of_current_fy_end)
    
class SchedulerServiceTBFinancialOfs:
    def __init__(self, manual_year: str | None = None, manual_quarter: str | None = None):
        request_context.request_id = str(uuid4())
        self.provision = provision_inject_orm()
        self.openDart_api_key = self.provision.OPENDART_API_KEY3
        self.reprt_codes = ["11011", "11012", "11013", "11014"]
        attach_error_email_handler(logger, service_name='WEB:FINANCIAL_STATES 스케줄러')
        self.manual_year = manual_year
        self.manual_quarter = manual_quarter

    def run(self):
        logger.info("[TB_FINANCIAL_STATEMENTS_OFS] -----> 스케줄러 시작")
        error_codes = set()

        with SessionLocal() as conn:
            company_query_factory = TBCompanyQueryFactory(conn)
            base_query_factory = BaseQueryFactory(conn, TB_FINANCIAL_STATEMENTS)
            corp_codes = [(code, acc_mt) for code, acc_mt in company_query_factory.corp_code()]
            rcept_no_list = set(r[0] for r in conn.query(TB_FINANCIAL_STATEMENTS.RCEPT_NO).distinct().all())

            for corp_code, acc_mt in corp_codes:
                if self.manual_year and self.manual_quarter:
                    reprt_code = self.manual_quarter
                    bsns_year = self.manual_year
                else:
                    reprt_code, bsns_year = choose_report_by_acc_mt(acc_mt, today=datetime.today())
                logger.info(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> 보고서코드: {reprt_code}, 연도: {bsns_year}")

                result = []

                for attempt in range(3):
                    try:
                        json_data = opendart_financial_api(
                            api_key=self.openDart_api_key,
                            corp_code=corp_code,
                            bsns_year=bsns_year,
                            reprt_code=reprt_code,
                            fs_div="OFS"
                        )
                        status = json_data.get("status", "900")

                        if status == "000":
                            rcept_no = json_data.get("list")[0]["rcept_no"]

                            if rcept_no in rcept_no_list:
                                break  # 이미 존재 → skip

                            result.extend(json_data.get("list"))
                            break 

                        elif status == "013":
                            logger.info(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> {OPENDART_ERROR_MESSAGES[status]}")
                            break

                        elif status == "020":
                            error_codes.add(status)
                            logger.warning(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> ERROR : {OPENDART_ERROR_MESSAGES[status]}")
                            if self.openDart_api_key == self.provision.OPENDART_API_KEY3:
                                self.openDart_api_key = self.provision.OPENDART_API_KEY4
                                time.sleep(5)
                                continue  # 예비키로 재시도
                            else:
                                error_codes.add(status)
                                logger.warning("[TB_FINANCIAL_STATEMENTS_OFS] -----> 예비 키도 한도 초과, 스킵")
                                break

                    except requests.exceptions.RequestException:
                        logger.info(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> 재시도 {attempt+1}회 실패")
                        if attempt < 3:
                            time.sleep(60)
                        else:
                            
                            logger.warning(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> ERROR : {corp_code} 3회 시도 후 실패")

                # ========== 1) 원본 테이블 삽입 (회사별) ==========
                instances = [
                    TB_FINANCIAL_STATEMENTS(
                        RCEPT_NO=row.get("rcept_no"),
                        REPRT_CODE=row.get("reprt_code"),
                        BSNS_YEAR=row.get("bsns_year"),
                        CORP_CODE=row.get("corp_code"),
                        SJ_DIV=row.get("sj_div"),
                        SJ_NM=row.get("sj_nm"),
                        ACCOUNT_ID=row.get("account_id"),
                        ACCOUNT_NM=row.get("account_nm"),
                        ACCOUNT_DETAIL=row.get("account_detail"),
                        THSTRM_NM=row.get("thstrm_nm"),
                        THSTRM_AMOUNT=row.get("thstrm_amount"),
                        FRMTRM_NM=row.get("frmtrm_nm"),
                        FRMTRM_AMOUNT=row.get("frmtrm_amount"),
                        BFEFRMTRM_NM=row.get("bfefrmtrm_nm"),
                        BFEFRMTRM_AMOUNT=row.get("bfefrmtrm_amount"),
                        ORD=row.get("ord"),
                        CURRENCY=row.get("currency"),
                        FS_DIV="OFS",
                    )
                    for row in result
                ]

                if instances:
                    try:
                        base_query_factory.insert_multi_row(instances)
                        logger.info(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> {OPENDART_ERROR_MESSAGES['000']} (삽입 {len(instances)}건)")
                        # 중복 방지를 위해 rcept_no_list 업데이트
                        for inst in instances:
                            if inst.RCEPT_NO:
                                rcept_no_list.add(inst.RCEPT_NO)
                    except Exception as e:
                        logger.warning(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> ERROR : Insert failed: {e}")
                        raise
                else:
                    logger.info(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> {OPENDART_ERROR_MESSAGES['013']}")

                # ========== 2) FinancialDataProcessor 가공 파이프라인 (회사별) ==========
                if result:
                    # 이미 적재된 변수 테이블의 (RCEPT_NO, ACCOUNT_NM) 키를 메모리 캐시에 올림
                    factory_var = BaseQueryFactory(conn, TB_FINANCIAL_VARIABLE)
                    try:
                        existing_pairs = set(
                            (r[0], r[1])
                            for r in conn.query(
                                TB_FINANCIAL_VARIABLE.RCEPT_NO,
                                TB_FINANCIAL_VARIABLE.ACCOUNT_NM
                            ).distinct().all()
                        )
                    except Exception:
                        existing_pairs = set()

                    fs_data = pd.DataFrame(result)
                    fs_data.columns = fs_data.columns.str.upper()
                    if "FS_DIV" not in fs_data.columns:
                        fs_data["FS_DIV"] = "OFS"
                    processor = FinancialDataProcessor(fs_data)

                    df = processor.apply_keyword_mapping()
                    df = processor.fill_missing_accounts(df)
                    df = processor.keep_latest_rcept_by_account(df)
                    df = processor.deduplicate_by_std_account(df)
                    df = processor.clean_amount_zero_if_ord_exists(df)

                    df = processor.merge_with_company_info(df)

                    # 공시 플래그
                    factory_disclosure = TBFINANCIALQueryFactory(conn, TB_DISCLOSURE_INFORMATION)
                    df = processor.add_disclosure_flags_as_rows(df, factory_disclosure)

                    # 시가총액
                    krx_factory = TBFINANCIALQueryFactory(conn, TB_KRX)
                    years = sorted(map(int, df['BSNS_YEAR'].dropna().unique().tolist()))
                    data = krx_factory.get_krx_marketcap_data(years=years)
                    df = processor.append_marketcap_fast(data, df)

                    # LLM 플래그 → 평균자기자본 → DB 스키마
                    df = processor.mark_note_extraction_target(df)
                    print("===== BEFORE add_avg_equity =====")
                    print(df.head(10).to_dict(orient="records"))  # 샘플 행 출력
                    print(df.dtypes)  # 컬럼별 dtype 확인

                    df = processor.add_avg_equity(df)
                    final_df = processor.format_for_database(df)

                    if not final_df.empty:
                        # 1) 배치 내 중복 제거 (RCEPT_NO, ACCOUNT_NM 기준)
                        final_df = final_df.drop_duplicates(subset=["RCEPT_NO", "ACCOUNT_NM"], keep="last").reset_index(drop=True)

                        # 2) DB에 이미 있는 (RCEPT_NO, ACCOUNT_NM) 제외
                        mask_new = ~final_df.apply(lambda r: (r["RCEPT_NO"], r["ACCOUNT_NM"]) in existing_pairs, axis=1)
                        final_df_to_insert = final_df.loc[mask_new].fillna({"ACCOUNT_AMOUNT": 0}).reset_index(drop=True)

                        if not final_df_to_insert.empty:
                            var_instances = [
                                TB_FINANCIAL_VARIABLE(
                                    CORP_CODE=row["CORP_CODE"],
                                    RCEPT_NO=row["RCEPT_NO"],
                                    REPRT_CODE=row["REPRT_CODE"],
                                    BSNS_YEAR=row["BSNS_YEAR"],
                                    ACCOUNT_NM=row["ACCOUNT_NM"],
                                    ACCOUNT_AMOUNT=str(row["ACCOUNT_AMOUNT"]),
                                    IS_LLM=bool(row.get("IS_LLM", False)),
                                    IS_COMPLETE=bool(row.get("IS_COMPLETE", False)),
                                )
                                for _, row in final_df_to_insert.iterrows()
                            ]

                            if var_instances:
                                factory_var.insert_multi_row(var_instances)
                                logger.info(f"[TB_FINANCIAL_VARIABLE] -----> 삽입 : {len(var_instances)}건 적재 완료")

                                for _, row in final_df_to_insert.iterrows():
                                    existing_pairs.add((row["RCEPT_NO"], row["ACCOUNT_NM"]))
                        else:
                            logger.info("[TB_FINANCIAL_VARIABLE] -----> 신규 삽입 대상 없음")
                    else:
                        logger.info(f"[TB_FINANCIAL_VARIABLE] -----> 가공 파이프라인 결과 없음")

                result = []
        if error_codes:
            logger.error(f"[TB_FINANCIAL_STATEMENTS_OFS] -----> 에러 코드 요약: {', '.join(sorted(error_codes))}")
        logger.info("[TB_FINANCIAL_STATEMENTS] -----> 스케줄러 종료")