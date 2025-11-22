import json
import re
from typing import List, Optional, Tuple

import pandas as pd
import re
from sqlalchemy import literal, cast, String
from Logger import logger
from infrastructure.queryFactory.base_orm import BaseQueryFactory
from setting.database_orm import SessionLocal
from db.public.models import TB_COMPANY, TB_FINANCIAL_VARIABLE, TB_FINANCIAL_STATEMENTS
from error.email.email_logger import attach_error_email_handler

class FinancialDataProcessor:
    def __init__(
        self,
        df: pd.DataFrame,
        keyword_map_path: str = "/app/infrastructure/opendart/financial/map/keyword.json",
        sj_map_path: str = "/app/infrastructure/opendart/financial/map/sj_nm.json",
    ):
        """
        df: OpenDART 원본(또는 1차 정제) DataFrame
        keyword_map_path: 표준계정명 ← 후보 계정명 리스트 매핑 파일
        sj_map_path: 표준계정명 ← 허용 SJ_NM 리스트 매핑 파일
        """
        self.df = df.copy()
        self.keyword_map_path = keyword_map_path
        self.sj_map_path = sj_map_path
        self.keyword_map = self._load_json(self.keyword_map_path)
        self.sj_map = self._load_json(self.sj_map_path)

        # 사업/분기 공통으로 “있어야 하는” 주요 계정
        self.full_reports_accounts = [
            "자본총계", "영업활동현금흐름", "투자활동현금흐름", "매출액", "당기순이익",
                "총차입금(단일)"
        ]
        # 사업보고서(11011)에서만 사용하는 항목
        self.business_report_only_accounts = [
            kw for kw in self.keyword_map.keys() if kw not in self.full_reports_accounts
        ]
        attach_error_email_handler(logger, service_name='WEB:FINANCIAL_VARIABLE 스케줄러')
    def _load_json(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
        
    def _clean_korean(self, text):
        if pd.isnull(text):
            return ""
        return re.sub(r"[^가-힣]", "", str(text))

    #  표준계정 매핑
    def apply_keyword_mapping(self) -> pd.DataFrame:
        """
        ACCOUNT_NM을 한글만 남기고 → keyword_map + sj_map으로 '표준계정명' 지정
        - 일부 항목은 사업보고서(11011)만 사용
        """
        use_cols = [
            "CORP_CODE", "RCEPT_NO", "REPRT_CODE", "BSNS_YEAR",
            "SJ_NM", "ACCOUNT_ID", "ACCOUNT_NM", "THSTRM_AMOUNT", "FS_DIV", "ORD"
        ]
        self.df = self.df.loc[:, [c for c in use_cols if c in self.df.columns]].copy()


        self.df["ACCOUNT_NM"] = self.df["ACCOUNT_NM"].apply(self._clean_korean)

        matched_rows: List[pd.DataFrame] = []
        for keyword, keyword_list in self.keyword_map.items():
            if keyword in {"총차입금"}:
                continue

            valid_sj_list = self.sj_map.get(keyword, [])
            cond = self.df["ACCOUNT_NM"].isin(keyword_list) & self.df["SJ_NM"].isin(valid_sj_list)
            filtered = self.df.loc[cond].copy()
            if filtered.empty:
                continue
            filtered["표준계정명"] = keyword
            matched_rows.append(filtered)

        if not matched_rows:
            logger.warning("[FIN-PRE] 표준계정 매핑 결과가 비어 있습니다.")
            return pd.DataFrame(columns=use_cols + ["표준계정명"])

        result_df = pd.concat(matched_rows, ignore_index=True)

        result_df["THSTRM_AMOUNT"] = pd.to_numeric(result_df["THSTRM_AMOUNT"], errors="coerce")

        # 사업보고서 전용 항목 필터링
        mask_full = result_df["표준계정명"].isin(self.full_reports_accounts)
        mask_br = (~result_df["표준계정명"].isin(self.full_reports_accounts)) & (result_df["REPRT_CODE"] == "11011")
        result_df = result_df.loc[mask_full | mask_br].reset_index(drop=True)

        return result_df

 
    # 누락 계정 보강 (행 추가)
    def fill_missing_accounts(self, result_df: pd.DataFrame) -> pd.DataFrame:
        """
        기업/연도/보고서/FS_DIV 단위로 누락 표준계정명을 보강(행 추가)
        - full_reports_accounts: 모든 보고서에서 채워넣기
        - business_report_only_accounts: 사업보고서(11011)일 때만 채워넣기
        """
        if result_df.empty:
            return result_df

        group_cols = ["CORP_CODE", "BSNS_YEAR", "REPRT_CODE", "FS_DIV"]
        new_rows = []

        for keys, group in result_df.groupby(group_cols):
            corp_code, year, reprt_code, fs_div = keys
            need_full = any(k in self.full_reports_accounts for k in group["표준계정명"]) or reprt_code == "11011"
            if not need_full and reprt_code != "11011":
                # 사업보고서가 아니고, full 계정도 전혀 없으면 스킵
                continue

            existing = set(group["표준계정명"])
            sample = group.iloc[0].copy()

            # 공통(모든 보고서에서 보강)
            for kw in self.full_reports_accounts:
                if kw not in existing:
                    row = sample.copy()
                    row["표준계정명"] = kw
                    row["ACCOUNT_ID"] = pd.NA
                    row["ACCOUNT_NM"] = f"누락_{kw}"
                    row["THSTRM_AMOUNT"] = pd.NA
                    row["SJ_NM"] = self.sj_map.get(kw, pd.NA)
                    row["ORD"] = pd.NA
                    new_rows.append(row)

            # 사업보고서 전용
            if reprt_code == "11011":
                for kw in self.business_report_only_accounts:
                    if kw not in existing:
                        row = sample.copy()
                        row["표준계정명"] = kw
                        row["ACCOUNT_ID"] = pd.NA
                        row["ACCOUNT_NM"] = f"누락_{kw}"
                        row["THSTRM_AMOUNT"] = pd.NA
                        row["SJ_NM"] = self.sj_map.get(kw, pd.NA)
                        row["ORD"] = pd.NA
                        new_rows.append(row)

        if new_rows:
            result_df = pd.concat([result_df, pd.DataFrame(new_rows)], ignore_index=True)

        return result_df

    # 최신 RCEPT_NO 유지
    def keep_latest_rcept_by_account(self, df: pd.DataFrame) -> pd.DataFrame:
        """기업+연도+보고서+표준계정 기준으로 최신(RCEPT_NO 최대) 한 건만 남김"""
        if df.empty:
            return df.copy()

        tmp = df.copy()
        # 숫자로 변환 실패 시 NaN → 정렬 위해 fillna
        tmp["RCEPT_NO_INT"] = pd.to_numeric(tmp["RCEPT_NO"], errors="coerce").fillna(0).astype(int)

        group_cols = ["CORP_CODE", "BSNS_YEAR", "REPRT_CODE", "표준계정명"]
        latest_idx = tmp.groupby(group_cols)["RCEPT_NO_INT"].idxmax()
        result = tmp.loc[latest_idx].drop(columns=["RCEPT_NO_INT"]).reset_index(drop=True)
        return result


    # 표준계정 중복 정제
    def deduplicate_by_std_account(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        표준계정명 기준 중복 제거 규칙을 적용하여 하나로 정제
        """
        if df.empty:
            return df.copy()

        def apply_rule(std_name, group):
            def by_ord_min(g): return g.sort_values("ORD").head(1)
            def by_ord_max(g): return g.sort_values("ORD", ascending=False).head(1)
            def by_account_nm(g, val): return g[g["ACCOUNT_NM"] == val]
            def by_account_id(g, val): return g[g["ACCOUNT_ID"] == val]
            def by_sj_priority(g):
                for p in ["현금흐름표", "손익계산서", "포괄손익계산서"]:
                    f = g[g["SJ_NM"] == p]
                    if not f.empty:
                        return f.head(1)
                return g.head(1)
            def drop_same_amount(g): return g.drop_duplicates("THSTRM_AMOUNT")
            def sum_all(g):
                res = g.iloc[0:1].copy()
                res["THSTRM_AMOUNT"] = g["THSTRM_AMOUNT"].sum(skipna=True)
                return res
            def ord_diff_within(g, diff=1):
                sorted_g = g.sort_values("ORD", ascending=False)
                if (sorted_g["ORD"].max() - sorted_g["ORD"].min()) <= diff:
                    return sorted_g.head(1)
                return sum_all(g)

            if std_name in ["단기대여금", "장기대여금"]:
                return sum_all(group)
            elif std_name in ["현금자산및현금성자산", "재고자산", "미청구채권", "미수금", "매출채권", "대손상각(현금흐름)",
                              "투자활동현금흐름", "영업활동현금흐름", "판매비과관리비"]:
                return by_ord_min(group)
            elif std_name == "자산총계":
                for name in ["자산총계", "자산", "총자산"]:
                    filtered = by_account_nm(group, name)
                    if not filtered.empty:
                        if len(filtered) > 1:
                            return filtered.sort_values("RCEPT_NO", ascending=False).head(1)
                        return filtered
                return group
            elif std_name == "자본잉여금":
                for name in ["자본잉여금", "주식발행초과금"]:
                    filtered = by_account_nm(group, name)
                    if not filtered.empty:
                        return by_ord_max(filtered)
                return group
            elif std_name in ["이자비용", "이익잉여금", "자본금"]:
                return by_ord_max(group)
            elif std_name == "무형자산":
                g1 = by_account_nm(group, "무형자산")
                if not g1.empty:
                    g2 = by_account_id(g1, "ifrs-full_IntangibleAssetsOtherThanGoodwill")
                    return by_ord_max(g2 if not g2.empty else g1)
                return by_ord_max(group)
            elif std_name == "매출원가":
                deduped = drop_same_amount(group)
                filtered = by_account_nm(deduped, "매출원가")
                return by_ord_max(filtered) if not filtered.empty else sum_all(deduped)
            elif std_name == "매출액":
                return by_ord_min(drop_same_amount(group))
            elif std_name == "매입채무":
                filtered = group[group["ACCOUNT_NM"].isin(["단기매입채무", "유동매입채무"])]
                return by_ord_min(filtered if not filtered.empty else group)
            elif std_name == "당기순이익":
                return by_sj_priority(group)
            else:
                return by_ord_min(group)

        result = []
        for std_name, group in df.groupby("표준계정명", sort=False):
            try:
                processed_groups = []
                for _, g in group.groupby(["CORP_CODE", "BSNS_YEAR", "REPRT_CODE"], sort=False):
                    processed_groups.append(apply_rule(std_name, g) if len(g) > 1 else g)
                result.append(pd.concat(processed_groups, ignore_index=True))
            except Exception as e:
                logger.error(f"[FIN-PRE] deduplicate 오류: 표준계정={std_name} / {e}", exc_info=True)
                result.append(group)

        return pd.concat(result, ignore_index=True)


    # 금액이 NaN이고 ORD가 있으면 0으로 대치
    def clean_amount_zero_if_ord_exists(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        mask = df["ORD"].notnull() & df["THSTRM_AMOUNT"].isnull()
        df.loc[mask, "THSTRM_AMOUNT"] = 0
        return df

    # 기업 기본정보 병합 (세션 컨텍스트)
    def merge_with_company_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        TB_COMPANY(코드/명/종목코드)와 병합
        """
        if df.empty:
            return df

        with SessionLocal() as session:
            factory = BaseQueryFactory(conn=session, model=TB_COMPANY)
            columns = factory.get_columns_by_names("CORP_CODE", "CORP_NAME", "STOCK_CODE")
            rows = session.query(*columns).all()
            company_df = pd.DataFrame(rows, columns=["CORP_CODE", "CORP_NAME", "STOCK_CODE"])

        out = df.copy()
        out["CORP_CODE"] = out["CORP_CODE"].astype(str).str.zfill(8)
        company_df["CORP_CODE"] = company_df["CORP_CODE"].astype(str).str.zfill(8)
        return out.merge(company_df, on="CORP_CODE", how="left")

    # 공시 플래그 행 추가 (사업보고서만)
    def add_disclosure_flags_as_rows(self, df: pd.DataFrame, factory) -> pd.DataFrame:
        """
        '소액공모공시', '최대주주2회변경' 플래그를 행으로 추가 (REPRT_CODE==11011만)
        factory: QueryFactory 혹은 해당 조회함수들을 가진 DAO
        """
        if df.empty:
            return df

        years = df["BSNS_YEAR"].dropna().astype(int).unique()
        majority_change_set = set()
        small_offering_set = set()

        for y in years:
            try:
                for row in factory.find_corp_codes_with_majority_changes_twice_in_year(int(y)):
                    majority_change_set.add((row.CORP_CODE, int(y)))
                for row in factory.find_corp_codes_with_small_public_offering(int(y)):
                    small_offering_set.add((row.CORP_CODE, int(y)))
            except Exception as e:
                logger.error(f"[FIN-PRE] 공시 플래그 조회 실패(year={y}): {e}", exc_info=True)

        base_keys = df[df["REPRT_CODE"] == "11011"][["CORP_CODE", "BSNS_YEAR", "REPRT_CODE"]].drop_duplicates()
        if base_keys.empty:
            return df

        extra_rows = []
        for _, row in base_keys.iterrows():
            corp = row["CORP_CODE"]
            year = int(row["BSNS_YEAR"])

            base_row = df[
                (df["CORP_CODE"] == corp) &
                (df["BSNS_YEAR"].astype(str) == str(year)) &
                (df["REPRT_CODE"] == "11011")
            ].iloc[0]

            common = {
                "CORP_CODE": corp,
                "BSNS_YEAR": year,
                "RCEPT_NO": base_row.get("RCEPT_NO", ""),
                "REPRT_CODE": base_row.get("REPRT_CODE", ""),
                "STOCK_CODE": base_row.get("STOCK_CODE", ""),
                "CORP_NAME": base_row.get("CORP_NAME", ""),
            }
            extra_rows.append({**common, "표준계정명": "소액공모공시", "THSTRM_AMOUNT": str((corp, year) in small_offering_set)})
            extra_rows.append({**common, "표준계정명": "최대주주2회변경", "THSTRM_AMOUNT": str((corp, year) in majority_change_set)})

        # 누락 컬럼 채움 후 병합
        existing_cols = set(df.columns)
        for r in extra_rows:
            for col in (existing_cols - r.keys()):
                r[col] = ""

        return pd.concat([df, pd.DataFrame(extra_rows)], ignore_index=True)

    # 시가총액 빠른 병합
    def append_marketcap_fast(self, marketcap_data: list, df: pd.DataFrame) -> pd.DataFrame:
        """
        RCEPT_NO(YYYYMMDD...) 기준 ±1일 내 BAS_DD가 있으면 '시가총액' 행을 추가
        """
        from datetime import timedelta

        if not marketcap_data or df.empty:
            return df

        marketcap_df = pd.DataFrame(marketcap_data)
        if marketcap_df.empty:
            logger.warning("[FIN-PRE] 시가총액 데이터 비어 있음")
            return df

        marketcap_df["BAS_DD"] = pd.to_datetime(marketcap_df["BAS_DD"], errors="coerce")
        marketcap_df["MKTCAP"] = pd.to_numeric(marketcap_df["MKTCAP"], errors="coerce")
        marketcap_df["STOCK_CODE"] = marketcap_df["STOCK_CODE"].astype(str).str.zfill(6)

        mktcap_dict = {
            (row["STOCK_CODE"], row["BAS_DD"].date()): row["MKTCAP"]
            for _, row in marketcap_df.dropna(subset=["BAS_DD"]).iterrows()
        }

        out = df.copy()
        out["BSNS_YEAR"] = out["BSNS_YEAR"].astype(str)
        out["STOCK_CODE"] = out["STOCK_CODE"].astype(str).str.zfill(6)

        keys_df = (
            out[["CORP_CODE", "BSNS_YEAR", "STOCK_CODE", "CORP_NAME", "REPRT_CODE", "RCEPT_NO"]]
            .dropna(subset=["RCEPT_NO"])
            .drop_duplicates()
        )
        keys_df["RCEPT_DATE"] = pd.to_datetime(keys_df["RCEPT_NO"].astype(str).str[:8], format="%Y%m%d", errors="coerce")

        extra_rows = []
        for _, row in keys_df.dropna(subset=["RCEPT_DATE"]).iterrows():
            stock = row["STOCK_CODE"]
            rdate = row["RCEPT_DATE"].date()
            for key in [(stock, rdate), (stock, rdate - timedelta(days=1)), (stock, rdate + timedelta(days=1))]:
                mkt = mktcap_dict.get(key)
                if pd.notna(mkt):
                    extra_rows.append({
                        "CORP_CODE": row["CORP_CODE"],
                        "BSNS_YEAR": row["BSNS_YEAR"],
                        "REPRT_CODE": row["REPRT_CODE"],
                        "RCEPT_NO": row["RCEPT_NO"],
                        "STOCK_CODE": stock,
                        "CORP_NAME": row["CORP_NAME"],
                        "표준계정명": "시가총액",
                        "THSTRM_AMOUNT": str(int(mkt)),
                    })
                    break

        if not extra_rows:
            return out

        existing_cols = set(out.columns)
        for r in extra_rows:
            for col in (existing_cols - r.keys()):
                r[col] = ""

        return pd.concat([out, pd.DataFrame(extra_rows)], ignore_index=True)


    # 평균자기자본 계산 (TB_FINANCIAL_VARIABLE 보조사용)

    def add_avg_equity(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        ck = self._clean_korean
        alias_set = {ck(x) for x in ["자본총계","총자본","자본","기말자본","분기말자본","반기말자본","당기말","분기말","반기말"]}
        sj_alias = {ck("재무상태표")}
        prev = {"11011":"11014","11014":"11012","11012":"11013","11013":"11011"}

        out = df.copy()
        out["REPRT_CODE"] = out["REPRT_CODE"].astype(str)
        if "ACCOUNT_NM" not in out.columns and "표준계정명" in out.columns:
            out["ACCOUNT_NM"] = out["표준계정명"]
        out["SJ_NM"] = out.get("SJ_NM", "")
        out["ACC_KO"], out["SJ_KO"] = out["ACCOUNT_NM"].map(ck), out["SJ_NM"].map(ck)

        equity_df = out[(out["ACC_KO"].isin(alias_set)) | ((out["SJ_KO"].isin(sj_alias)) & out["ACC_KO"].str.contains(ck("자본"), na=False))].copy()
        if equity_df.empty:
            return out
        equity_df["BSNS_YEAR"] = pd.to_numeric(equity_df["BSNS_YEAR"], errors="coerce").astype("Int64")
        equity_df["THSTRM_AMOUNT"] = pd.to_numeric(equity_df["THSTRM_AMOUNT"].astype(str).str.replace(",", ""), errors="coerce")

        # 필요한 기업/전분기만 타겟팅
        corps = sorted(set(equity_df["CORP_CODE"].dropna()))
        refs = []
        for _, r in equity_df.dropna(subset=["BSNS_YEAR"]).iterrows():
            y, c = int(r["BSNS_YEAR"]), str(r["REPRT_CODE"]) if pd.notna(r["REPRT_CODE"]) else None
            rc = prev.get(c)
            if rc:
                refs.append((r["CORP_CODE"], (y-1 if c=="11013" else y), rc))
        if not refs:
            return out
        years = sorted({y for _, y, _ in refs})
        codes = sorted({rc for *_, rc in refs})
        years_s = [str(y) for y in years]
        codes_s = [str(c) for c in codes]

        # DB에서 최소 범위만 로드
        with SessionLocal() as session:
            def fetch_var():
                model = TB_FINANCIAL_VARIABLE
                cols = [model.CORP_CODE, model.BSNS_YEAR, model.REPRT_CODE]
                if hasattr(model, "ACCOUNT_NM"): cols.append(model.ACCOUNT_NM)
                if hasattr(model, "SJ_NM"): cols.append(model.SJ_NM)
                # 변수 테이블은 ACCOUNT_AMOUNT 고정
                amt = getattr(model, "ACCOUNT_AMOUNT", literal(0)).label("ACCOUNT_AMOUNT")
                cols.append(amt)
                q = session.query(*cols)
                if corps: q = q.filter(model.CORP_CODE.in_(corps))
                if years_s: q = q.filter(cast(model.BSNS_YEAR, String).in_(years_s))
                if codes_s: q = q.filter(cast(model.REPRT_CODE, String).in_(codes_s))
                return pd.read_sql(q.statement, session.bind)

            def fetch_raw():
                model = TB_FINANCIAL_STATEMENTS
                cols = [model.CORP_CODE, model.BSNS_YEAR, model.REPRT_CODE]
                if hasattr(model, "ACCOUNT_NM"): cols.append(model.ACCOUNT_NM)
                if hasattr(model, "SJ_NM"): cols.append(model.SJ_NM)
                # 원본 테이블은 THSTRM_AMOUNT 고정 → ACCOUNT_AMOUNT 이름으로 라벨링
                amt = getattr(model, "THSTRM_AMOUNT", literal(0)).label("ACCOUNT_AMOUNT")
                cols.append(amt)
                q = session.query(*cols)
                if corps: q = q.filter(model.CORP_CODE.in_(corps))
                if years_s: q = q.filter(cast(model.BSNS_YEAR, String).in_(years_s))
                if codes_s: q = q.filter(cast(model.REPRT_CODE, String).in_(codes_s))
                return pd.read_sql(q.statement, session.bind)

            db_equity_df  = fetch_var()
            raw_equity_df = fetch_raw()

        def norm(d):
            if "ACCOUNT_NM" not in d.columns: d["ACCOUNT_NM"] = ""
            if "SJ_NM" not in d.columns: d["SJ_NM"] = ""
            d["BSNS_YEAR"] = pd.to_numeric(d["BSNS_YEAR"], errors="coerce").astype("Int64")
            d["REPRT_CODE"] = d["REPRT_CODE"].astype(str)
            d["ACCOUNT_AMOUNT"] = pd.to_numeric(d["ACCOUNT_AMOUNT"], errors="coerce")
            d["ACC_KO"], d["SJ_KO"] = d["ACCOUNT_NM"].map(ck), d["SJ_NM"].map(ck)
            return d[(d["ACC_KO"].isin(alias_set)) | ((d["SJ_KO"].isin(sj_alias)) & d["ACC_KO"].str.contains(ck("자본"), na=False))]

        db_equity_df, raw_equity_df = norm(db_equity_df), norm(raw_equity_df)

        rows = []
        for _, r in equity_df.dropna(subset=["BSNS_YEAR"]).iterrows():
            corp, y, c, amt = r["CORP_CODE"], int(r["BSNS_YEAR"]), r["REPRT_CODE"], r["THSTRM_AMOUNT"]
            ref_code = prev.get(c); ref_year = y-1 if c=="11013" else y
            ref = pd.NA
            if ref_code:
                s = equity_df[(equity_df["CORP_CODE"]==corp)&(equity_df["BSNS_YEAR"]==ref_year)&(equity_df["REPRT_CODE"]==ref_code)]["THSTRM_AMOUNT"]
                ref = s.iloc[0] if len(s)>0 else pd.NA
                if pd.isna(ref):
                    s = db_equity_df[(db_equity_df["CORP_CODE"]==corp)&(db_equity_df["BSNS_YEAR"]==ref_year)&(db_equity_df["REPRT_CODE"]==ref_code)]["ACCOUNT_AMOUNT"]
                    ref = s.iloc[0] if len(s)>0 else ref
                if pd.isna(ref):
                    s = raw_equity_df[(raw_equity_df["CORP_CODE"]==corp)&(raw_equity_df["BSNS_YEAR"]==ref_year)&(raw_equity_df["REPRT_CODE"]==ref_code)]["ACCOUNT_AMOUNT"]
                    ref = s.iloc[0] if len(s)>0 else ref
            avg = (amt + ref)/2 if (pd.notna(amt) and pd.notna(ref)) else pd.NA
            newr = r.copy(); newr["표준계정명"] = "평균자기자본"; newr["THSTRM_AMOUNT"] = avg; rows.append(newr)

        if rows:
            out = pd.concat([out, pd.DataFrame(rows)], ignore_index=True)
        return out

    # DB 삽입 스키마로 정리 / LLM 추가 할려면 여기서 
    def mark_note_extraction_target(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        주석(LLM) 추출 대상 플래그 추가:
        - 표준계정명이 target_accounts에 포함되고 THSTRM_AMOUNT가 NaN 이면 True
        """
        if df.empty:
            return df

        target_accounts = [
            "매입채무", "매출채권", "유형자산감가상각비", "무형자산감가상각비",
            "대손상각(현금흐름)", "미청구채권", "미수금", "광고선전비", "판매촉진비",
            "개발비", "매출채권처분손실", "무형자산손상차손", "총차입금(단일)","단기대여금","장기대여금","이자비용"
        ]
        out = df.copy()
        out["IS_LLM"] = out["표준계정명"].isin(target_accounts) & out["THSTRM_AMOUNT"].isna()
        return out

    def format_for_database(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        target_cols = [
            "CORP_CODE", "RCEPT_NO", "REPRT_CODE", "BSNS_YEAR",
            "표준계정명", "THSTRM_AMOUNT", "IS_LLM", "IS_COMPLETE"
        ]
        out = df.copy()
        for col in target_cols:
            if col not in out.columns:
                out[col] = False if col == "IS_COMPLETE" else ""

        out = out[target_cols].rename(columns={"표준계정명": "ACCOUNT_NM", "THSTRM_AMOUNT": "ACCOUNT_AMOUNT"})

        loan_items = ["장기대여금", "단기대여금"]
        out["ACCOUNT_AMOUNT"] = pd.to_numeric(out["ACCOUNT_AMOUNT"], errors="coerce")
        grouped2 = out.groupby(["CORP_CODE", "BSNS_YEAR", "REPRT_CODE"], sort=False)
        append_rows = []

        for (corp, year, report), group in grouped2:
            tmpl = group.iloc[0].copy()  

            for loan_nm in loan_items:
                mask = (group["ACCOUNT_NM"] == loan_nm)

                if not mask.any():
                    # 해당 계정이 없으면 0으로 새 행 추가
                    row = tmpl.copy()
                    row["ACCOUNT_NM"] = loan_nm
                    row["ACCOUNT_AMOUNT"] = 0
                    row["IS_LLM"] = True
                    row["IS_COMPLETE"] = False
                    append_rows.append(row)
                else:
                    # 계정은 있는데 금액이 NaN인 경우만 0으로 보정 (기존 값은 그대로)
                    sel = out[
                        (out["CORP_CODE"] == corp) &
                        (out["BSNS_YEAR"] == year) &
                        (out["REPRT_CODE"] == report) &
                        (out["ACCOUNT_NM"] == loan_nm)
                    ]
                    if sel["ACCOUNT_AMOUNT"].isna().any():
                        out.loc[sel.index, "ACCOUNT_AMOUNT"] = sel["ACCOUNT_AMOUNT"].fillna(0)

        if append_rows:
            out = pd.concat([out, pd.DataFrame(append_rows)], ignore_index=True)

        # out = out[~out["ACCOUNT_NM"].isin(loan_items)]

        # IS_COMPLETE 보장
        if "IS_COMPLETE" not in out.columns:
            out["IS_COMPLETE"] = False

        return out