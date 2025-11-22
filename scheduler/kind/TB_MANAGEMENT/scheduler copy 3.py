import time
import pandas as pd
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from infrastructure.queryFactory.base_orm import BaseQueryFactory
from setting.database_orm import SessionLocal
from db.public.models import TB_MANAGEMENT
from Logger import logger 
from error.email.email_logger import attach_error_email_handler
"""
    * TB_MANAGEMENT(관리종목)
        - 관리종목 지정 기업 정보를 크롤링하여 DB(TB_MANAGEMENT)에 저장하는 스케줄러
        - 스케줄러 주기: 매일
"""

class SchedulerServiceTBManagement:
    def __init__(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=9222")

        self.driver = webdriver.Chrome(options=options)
        self.url = "https://kind.krx.co.kr/investwarn/adminissue.do?method=searchAdminIssueList"
        self.wait = WebDriverWait(self.driver, 20)
        self.data = []
        attach_error_email_handler(logger, service_name='WEB:MANAGEMENT 스케줄러')
    def open_page(self):
        self.driver.get(self.url)
        time.sleep(2)

    def scrape_table(self):
        try:
            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.list.type-00.tmt30"))
            )
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table.list.type-00.tmt30 tbody tr")
            page_data = []

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 3:
                    continue

                corp_name = cols[0].text.strip()
                date_str = cols[1].text.strip()
                reason = cols[2].text.strip()

                # 날짜 파싱 (실패 시 건너뜀)
                try:
                    date = pd.to_datetime(date_str)
                except Exception as e:
                    logger.error(f"[TB_MANAGEMENT] -----> 날짜 파싱 실패: {date_str} / {e}", exc_info=True)
                    continue

                page_data.append({
                    "CORP_NAME": corp_name,
                    "DATE": date,
                    "REASON": reason,
                })

            return page_data

        except Exception as e:
            logger.error(f"[TB_MANAGEMENT] -----> 테이블 파싱 실패: {e}", exc_info=True)
            return []

    def run(self):
        try:
            self.open_page()
            self.data.extend(self.scrape_table())
        finally:
            if self.driver:
                self.driver.quit()

        self.crud()
        return pd.DataFrame(self.data)

    def crud(self):
        logger.info("[TB_MANAGEMENT] -----> 스케줄러 시작")
        df = pd.DataFrame(self.data)

        if df.empty:
            logger.info("[TB_MANAGEMENT] -----> 조회된 데이터가 없습니다.")
            logger.info("[TB_MANAGEMENT] -----> 스케줄러 종료")
            return

        # 타입 정규화
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df["DATE_ONLY"] = df["DATE"].dt.date

        conn = SessionLocal()
        try:
            factory = BaseQueryFactory(conn=conn, model=TB_MANAGEMENT)
            db_rows = factory.find_all()
            existing_keys = set((r.CORP_NAME, r.DATE.date() if hasattr(r.DATE, "date") else r.DATE) for r in db_rows)

            df = df.drop_duplicates(subset=["CORP_NAME", "DATE_ONLY"], keep="first").copy()
            df["__KEY__"] = list(zip(df["CORP_NAME"], df["DATE_ONLY"]))
            df_new = df.loc[~df["__KEY__"].isin(existing_keys)].copy()

            instances = []
            for _, row in df_new.iterrows():
                instance = TB_MANAGEMENT(
                    CORP_NAME=row["CORP_NAME"],
                    DATE=row["DATE"],
                    REASON=row["REASON"],
                )
                instances.append(instance)

            if instances:
                factory.insert_multi_row(instances)
                logger.info(f"[TB_MANAGEMENT] -----> 삽입 완료: {len(instances)}건")

        except Exception as e:
            logger.error(f"[TB_MANAGEMENT] -----> Insert 실패: {e}", exc_info=True)

        finally:
            conn.close()
            logger.info("[TB_MANAGEMENT] -----> 스케줄러 종료")