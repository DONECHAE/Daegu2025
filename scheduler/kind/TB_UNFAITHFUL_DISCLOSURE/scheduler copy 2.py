import pandas as pd
import time
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from infrastructure.queryFactory.base_orm import BaseQueryFactory
from setting.database_orm import SessionLocal
from db.public.models import *
from Logger import logger 
from error.email.email_logger import attach_error_email_handler

"""
    * TB_UNFAITHFUL_DISCLOSURE(불성실공시)
        - 불성실공시 지정 기업 정보를 크롤링하여 DB(TB_UNFAITHFUL_DISCLOSURE)에 저장하는 스케줄러
        - 스케줄러 주기: 매일
        - 수집 항목: 번호, 기업명, 종목코드, 벌점, 제재금, 공시책임자교체, 불성실유형, 지정일, 지정사유
"""



class SchedulerServiceTBUnfaithfulDisclosure:
    def __init__(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=9222")

        self.driver = webdriver.Chrome(options=options)
        self.url = "https://kind.krx.co.kr/investwarn/undisclosure.do?method=searchUnfaithfulDisclosureCorpList"
        self.wait = WebDriverWait(self.driver, 20)
        self.data = []

        today = datetime.datetime.today()
        self.today_year = today.strftime("%Y")
        attach_error_email_handler(logger, service_name='WEB:UNFAITHFUL_DISCLOSURE 스케줄러')
    def open_page(self):
        self.driver.get(self.url)
        time.sleep(2)

    def scrape_table(self):
        try:
            rows = self.wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.list.type-00 tbody tr"))
            )
            current_page_data = []
            main_window = self.driver.current_window_handle

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 8:
                    continue

                company_name = cols[1].text.strip()
                stock_code = "N/A"

                # 종목명 클릭 → 새 창에서 종목코드 가져오기
                try:
                    link = cols[1].find_element(By.TAG_NAME, "a")
                    self.driver.execute_script("arguments[0].click();", link)
                    time.sleep(2)

                    self.wait.until(EC.number_of_windows_to_be(2))
                    new_window = [w for w in self.driver.window_handles if w != main_window][0]
                    self.driver.switch_to.window(new_window)

                    stock_code_element = self.wait.until(
                        EC.presence_of_element_located(
                            (By.XPATH, "//th[contains(text(), '종목코드')]/following-sibling::td")
                        )
                    )
                    stock_code = stock_code_element.text.strip()

                    self.driver.close()
                    self.driver.switch_to.window(main_window)

                except Exception as e:
                    logger.error(f"[TB_UNFAITHFUL_DISCLOSURE] -----> 종목코드 가져오기 실패: {e}", exc_info=True)

                item = {
                    "번호": cols[0].text.strip(),
                    "기업명": company_name,
                    "종목코드": stock_code,
                    "벌점": cols[2].text.strip(),
                    "제재금": cols[3].text.strip(),
                    "공시책임자교체": cols[4].text.strip(),
                    "불성실유형": cols[5].text.strip(),
                    "지정일": cols[6].text.strip(),
                    "지정사유": cols[7].text.strip(),
                }
                current_page_data.append(item)

            return current_page_data

        except Exception as e:
            logger.error(f"[TB_UNFAITHFUL_DISCLOSURE] -----> 데이터 수집 실패: {e}", exc_info=True)
            return []

    def run(self):
        """첫 페이지 데이터만 크롤링"""
        try:
            self.open_page()
            # 첫 페이지만 수집
            self.data.extend(self.scrape_table())
        finally:
            if self.driver:
                self.driver.quit()

        self.crud()
        return pd.DataFrame(self.data)

    def crud(self):
        logger.info("[TB_UNFAITHFUL_DISCLOSURE] -----> 스케줄러 시작")
        df = pd.DataFrame(self.data)

        if df.empty:
            logger.info("[TB_UNFAITHFUL_DISCLOSURE] -----> 조회된 데이터가 없습니다.")
            logger.info("[TB_UNFAITHFUL_DISCLOSURE] -----> 스케줄러 종료")
            return

        # Normalize DATE column
        df["DATE"] = pd.to_datetime(df["지정일"], errors='coerce')
        df["DATE_ONLY"] = df["DATE"].dt.date
        df["__KEY__"] = list(zip(df["종목코드"], df["불성실유형"], df["DATE_ONLY"]))

        conn = SessionLocal()
        try:
            query_factory = BaseQueryFactory(conn=conn, model=TB_UNFAITHFUL_DISCLOSURE)
            existing_rows = query_factory.find_all()
            existing_keys = set((r.STOCK_CODE, r.TYPE, r.DATE.date() if hasattr(r.DATE, "date") else r.DATE) for r in existing_rows)

            # Drop duplicates inside the batch
            df = df.drop_duplicates(subset=["종목코드","불성실유형","DATE_ONLY"], keep="first")

            # Filter out existing keys
            df_new = df.loc[~df["__KEY__"].isin(existing_keys)].copy()

            if df_new.empty:
                logger.info("[TB_UNFAITHFUL_DISCLOSURE] -----> 스케줄러 종료")
                return

            instances = []

            for _, row in df_new.iterrows():
                instance = TB_UNFAITHFUL_DISCLOSURE(
                    STOCK_CODE=row.get('종목코드'),
                    CORP_NAME=row.get('기업명'),
                    DEMERIT=row.get('벌점'),
                    SANCTIONS_AMT=row.get('제재금'),
                    OFFICER_CHANGE=row.get('공시책임자교체'),
                    TYPE=row.get('불성실유형'),
                    DATE=row.get('지정일'),
                    REASON=row.get('지정사유'),
                )
                instances.append(instance)

            if instances:
                query_factory.insert_multi_row(instances)
                logger.info(f"[TB_UNFAITHFUL_DISCLOSURE] -----> 삽입 완료: {len(instances)}건")

        except Exception as e:
            logger.error(f"[TB_UNFAITHFUL_DISCLOSURE] -----> Insert 실패: {e}", exc_info=True)

        finally:
            try:
                conn.close()
            except Exception:
                pass

            logger.info("[TB_UNFAITHFUL_DISCLOSURE] -----> 스케줄러 종료")