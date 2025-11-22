import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from infrastructure.queryFactory.base_orm import BaseQueryFactory
from setting.database_orm import SessionLocal
from db.public.models import *
from Logger import logger 
from error.email.email_logger import attach_error_email_handler
"""
    * TB_INVESTMENT_ATTENTION(투자주의환기종목)
        - 투자주의환기 정보를 크롤링하여 DB에 저장하는 스케줄러
        - 스케줄러 주기: 매일
"""

class SchedulerServiceTBInvestmentAttention:
    def __init__(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")  
        self.driver = webdriver.Chrome(options=options)
        self.url = "https://kind.krx.co.kr/investwarn/hwangiissue.do?method=searchHwangiIssueMain"
        self.wait = WebDriverWait(self.driver, 20)
        self.data = []
        attach_error_email_handler(logger, service_name='WEB:INVESTMENT_ATTENTION 스케줄러')
        
    def open_page(self):
        """웹 페이지 열기"""
        self.driver.get(self.url)
        time.sleep(2)

    def scraping(self):
        new_data = []
        try:
            rows = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.list.type-00 tbody tr")))
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 3:
                    continue

                corp_cell = cols[0]
                date_str = cols[1].text.strip()
                reason = cols[2].text.strip()
                # 회사명 및 종목코드 추출
                corp_name = corp_cell.text.strip().split("\n")[0]
                stock_code = None
                try:
                    link = corp_cell.find_element(By.TAG_NAME, "a")
                    corp_name = link.text.strip()

                    # 새 탭 열기 및 전환
                    link.send_keys(Keys.CONTROL + Keys.RETURN)
                    self.driver.switch_to.window(self.driver.window_handles[-1])
                    time.sleep(1)

                    stock_code_element = self.wait.until(EC.presence_of_element_located(
                        (By.XPATH, "//th[contains(text(), '종목코드')]/following-sibling::td")
                    ))
                    stock_code = stock_code_element.text.strip()

                    # 탭 닫고 메인 탭으로 전환
                    self.driver.close()
                    self.driver.switch_to.window(self.driver.window_handles[0])

                except Exception as e:
                    logger.error(f"[TB_INVESTMENT_ATTENTION : 투자주의환기] -----> ERROR : 종목코드 추출 실패 ({corp_name})", exc_info=True)

                new_data.append({
                    "CORP_NAME": corp_name,
                    "STOCK_CODE": stock_code,
                    "DATE": date_str,
                    "REASON": reason,
                })

        except Exception as e:
            logger.error(f"[TB_INVESTMENT_ATTENTION : 투자주의환기] -----> ERROR : 크롤링 실패: {e}", exc_info=True)

        return new_data

    def crawler(self):
        self.open_page()
        page = 1
        previous_page_data = None
        while True:
            new_data = self.scraping()
            if not new_data or new_data == previous_page_data:
                break
            self.data.extend(new_data)
            previous_page_data = new_data

            try:
                page += 1
                self.driver.execute_script(f"fnPageGo('{page}')")
                time.sleep(2)
            except Exception as e:
                logger.error(f"[TB_INVESTMENT_ATTENTION : 투자주의환기] -----> ERROR : 다음 페이지 이동 실패: {e}", exc_info=True)
                break

        self.driver.quit()
        return self.data
        
    def run(self):
        logger.info("[TB_DISCLOSURE_INFOMATION : 공시검색 크롤링] -----> 스케줄러 시작")
        investment_api = pd.DataFrame(self.crawler())

        if not investment_api.empty:
            conn = SessionLocal()

            try:
                base_query_factory = BaseQueryFactory(conn, TB_INVESTMENT_ATTENTION)
                balance_sheets = base_query_factory.find_all()
                investment_df = pd.DataFrame([row.__dict__ for row in balance_sheets])

                diff = set(zip(investment_df['STOCK_CODE'].astype(str), investment_df['DATE'].astype(str)))
                investment_api[['STOCK_CODE','DATE']] = investment_api[['STOCK_CODE','DATE']].astype(str)
                insert_df = investment_api.loc[~investment_api.apply(lambda row: (row['STOCK_CODE'], row['DATE']) in diff, axis=1)].reset_index(drop=True)
         

                instances = []

                for _, row in insert_df.iterrows():
                  instance = {
                  "STOCK_CODE": row['STOCK_CODE'],
                  "CORP_NAME": row['CORP_NAME'],
                  "DATE": row['DATE'],
                  "REASON": row['REASON'],
                  }

                  instances.append(TB_INVESTMENT_ATTENTION(**instance))

                if instances:
                    base_query_factory.insert_multi_row(instances)
                    logger.info(f"[TB_INVESTMENT_ATTENTION : 투자주의환기] -----> 삽입 : {len(instances)}건 적재 완료")
                else:
                    logger.info("[TB_INVESTMENT_ATTENTION : 투자주의환기] -----> 조회된 데이터가 없습니다.")
            except Exception as e:
                logger.error(f"[TB_INVESTMENT_ATTENTION : 투자주의환기] -----> ERROR : DB 처리 실패: {e}", exc_info=True)

        logger.info("[TB_INVESTMENT_ATTENTION : 투자주의환기] -----> 스케줄러 종료")   



