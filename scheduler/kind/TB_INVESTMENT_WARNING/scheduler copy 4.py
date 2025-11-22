import pandas as pd
from datetime import timedelta
import datetime
import time
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
    * TB_INVESTMENT_WARNING(투자주의/경고/위험종목)
        - 투자주의/경고/위험종목 정보를 크롤링하여 DB에 저장하는 스케줄러
        - 스케줄러 주기: 매일
"""

class SchedulerServiceTBInvestmentWarning:
    def __init__(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")  
        self.driver = webdriver.Chrome(options=options)
        # 날짜 및 연도 설정
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")
        self.lastweek = (datetime.datetime.today() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        self.url = "https://kind.krx.co.kr/investwarn/investattentwarnrisky.do?method=investattentwarnriskyMain#"
        self.wait = WebDriverWait(self.driver, 20)
        self.data = []
        attach_error_email_handler(logger, service_name='WEB:INVESTMENT_WARNING 스케줄러')
    def open_page(self):
        """웹 페이지 열기"""
        self.driver.get(self.url)
        time.sleep(3)

    def set_today_date(self):
      """오늘 날짜로 조회 설정"""
      try:
          from_input = self.wait.until(EC.presence_of_element_located((By.ID, "startDate")))
          to_input = self.wait.until(EC.presence_of_element_located((By.ID, "endDate")))

          self.driver.execute_script("arguments[0].removeAttribute('readonly')", from_input)
          self.driver.execute_script("arguments[0].removeAttribute('readonly')", to_input)
          from_input.clear()
          to_input.clear()
          from_input.send_keys(self.lastweek)
          to_input.send_keys(self.today)
          time.sleep(2)
      except Exception as e:
          pass
      
    def click_search_button(self):
        """검색 버튼 클릭"""
        try:
            search_button = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//a[@class='btn-sprite type-00 vmiddle search-btn' and @title='검색']")
            ))
            self.driver.execute_script("arguments[0].click();", search_button)
            time.sleep(5)
        except Exception as e:
            pass
        
    def click_next_page(self, category):
        """다음 페이지 버튼 클릭"""
        try:
            next_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@href='#nextPage' and contains(@class, 'next')]")))
            self.driver.execute_script("arguments[0].click();", next_button)
            time.sleep(3)
            return True
        except Exception:
            return False
        
    def select_category(self, category):
        """투자주의/경고/위험 종목 카테고리 선택"""
        category_map = {"주의": "투자주의종목", "경고": "투자경고종목", "위험": "투자위험종목"}
        if category not in category_map:
            logger.error(f"[TB_INVESTMENT_WARNING : 투자주의환기] -----> ERROR : 잘못된 카테고리: {category}", exc_info=True)
            return False
        
        try:
            tab_button = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, f"//li/a[@title='{category_map[category]}']")
            ))
            tab_button.click()
            time.sleep(3)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.list.type-00 tbody tr")))
            return True
        except Exception as e:
            return False

    def parser(self, category):
        """현재 페이지의 데이터를 크롤링"""
        try:
            rows = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.list.type-00 tbody tr")))
            current_page_data = []
            main_window = self.driver.current_window_handle  # 현재 창 핸들 저장

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 5:
                    continue

                # 종목명 클릭하여 종목코드 가져오기
                company_element = cols[1].find_element(By.TAG_NAME, "a")
                stock_name = company_element.text.strip()
                stock_code = "N/A"  # 기본값 설정

                try:
                    # 종목명 클릭하여 새 창 열기
                    self.driver.execute_script("arguments[0].click();", company_element)
                    time.sleep(2)

                    # 새 창으로 전환
                    self.wait.until(EC.number_of_windows_to_be(2))
                    new_window = [w for w in self.driver.window_handles if w != main_window][0]
                    self.driver.switch_to.window(new_window)

                    # 종목코드 가져오기
                    try:
                        stock_code_element = self.wait.until(EC.presence_of_element_located(
                            (By.XPATH, "//th[contains(text(), '종목코드')]/following-sibling::td")
                        ))
                        stock_code = stock_code_element.text.strip()
                    except Exception as e:
                        pass

                    # 새 창 닫고 원래 창으로 복귀
                    self.driver.close()
                    self.driver.switch_to.window(main_window)

                except Exception as e:
                    pass

                # 데이터 항목 구성
                if category == "주의":
                    item = {
                        "번호": cols[0].text.strip(),
                        "종목": stock_name,
                        "유형": cols[2].text.strip(),  # 주의 카테고리는 유형 컬럼 포함
                        "공시일": cols[3].text.strip(),
                        "지정일": cols[4].text.strip(),
                        "해제일": "",  # 주의 카테고리는 해제일 없음
                        "카테고리": category,
                        "종목코드": stock_code
                    }
                else:  # "경고" 또는 "위험"
                    해제일 = cols[4].text.strip() if cols[4].text.strip() else "해제되지 않음"
                    item = {
                        "번호": cols[0].text.strip(),
                        "종목": stock_name,
                        "유형": "",  # 경고, 위험은 유형 없음
                        "공시일": cols[2].text.strip(),
                        "지정일": cols[3].text.strip(),
                        "해제일": 해제일,
                        "카테고리": category,
                        "종목코드": stock_code
                    }

                current_page_data.append(item)
            return current_page_data
        except Exception as e:
            return []

    def crawler(self):
        """오늘 날짜 크롤링 시작"""
        self.open_page()
        self.set_today_date()
        self.click_search_button()

        categories = ["주의", "경고", "위험"]
        for category in categories:
            if not self.select_category(category):
                continue

            previous_page_data = None  # 이전 페이지 데이터 저장

            while True:
                page_data = self.parser(category)

                # 이전 페이지 데이터와 현재 데이터가 동일하면 종료
                if page_data == previous_page_data:
                    break

                self.data.extend(page_data)
                previous_page_data = page_data  # 이전 페이지 데이터 갱신

                if not self.click_next_page(category):
                    break

        self.driver.quit()
        return pd.DataFrame(self.data)
        
    def run(self):
        logger.info("[TB_INVESTMENT_WARNING : 투자주의/경고/위험] -----> 스케줄러 시작")
        investment_api = pd.DataFrame(self.crawler())

        if not investment_api.empty:
            conn = SessionLocal()

            try:
                base_query_factory = BaseQueryFactory(conn, TB_INVESTMENT_WARNING)
                balance_sheets = base_query_factory.find_all()
                investment_df = pd.DataFrame([row.__dict__ for row in balance_sheets])

                diff = set(zip(investment_df['STOCK_CODE'].astype(str), investment_df['POST_DATE'].astype(str), investment_df['CATEGORY'], investment_df['TYPE']))
                investment_api[['종목코드','공시일','카테고리','유형']] = investment_api[['종목코드','공시일','카테고리','유형']].astype(str)
                insert_df = investment_api.loc[~investment_api.apply(lambda row: (row['종목코드'], row['공시일'], row['카테고리'], row['유형']) in diff, axis=1)].reset_index(drop=True)

                instances = []

                for _, row in insert_df.iterrows():
                  instance = {
                  "STOCK_CODE": row['종목코드'],
                  "CORP_NAME": row['종목'],
                  "TYPE": row['유형'],
                  "POST_DATE": row['공시일'] if row['공시일'] not in ["", "-", None] else None,
                  "DESIGNATED_DATE": row['지정일'] if row['지정일'] not in ["", "-", None] else None,
                  "CANCLE_DATE": row['해제일'] if row['해제일'] not in ["", "-", None] else None,
                  "CATEGORY": row['카테고리'],
                  }

                  
                  instances.append(TB_INVESTMENT_WARNING(**instance))

                if instances:
                    base_query_factory.insert_multi_row(instances)
                    logger.info(f"[TB_INVESTMENT_WARNING : 투자주의/경고/위험] -----> 삽입 : {len(instances)}건 적재 완료")
                else:
                    logger.info("[TB_INVESTMENT_WARNING : 투자주의/경고/위험] -----> 조회된 데이터가 없습니다.")
            except Exception as e:
                logger.error(f"[TB_INVESTMENT_WARNING : 투자주의/경고/위험] -----> ERROR : DB 처리 실패: {e}", exc_info=True)

        logger.info("[TB_INVESTMENT_WARNING : 투자주의/경고/위험] -----> 스케줄러 종료")   



