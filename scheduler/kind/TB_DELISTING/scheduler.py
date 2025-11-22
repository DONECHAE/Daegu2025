import pandas as pd
import time
import datetime
from datetime import timedelta
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
    * TB_DELISTING(상장폐지)
        - 상장폐지 기업 정보를 크롤링하여 DB에 저장하는 스케줄러
        - 스케줄러 주기: 매일
        - 이전상장 및 시장 상장의 경우 코스닥->코스피(코스피->코스닥)로 이전된 것으로 기업 테이블에서 IS_ACTIVE=False로 업데이트 하지 않음
        - 상장폐지된 기업의 경우 기업 테이블에서 IS_ACTIVE 칼럼의 값을 FALSE로 업데이트
"""

class SchedulerServiceTBDelisting:
    def __init__(self, from_date=None, to_date=None):
        """ 셀레니움 크롤러 초기화 (오늘 날짜로 검색) """
        options = Options()

        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")  
        options.add_argument("--remote-debugging-port=9222")  


        self.driver = webdriver.Chrome(options=options)
        self.url = "https://kind.krx.co.kr/investwarn/delcompany.do?method=searchDelCompanyMain"
        self.wait = WebDriverWait(self.driver, 20)
        self.data = []

        # 날짜 및 연도 설정
        today = datetime.datetime.today()
        yesterday = today - timedelta(days=1)
        # 기본값 설정
        if from_date is None:
            self.from_date = yesterday.strftime("%Y%m%d")
        else:
            self.from_date = from_date if isinstance(from_date, str) else from_date.strftime("%Y%m%d")

        if to_date is None:
            self.to_date = today.strftime("%Y%m%d")
        else:
            self.to_date = to_date if isinstance(to_date, str) else to_date.strftime("%Y%m%d")
        attach_error_email_handler(logger, service_name='WEB:DELISTING 스케줄러')

    def open_page(self):
        """ 페이지 열기 """
        self.driver.get(self.url)
        time.sleep(2)

    def set_date_range(self):
        """ 기간 설정 """
        try:
            from_input = self.wait.until(EC.presence_of_element_located((By.ID, "fromDate")))
            to_input = self.wait.until(EC.presence_of_element_located((By.ID, "toDate")))
            
            self.driver.execute_script("arguments[0].value = '';", from_input)
            self.driver.execute_script("arguments[0].value = '';", to_input)
            from_input.send_keys(self.from_date)
            to_input.send_keys(self.to_date)
            
            time.sleep(2)
        except Exception as e:
            logger.error(f"[TB_DELISTING : 상장폐지] -----> ERROR(기간 설정 실패) : {e}", exc_info=True)

    def click_search_button(self):
        """ 검색 버튼 클릭 """
        try:
            search_button = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//a[@class='btn-sprite type-00 vmiddle search-btn' and @title='검색']")
            ))
            self.driver.execute_script("arguments[0].click();", search_button)  
            time.sleep(5)
        except Exception as e:
            logger.error(f"[TB_DELISTING : 상장폐지] -----> ERROR(검색 버튼 클릭 실패) : {e}", exc_info=True)

    def click_next_page(self):
        """ 다음 페이지 버튼 클릭 """
        try:
            next_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@class='next']")))
            next_button.click()
            time.sleep(3)
            return True
        except Exception as e:
            logger.error(f"[TB_DELISTING : 상장폐지] -----> ERROR(다음 페이지가 없습니다.) : {e}", exc_info=True)

    def scraping(self):
        """ 표 데이터 수집 및 종목코드 가져오기 """
        try:
            rows = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.list.type-00 tbody tr")))
            current_page_data = []
            main_window = self.driver.current_window_handle  # 현재 창 핸들 저장

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 5:
                    continue

                company_name = cols[1].text.strip()
                stock_code = None

                # 기업명 클릭하여 새 창 열기
                try:
                    link = cols[1].find_element(By.TAG_NAME, "a")
                    self.driver.execute_script("arguments[0].click();", link)
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
                        logger.error(f"[TB_DELISTING : 상장폐지] -----> ERROR(종목코드 가져오기 실패) : {e}", exc_info=True)
                    # 새 창 닫고 원래 창으로 복귀
                    self.driver.close()
                    self.driver.switch_to.window(main_window)

                except Exception as e:
                    logger.error(f"[TB_DELISTING : 상장폐지] -----> ERROR(링크 클릭 실패) : {e}", exc_info=True)

                item = {
                    "번호": cols[0].text.strip(),
                    "기업명": company_name,
                    "폐지일자": cols[2].text.strip(),
                    "폐지사유": cols[3].text.strip(),
                    "비고": cols[4].text.strip(),
                    "종목코드": stock_code if stock_code else "N/A"
                }
                current_page_data.append(item)

            return current_page_data
        except Exception as e:
            logger.error(f"[TB_DELISTING : 상장폐지] -----> ERROR(데이터 수집 실패) : {e}", exc_info=True)
            return []


    def run(self):
        """ 크롤링 시작 """
        try:
            self.open_page()
            self.set_date_range()
            self.click_search_button()

            page = 1
            previous_page_data = None
            
            while True:
                current_page_data = self.scraping()
                
                if not current_page_data or current_page_data == previous_page_data:
                    break
                
                self.data.extend(current_page_data)
                previous_page_data = current_page_data
                
                if not self.click_next_page():
                    break
                
                page += 1

        finally:
            if self.driver:
                self.driver.quit()

        self.crud()
        return pd.DataFrame(self.data)
    

    def crud(self):
        logger.info("[TB_DELISTING : 상장폐지] -----> 스케줄러 시작")
        df = pd.DataFrame(self.data)
        if not df.empty:
            conn = SessionLocal()
            try:
                query_factory = BaseQueryFactory(conn=conn, model=TB_DELISTING)
                query_factory_company = BaseQueryFactory(conn=conn, model=TB_COMPANY)
                balance_sheets = query_factory.find_all()
                tb_delisting = pd.DataFrame([row.__dict__ for row in balance_sheets])

                diff = set(df['종목코드'].astype(str)) - set(tb_delisting['STOCK_CODE'].astype(str))
                insert_df = df.loc[df['종목코드'].isin(diff)].reset_index(drop=True)

                # 이전상장, 시장 상장의 경우 종목코드가 변하지 않으므로 기업 테이블에서 IS_ACTIVE를 기존의 TRUE로 유지
                update_company = insert_df.loc[
                    (insert_df['종목코드'] != "N/A") &
                    (~insert_df['폐지사유'].str.contains("이전상장", na=False)) &
                    (~insert_df['폐지사유'].str.contains("시장 상장", na=False))
                ]

                instances = []
                for _, row in insert_df.iterrows():
                    instance = {
                    "STOCK_CODE": row['종목코드'],
                    "CORP_NAME": row['기업명'],
                    "DATE": row['폐지일자'],
                    "REASON": row['폐지사유'],
                    "RM": row['비고'],
                    }

                    instances.append(TB_DELISTING(**instance))

                if instances:
                    try:
                        query_factory.insert_multi_row(instances)
                        # TB_COMPANY에 IS_ACTIVE=False 업데이트
                        stock_codes = update_company['종목코드'].dropna().replace("N/A", pd.NA).dropna().tolist()
                        tb_company = query_factory_company.find_all_in(column_name="STOCK_CODE", values=stock_codes)
                        [query_factory.update(company, IS_ACTIVE=False) for company in tb_company]
                        logger.info(f"[TB_DELISTING : 상장폐지] -----> 삽입 : {len(instances)}건 적재 완료")
                    except Exception as e:
                        logger.error(f"[TB_DELISTING : 상장폐지] -----> ERROR : Insert failed: {e}", exc_info=True)
                        raise 
            except:
                logger.error(f"[TB_DELISTING : 상장폐지] -----> ERROR : DB 처리 실패: {e}", exc_info=True)
        else:
            logger.info("[TB_DELISTING : 상장폐지] -----> 조회된 데이터가 없습니다.")

        logger.info("[TB_DELISTING : 상장폐지] -----> 스케줄러 종료")      
