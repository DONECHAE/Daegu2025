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
    * TB_EMBEZZLEMENT(횡령)
        - 횡령 정보를 크롤링하여 DB에 저장하는 스케줄러
        - 스케줄러 주기: 매일
"""

class SchedulerServiceTBEmbezzlement:
    """오늘 날짜 횡령 공시 크롤러"""
    def __init__(self, from_date=None, to_date=None):
        """오늘 날짜 횡령 공시 크롤러 초기화"""
        options = Options()
       
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")  
        options.add_argument("--remote-debugging-port=9222")  
        self.driver = webdriver.Chrome(options=options)
        self.url = "https://kind.krx.co.kr/investwarn/investattentEmbezzlement.do?method=searchInvestAttentEmbezzlementMain"
        self.wait = WebDriverWait(self.driver, 20)
        self.data = []

        # 날짜 설정 (기본: 최근 7일, 필요 시 파라미터로 오버라이드)
        today = datetime.datetime.today()
        default_from = today - timedelta(days=7)

        if from_date is None:
            self.from_date_str = default_from.strftime("%Y%m%d")
        else:
            self.from_date_str = from_date if isinstance(from_date, str) else from_date.strftime("%Y%m%d")

        if to_date is None:
            self.to_date_str = today.strftime("%Y%m%d")
        else:
            self.to_date_str = to_date if isinstance(to_date, str) else to_date.strftime("%Y%m%d")

        self.today_year = today.strftime("%Y")
        attach_error_email_handler(logger, service_name='WEB:EMBEZZLEMENT 스케줄러')
        
    def open_page(self):
        """웹 페이지 열기"""
        self.driver.get(self.url)
        time.sleep(2)

    def set_today_date(self):
        """오늘 날짜로 조회 설정"""
        try:
            from_input = self.wait.until(EC.presence_of_element_located((By.ID, "fromDate")))
            to_input = self.wait.until(EC.presence_of_element_located((By.ID, "toDate")))

            self.driver.execute_script("arguments[0].value = '';", from_input)
            self.driver.execute_script("arguments[0].value = '';", to_input)
            from_input.send_keys(self.from_date_str)
            to_input.send_keys(self.to_date_str)
            time.sleep(2)
        except Exception as e:
            logger.error(f"[TB_EMBEZZLEMENT : 횡령] -----> ERROR(날짜 설정 실패) : {e}", exc_info=True)
            return False

    def click_search_button(self):
        """검색 버튼 클릭"""
        try:
            search_button = self.wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//a[@class='btn-sprite type-00 vmiddle search-btn' and @title='검색']")
            ))
            self.driver.execute_script("arguments[0].click();", search_button)
            time.sleep(5)
        except Exception as e:
            logger.error(f"[TB_EMBEZZLEMENT : 횡령] -----> ERROR(검색 버튼 클릭 실패) : {e}", exc_info=True)
            False

    def click_next_page(self):
        """다음 페이지 버튼 클릭"""
        try:
            next_button = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//a[@href='#nextPage' and contains(@class, 'next')]")))
            self.driver.execute_script("arguments[0].click();", next_button)
            time.sleep(3)
            return True
        except Exception as e:
            logger.error(f"[TB_EMBEZZLEMENT : 횡령] -----> ERROR(다음 페이지가 존재하지 않습니다.) : {e}", exc_info=True)
            return False
        
    def scrape_table(self):
        """표 데이터 수집"""
        try:
            rows = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.list.type-00 tbody tr")))
            current_page_data = []
            main_window = self.driver.current_window_handle  # 현재 창 핸들 저장

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 4:
                    continue

                # 기업명 클릭하여 종목코드 가져오기
                company_element = cols[2].find_element(By.TAG_NAME, "a")
                company_name = company_element.text.strip()
                stock_code = "N/A"  # 기본값 설정

                try:
                    # 기업명 클릭하여 새 창 열기
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
                        logger.error(f"[TB_EMBEZZLEMENT : 횡령] -----> ERROR(종목코드 가져오기 실패) : {e}", exc_info=True)
                        return False

                    # 새 창 닫고 원래 창으로 복귀
                    self.driver.close()
                    self.driver.switch_to.window(main_window)

                except Exception as e:
                    logger.error(f"[TB_EMBEZZLEMENT : 횡령] -----> ERROR(링크 클릭 실패) : {e}", exc_info=True)
                    return False

                # 공시제목 가져오기
                public_notice = cols[3].find_element(By.TAG_NAME, "a").get_attribute("title").strip()

                item = {
                    "번호": cols[0].text.strip(),
                    "공시일자": cols[1].text.strip(),
                    "기업명": company_name,
                    "종목코드": stock_code,  # 종목코드 추가
                    "공시제목": public_notice,
                }
                current_page_data.append(item)

            return current_page_data
        except Exception as e:
            logger.error(f"[TB_EMBEZZLEMENT : 횡령] -----> ERROR(데이터 수집 실패) : {e}", exc_info=True)
            return []
    
    def run(self):
        """오늘 날짜 크롤링 시작"""
        self.open_page()
        self.set_today_date()
        self.click_search_button()
        
        page = 1
        previous_page_data = None
        
        while True:
            current_page_data = self.scrape_table()

            # 동일한 페이지 데이터가 반복될 경우 크롤링 종료
            if not current_page_data or current_page_data == previous_page_data:
                break

            self.data.extend(current_page_data)
            previous_page_data = current_page_data

            if not self.click_next_page():
                break
            
            page += 1

        self.driver.quit()
        self.crud()
        return pd.DataFrame(self.data)
    
    def crud(self):
        logger.info("[TB_EMBEZZLEMENT : 횡령] -----> 스케줄러 시작")
        df = pd.DataFrame(self.data)

        if not df.empty:
            conn = SessionLocal()

            try:
                query_factory = BaseQueryFactory(conn=conn, model=TB_EMBEZZLEMENT)
                balance_sheets = query_factory.find_all()
                tb_embezzlement = pd.DataFrame([row.__dict__ for row in balance_sheets])

                diff = set(zip(tb_embezzlement['STOCK_CODE'].astype(str), tb_embezzlement['DATE'].astype(str)))
                df[['종목코드','공시일자']] = df[['종목코드','공시일자']].astype(str)
                insert_df = df.loc[
                    ~df.apply(lambda row: (row['종목코드'], row['공시일자']) in diff, axis=1)
                ].reset_index(drop=True)


                instances = []
                for _, row in insert_df.iterrows():
                    instance = {
                    "STOCK_CODE": row['종목코드'],
                    "CORP_NAME": row['기업명'],
                    "TITLE": row['공시제목'],
                    "DATE": row['공시일자'],
                    }

                    instances.append(TB_EMBEZZLEMENT(**instance))

                if instances:
                    query_factory.insert_multi_row(instances)
                    logger.info(f"[TB_EMBEZZLEMENT : 횡령] -----> 삽입 : {len(instances)}건 적재 완료")
                else:
                    logger.info("[TB_EMBEZZLEMENT : 횡령] -----> 조회된 데이터가 없습니다.")
                
            except Exception as e:
                logger.error(f"[TB_EMBEZZLEMENT : 횡령 -----> ERROR : DB 처리 실패: {e}", exc_info=True)

        logger.info("[TB_EMBEZZLEMENT : 횡령] -----> 스케줄러 종료")    