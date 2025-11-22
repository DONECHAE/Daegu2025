from setting.database_orm import SessionLocal
from infrastructure.queryFactory.base_orm import BaseQueryFactory
from db.public.models import TB_DISCLOSURE_INFORMATION
from Logger import logger , request_context
from uuid import uuid4
from datetime import datetime, timedelta
import requests
import re
import difflib
from bs4 import BeautifulSoup
import time
import pandas as pd
from error.email.email_logger import attach_error_email_handler

class SchedulerServiceTBDisclosureCrawlerCRTCVT:
    def __init__(self):
        # logger request_context내 UUID 직접 할당
        request_context.request_id = str(uuid4())
        self.USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.3904.108 Safari/537.36'
        self.three_days_ago = (datetime.today() - timedelta(days=3)).date()
        attach_error_email_handler(logger, service_name='WEB:TB_DISCLOSURE_INFOMATION_주석 스케줄러')


    def document_link(self, rcp_no:str, match=None):
      '''
      지정한 URL문서에 속해있는 하위 문서 목록정보(title, url)을 데이터프레임으로 반환합니다.
      * rcp_no: 접수번호를 지정합니다. rcp_no 대신 첨부문서의 URL(http로 시작)을 사용할 수도 있습니다.
      * match: 매칭할 문자열 (문자열을 지정하면 문서 제목과 가장 유사한 순서로 정렬합니다)
      '''
      if rcp_no.isdecimal():
          r = requests.get(f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}', headers={'User-Agent': self.USER_AGENT})
      elif rcp_no.startswith('http'):
          r = requests.get(rcp_no, headers={'User-Agent': self.USER_AGENT})
      else:
          logger.error(f"[TB_DISCLOSURE_INFORMATION : 공시검색 크롤링] -----> ERROR : invalid `rcp_no`(or url)", exc_info=True)
          
      ## 하위 문서 URL 추출
      multi_page_re = (
          "\s+node[12]\['text'\][ =]+\"(.*?)\"\;" 
          "\s+node[12]\['id'\][ =]+\"(\d+)\";"
          "\s+node[12]\['rcpNo'\][ =]+\"(\d+)\";"
          "\s+node[12]\['dcmNo'\][ =]+\"(\d+)\";"
          "\s+node[12]\['eleId'\][ =]+\"(\d+)\";"
          "\s+node[12]\['offset'\][ =]+\"(\d+)\";"
          "\s+node[12]\['length'\][ =]+\"(\d+)\";"
          "\s+node[12]\['dtd'\][ =]+\"(.*?)\";"
          "\s+node[12]\['tocNo'\][ =]+\"(\d+)\";"
      )
      matches = re.findall(multi_page_re, r.text)
      
      if matches:
          row_list = []
          for m in matches:
              params = f'rcpNo={m[2]}&dcmNo={m[3]}&eleId={m[4]}&offset={m[5]}&length={m[6]}&dtd={m[7]}'
              doc_url = f'http://dart.fss.or.kr/report/viewer.do?{params}'
              row_list.append([m[0], doc_url])
          
          df = pd.DataFrame(row_list, columns=['title', 'url'])
          
          if match:
              df['similarity'] = df['title'].apply(lambda x: difflib.SequenceMatcher(None, x, match).ratio())
              df = df.sort_values('similarity', ascending=False)
          
          return df[['title', 'url']]
      
      else:
          single_page_re = "\t\tviewDoc\('(\d+)', '(\d+)', '(\d+)', '(\d+)', '(\d+)', '(\S+)',''\)\;"
          matches = re.findall(single_page_re, r.text)
          
          if matches:
              doc_title = BeautifulSoup(r.text, features="lxml").title.text.strip()
              m = matches[0]
              params = f'rcpNo={m[0]}&dcmNo={m[1]}&eleId={m[2]}&offset={m[3]}&length={m[4]}&dtd={m[5]}'
              doc_url = f'http://dart.fss.or.kr/report/viewer.do?{params}'
              return pd.DataFrame([[doc_title, doc_url]], columns=['title', 'url'])
          else:
              logger.error(f"[TB_DISCLOSURE_INFORMATION : 공시검색 크롤링] -----> ERROR : URL {rcp_no} 하위 페이지를 포함하고 있지 않습니다", exc_info=True)

    def get_document_html(self, df):
      if df.empty:
          return ""
      full_text = ""
      for _, row in df.iterrows():
          url = row['url']
          max_retries = 3
          delay = 5
          for attempt in range(max_retries):
              try:
                  response = requests.get(url, timeout=10)
                  time.sleep(0.5)  # 요청 간 0.5초 대기
                  break  # 성공하면 반복 종료
              except requests.exceptions.RequestException as req_err:
                  if attempt < max_retries - 1:
                      time.sleep(delay)
                      continue
                  else:
                      continue  # 다음 URL로 넘어감
          else:
              continue  # 실패한 경우 다음 URL로 넘어감
          soup = BeautifulSoup(response.text, "html.parser")
          html_content = soup.prettify()
          full_text += html_content + "\n---\n"
      return full_text.strip()

    def run(self):
        logger.info("[TB_DISCLOSURE_INFOMATION : 공시검색 크롤링] -----> 스케줄러 시작")
        update_count = 0
        with SessionLocal() as conn:
            try:
                query_factory = BaseQueryFactory(conn, TB_DISCLOSURE_INFORMATION)
                queryInfo = conn.query(TB_DISCLOSURE_INFORMATION).filter(TB_DISCLOSURE_INFORMATION.RCEPT_DT >= self.three_days_ago).all()
                df = pd.DataFrame([row.__dict__ for row in queryInfo])
                if df.empty:
                    logger.info("[TB_DISCLOSURE_INFORMATION] -----> 최근 3일 데이터 없음")
                    return
                df.columns = [c.upper() for c in df.columns]
                report = df.loc[
                        df['REPORT_NM'].str.contains(
                            "감사보고서|합병등종료보고서|회사합병결정|투자설명서",
                            na=False
                        ) & (df['CRT_CVT_COMMENT'].isna())
                    ].reset_index(drop=True)

                for _, row in report.iterrows():
                    reportNum = row['RCEPT_NO']
                    reportNm = str(row.get('REPORT_NM', '') or '')

                    # 보고서 유형별 하위 문서 필터 키워드 매핑
                    keyword = None
                    if '감사보고서' in reportNm:
                        keyword = '독립된'  # 예: "독립된 감사보고서"
                    elif '합병등종료보고서' in reportNm:
                        keyword = '요약재무정보'
                    elif '회사합병결정' in reportNm:
                        keyword = '회사합병'
                    elif '투자설명서' in reportNm:
                        keyword = '요약정보'

                    comment = self.document_link(reportNum)
                    if comment is None or comment.empty:
                        continue

                    # 제목에 키워드가 포함된 하위 문서만 선별
                    if keyword:
                        comment = comment[comment['title'].str.contains(keyword, na=False)]
                    if comment is None or comment.empty:
                        continue

                    full_html = self.get_document_html(comment)

                    update_fields = {}
                    if full_html:
                        update_fields["CRT_CVT_COMMENT"] = full_html

                    update_filed = query_factory.find_one(RCEPT_NO=reportNum)

                    if update_filed and update_fields:
                        query_factory.update(update_filed, **update_fields)
                        update_count += 1

                if update_count == 0:
                    logger.info("[TB_DISCLOSURE_INFORMATION : 공시검색 크롤링] -----> 조회된 데이터가 없습니다")  

            except Exception as e:
                logger.error(f"[TB_DISCLOSURE_INFORMATION : 공시검색 크롤링] ERROR -----> {e}", exc_info=True)
  
        logger.info(f"[TB_DISCLOSURE_INFORMATION : 공시검색 크롤링] -----> 주석 업데이트 : {update_count}건 업데이트 완료")  
        logger.info("[TB_DISCLOSURE_INFORMATION : 공시검색 크롤링] -----> 스케줄러 종료")