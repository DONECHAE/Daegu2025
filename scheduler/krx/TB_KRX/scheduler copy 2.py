from setting.database_orm import SessionLocal
from Logger import logger, request_context
from uuid import uuid4
from db.public.models import *
from setting.inject import provision_inject_orm
from datetime import datetime, timedelta
from infrastructure.queryFactory.base_orm import BaseQueryFactory
import requests
import pandas as pd
from error.email.email_logger import attach_error_email_handler
"""
    * TB_KRX(한국거래소)
        - 코스피, 코스닥 일별 매매 정보를 DB에 저장하는 스케줄러
        - 스케줄러 주기 : 매일
"""

class SchedulerServiceTBKrx:
    def __init__(self, from_date=None, to_date=None, lookback_days: int = 7):
        # logger request_context내 UUID 직접 할당
        request_context.request_id = str(uuid4())
        self.provision = provision_inject_orm()
        self.krx_api_key = self.provision.KRX_API_KEY
        self.BASE_URLS = {  
            "KOSPI": "http://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd",
            "KOSDAQ": "http://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd",
        }
        # 조회 날짜 범위 설정
        today = datetime.today()
        default_to = (today - timedelta(days=1))
        default_from = default_to

        def _fmt(d):
            if isinstance(d, str):
                return d
            return d.strftime("%Y%m%d")

        if from_date is None:
            self.from_date = _fmt(default_from)
        else:
            self.from_date = _fmt(from_date if not isinstance(from_date, str) else datetime.strptime(from_date, "%Y%m%d"))

        if to_date is None:
            self.to_date = _fmt(default_to)
        else:
            self.to_date = _fmt(to_date if not isinstance(to_date, str) else datetime.strptime(to_date, "%Y%m%d"))

        # from_date가 to_date보다 뒤일 경우 스왑
        if self.from_date > self.to_date:
            logger.warning(f"[TB_KRX] from_date({self.from_date})가 to_date({self.to_date})보다 커서 스왑합니다.")
            self.from_date, self.to_date = self.to_date, self.from_date

        self.lookback_days = int(lookback_days) if isinstance(lookback_days, int) else 7
        attach_error_email_handler(logger, service_name='WEB:KRX 스케줄러')
    def request(self, bas_dd):
      result = []
      for market in ['KOSPI', 'KOSDAQ']:
        headers = {"AUTH_KEY": self.krx_api_key}
        params = {"basDd": bas_dd}
        response = requests.get(self.BASE_URLS[market], headers=headers, params=params)

        data = response.json()
        result.extend(data.get("OutBlock_1"))

      api = pd.DataFrame(result)
      if api.empty:
          logger.warning(f"[TB_KRX] -----> {bas_dd} 데이터가 없습니다.")
          return api
      api.columns = api.columns.str.upper()
      print(api.columns.tolist())  
      return api
        
    def run(self):
        logger.info("[TB_KRX : 한국거래소] -----> 스케줄러 시작")
        start_date = datetime.strptime(self.from_date, "%Y%m%d")
        end_date = datetime.strptime(self.to_date, "%Y%m%d")
        date_list = [(start_date + timedelta(days=i)).strftime("%Y%m%d") for i in range((end_date - start_date).days + 1)]
        combined = []
        try:
            for date in date_list:
                logger.info(f"[TB_KRX] -----> {date} 데이터 요청 중...")
                api_part = self.request(date)
                if not api_part.empty:
                    combined.append(api_part)

            if not combined:
                logger.info("[TB_KRX] -----> 조회된 데이터가 없습니다.")
                return

            api = pd.concat(combined, ignore_index=True)
        except Exception as e:
            logger.error("[TB_KRX : 한국거래소] -----> ERROR : API 요청 실패", exc_info=True)
            logger.info("[KRX : 한국거래소] -----> 스케줄러 종료")
            return

        try:
          with SessionLocal() as conn:
              base_query_factory = BaseQueryFactory(conn, TB_KRX)

              # KRX 테이블에서 최신 1주일 데이터 쿼리
              a_week_ago = (datetime.strptime(self.to_date, "%Y%m%d") - timedelta(days=self.lookback_days)).strftime("%Y%m%d")
              balance_sheets = conn.query(TB_KRX).filter(TB_KRX.BAS_DD >= a_week_ago).all()
              if balance_sheets:
                  db = pd.DataFrame([row.__dict__ for row in balance_sheets])
                  if "_sa_instance_state" in db.columns:
                        db = db.drop(columns=["_sa_instance_state"])
              else:
                  db = pd.DataFrame(columns=['BAS_DD', 'STOCK_CODE'])

              if not db.empty:
                    db["BAS_DD"] = pd.to_datetime(db["BAS_DD"], errors="coerce").dt.strftime("%Y%m%d")
                    db["STOCK_CODE"] = db["STOCK_CODE"].astype(str)
                    diff = set(zip(db["BAS_DD"], db["STOCK_CODE"]))      
              else:
                  diff = set()

              api['BAS_DD'] = api['BAS_DD'].astype(str)
              api['ISU_CD'] = api['ISU_CD'].astype(str)    
              db = pd.DataFrame([row.__dict__ for row in balance_sheets])

              insert_df = api.loc[~api.apply(lambda row: (row['BAS_DD'], row['ISU_CD']) in diff, axis=1)].reset_index(drop=True)

              # 우선주에 해당하는 기업은 제외
              regex = ['5', '7', '9'] + [chr(c) for c in range(ord('K'), ord('Z') + 1)] 
              insert_df = insert_df[~insert_df['ISU_CD'].astype(str).str[-1].isin(regex)]

              instances = []

              for _, row in insert_df.iterrows():
                instance = {
                "STOCK_CODE": row['ISU_CD'],
                "BAS_DD": row['BAS_DD'],
                "ISU_NM": row['ISU_NM'],
                "MKT_NM": row['MKT_NM'],
                "SECT_TP_NM": row['SECT_TP_NM'],
                "TDD_CLSPRC": row['TDD_CLSPRC'],
                "CMPPREVDD_PRC": row['CMPPREVDD_PRC'],
                "FLUC_RT": row['FLUC_RT'],
                "TDD_OPNPRC": row['TDD_OPNPRC'],
                "TDD_HGPRC": row['TDD_HGPRC'],
                "TDD_LWPRC": row['TDD_LWPRC'],
                "ACC_TRDVOL": row['ACC_TRDVOL'],
                "ACC_TRDVAL": row['ACC_TRDVAL'],
                "MKTCAP": row['MKTCAP'],
                "LIST_SHRS": row['LIST_SHRS'],
                }

                instances.append(TB_KRX(**instance))

              if instances:
                  try:
                      base_query_factory.insert_multi_row(instances)
                      logger.info(f"[KRX : 한국거래소] -----> 삽입 : {len(instances)}건 적재 완료")

                  except Exception as e:
                      logger.error(f"[KRX : 한국거래소] -----> ERROR : Insert failed: {e}", exc_info=True)
                      raise
              else:
                  logger.info("[KRX : 한국거래소] -----> 조회된 데이터가 없습니다")

        except Exception as e:
            logger.error(f"[KRX : 한국거래소] -----> ERROR : {e}", exc_info=True)

        logger.info("[KRX : 한국거래소] -----> 스케줄러 종료")