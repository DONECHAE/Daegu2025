import json
import requests
from datetime import datetime, timedelta

from sqlalchemy import func  # 최신 날짜 조회에 필요

from infrastructure.queryFactory.base_orm import BaseQueryFactory
from setting.database_orm import SessionLocal
from db.public.models import TB_TREASURY_SECURITY
from Logger import logger 
from setting.inject import provision_inject_orm
from error.fred.errors import FRED_ERROR_MESSAGES  
from error.email.email_logger import attach_error_email_handler

"""
    * TB_TREASURY_SECURITY (미국 국채금리 예: DGS10)
      - 스케줄러 주기: 매일
"""

class SchedulerServiceTBTreasurySecurity:
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, series_ids=None):
        self.series_ids = ["DGS10"] if series_ids is None else series_ids
        self.provision = provision_inject_orm()
        self.api_key = self.provision.FRED_API_KEY
        attach_error_email_handler(logger, service_name='FRED:DGS10 스케줄러')

    def _get_latest_date(self, session):
   
        try:
            latest = session.query(func.max(TB_TREASURY_SECURITY.DATE)).scalar()
            return latest
        except Exception as e:
            logger.error(f"[FRED] 최신 DATE 조회 실패: {e}", exc_info=True)
            return None


    def _fetch_observations(self, series_id: str) -> list:
        params = {
            "api_key": self.api_key,
            "series_id": series_id,
            "file_type": "json",
            "sort_order": "asc",
        }

        resp = requests.get(self.BASE_URL, params=params, timeout=30)

        if resp.status_code != 200:
            msg = FRED_ERROR_MESSAGES.get(resp.status_code, "Unknown error")
            logger.error(f"[FRED] HTTP 오류: {resp.status_code} ({msg}) (series_id={series_id})", exc_info=True)
            raise RuntimeError(f"FRED API 요청 실패: HTTP {resp.status_code}")

        try:
            data = resp.json()
        except json.JSONDecodeError:
            logger.error(f"[FRED] JSON 파싱 실패 (series_id={series_id})", exc_info=True)
            raise

        observations = data.get("observations", [])
        if not observations:
            logger.info(f"[FRED] 관측치 없음 (series_id={series_id})")
        return observations


    def _filter_new_observations(self, observations: list, latest_date) -> list:
        """DB 최신 날짜(latest_date) 이후의 관측치만 필터링"""
        if not latest_date:
            logger.info("[FRED] DB 비어있음 → 전체 삽입 대상")
            return observations

        new_obs = []
        for obs in observations:
            try:
                obs_date = datetime.strptime(obs["date"], "%Y-%m-%d").date()
            except Exception:
                continue
            if obs_date > latest_date:
                new_obs.append(obs)

        if not new_obs:
            logger.info(f"[FRED] 최신 DATE({latest_date}) 이후 신규 관측치 없음")
        return new_obs


    def _postprocess(self, observations: list) -> None:
        for obs in observations:
            try:
                obs_date = datetime.strptime(obs["date"], "%Y-%m-%d").date()
                obs["IS_FRIDAY"] = (obs_date.weekday() == 4)  # 월(0)~일(6), 4=금
            except Exception:
                obs["IS_FRIDAY"] = None


    def _map_to_model_instances(self, observations: list):
        instances = []
        for obs in observations:
            obs = {k.upper(): v for k, v in obs.items()}
            raw_value = obs.get("VALUE")
            value_text = None if raw_value in (None, ".", "") else str(raw_value)

            try:
                obs_date = datetime.strptime(obs["DATE"], "%Y-%m-%d").date()
            except Exception:
                continue

            instances.append(
                TB_TREASURY_SECURITY(
                    DATE=obs_date,          
                    VALUE=value_text,       
                    IS_FRIDAY=obs.get("IS_FRIDAY"),
                )
            )
        return instances

    def _insert_observations(self, session, observations: list) -> int:
        factory = BaseQueryFactory(conn=session, model=TB_TREASURY_SECURITY)
        instances = self._map_to_model_instances(observations)
        if not instances:
            return 0
        logger.info(f"[FRED] 삽입 시작 (건수: {len(instances)})")
        factory.insert_multi_row(instances)
        logger.info(f"[FRED] 삽입 완료")
        return len(instances)

    def run(self):
        logger.info(f"[FRED] 국채금리 스케줄러 시작 (series={self.series_ids})")

        with SessionLocal() as session:
            total_inserted = 0

            latest_date = self._get_latest_date(session)
            if latest_date:
                logger.info(f"[FRED] DB 최신 DATE: {latest_date}")

            for sid in self.series_ids:
                try:
                    all_obs = self._fetch_observations(series_id=sid)       
                    new_obs = self._filter_new_observations(all_obs, latest_date)  

                    if not new_obs:
                        continue

                    self._postprocess(new_obs)                              
                    total_inserted += self._insert_observations(session=session, observations=new_obs)

                except Exception as e:
                    logger.error(f"[FRED] 시리즈 처리 실패: {sid} / {e}", exc_info=True)

        logger.info(f"[FRED] 스케줄러 완료 (삽입: {total_inserted}건)")

