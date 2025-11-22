from error.errors import DataBaseError
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect
from sqlalchemy import func
from typing import Type, TypeVar, Generic, Optional, List
from Logger import Logger as logger
from datetime import datetime, timedelta
T = TypeVar("T")

class TBFINANCIALQueryFactory(Generic[T]):
    def __init__(self, conn: Session, model: Type[T]):
        self.conn = conn
        self.model = model
        self.primary_key = inspect(model).primary_key[0].name if inspect(model).primary_key else None
        
    def find_corp_codes_with_majority_changes_twice_in_year(self, year: int) -> List[T]:
            from sqlalchemy import func
            try:
                base_date = datetime(year, 1, 1)
                subquery = (
                    self.conn.query(self.model.CORP_CODE)
                    .filter(
                        self.model.REPORT_NM.in_(['최대주주변경', '[기재정정]최대주주변경']),
                        self.model.RCEPT_DT >= base_date - timedelta(days=365),
                        self.model.RCEPT_DT < base_date,
                        func.length(self.model.RM) == 1
                    )
                    .group_by(self.model.CORP_CODE)
                    .having(func.count('*') >= 2)
                    .subquery()
                )

                result = (
                    self.conn.query(self.model.CORP_NAME, self.model.CORP_CODE, self.model.STOCK_CODE)
                    .filter(
                        self.model.CORP_CODE.in_(subquery),
                        self.model.REPORT_NM.in_(['최대주주변경', '[기재정정]최대주주변경']),
                        self.model.RCEPT_DT >= base_date - timedelta(days=365),
                        self.model.RCEPT_DT < base_date,
                        func.length(self.model.RM) == 1
                    )
                    .distinct()
                    .order_by(self.model.CORP_NAME)
                    .all()
                )
                return result
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB Error {e}")
                raise DataBaseError(message="데이터 조회 중 오류 발생")

    def find_corp_codes_with_small_public_offering(self, year: int) -> List[T]:
            try:
                base_date = datetime(year, 1, 1)
                result = (
                    self.conn.query(self.model.CORP_NAME, self.model.CORP_CODE, self.model.STOCK_CODE)
                    .filter(
                        self.model.REPORT_NM == '소액공모실적보고서',
                        self.model.RCEPT_DT >= base_date - timedelta(days=730),
                        self.model.RCEPT_DT < base_date
                    )
                    .distinct()
                    .order_by(self.model.CORP_NAME)
                    .all()
                )
                return result
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB Error {e}")
                raise DataBaseError(message="데이터 조회 중 오류 발생")

    def get_krx_marketcap_data(self, years: List[int], stock_codes: Optional[List[str]] = None) -> List[dict]:
            """
            TB_KRX에서 연도 및 (선택적으로) 종목코드 기준으로 BAS_DD, STOCK_CODE, MKTCAP 조회
            """
            from sqlalchemy import or_, and_
            from datetime import date
            try:
                if self.model.__tablename__.upper() != "TB_KRX":
                    raise DataBaseError(message="get_krx_marketcap_data는 TB_KRX 테이블에만 사용 가능합니다.")
                
                query = self.conn.query(
                    self.model.BAS_DD,
                    self.model.STOCK_CODE,
                    self.model.MKTCAP
                )

                year_filters = [
                    self.model.BAS_DD.between(date(year, 1, 1), date(year, 12, 31)) for year in years
                ]
                query = query.filter(or_(*year_filters))

                query = query.filter(self.model.MKT_NM.in_(["KOSPI", "KOSDAQ"]))

                if stock_codes:
                    query = query.filter(self.model.STOCK_CODE.in_(stock_codes))

                result = query.all()
                return [
                    {"BAS_DD": row[0], "STOCK_CODE": row[1], "MKTCAP": row[2]}
                    for row in result
                ]
            except Exception as e:
                self.conn.rollback()
                logger.error(f"DB Error {e}")
                raise DataBaseError(message="TB_KRX 마켓캡 데이터 조회 중 오류 발생")