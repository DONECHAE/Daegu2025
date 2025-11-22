"""
ORM 기반 DB QueryFactory 모듈
기업 개황 테이블에서 기업 고유번호 추출하는 로직
- IS_ACTIVE: 기업활동 여부
- IS_CALCULATE: 지표산출 여부
- CORP_CLS: 기업 구분 (Y: 코스피, K: 코스닥)
"""
from sqlalchemy.orm import Session
from db.public.models import TB_COMPANY
from infrastructure.queryFactory.base_orm import BaseQueryFactory


class TBCompanyQueryFactory(BaseQueryFactory):
    def __init__(self, conn:Session):       
        super().__init__(conn=conn, model=TB_COMPANY)
        
    def corp_code(self):
        try:
            return self.conn.query(self.model.CORP_CODE, self.model.ACC_MT).filter(
                self.model.IS_ACTIVE == True,
                self.model.IS_CALCULATE == True,
                self.model.CORP_CLS.in_(['Y', 'K'])
            ).all()
        except:
            self.conn.rollback()
            return None
        
        

        

        

        