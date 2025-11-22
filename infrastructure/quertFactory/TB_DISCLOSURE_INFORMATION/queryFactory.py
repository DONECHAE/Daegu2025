from sqlalchemy.orm import Session
from db.public.models import TB_CRT_CVT
from infrastructure.queryFactory.base_orm import BaseQueryFactory


class TBCrtCvtQueryFactory(BaseQueryFactory):
    def __init__(self, conn:Session):       
        super().__init__(conn=conn, model=TB_CRT_CVT)
        
    def corp_code(self):
        try:
            return self.conn.query(self.model.CORP_CODE, self.model.BSNS_YEAR,self.model.REPRT_CODE).filter(
            ).all()
        except:
            self.conn.rollback()
            return None
        
        

        

        

        