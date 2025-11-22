# infrastructure/queryFactory/base_orm.py
"""
ORM 기반 DB QueryFactory 모듈
- 해당 모듈 활용 DB 테이블별 CRUD 수행
"""
from error.errors import DataBaseError
from sqlalchemy.orm import Session
from typing import Type, TypeVar, Generic, Optional, List
from Logger import logger

# TypeVar를 사용하여 모델 타입을 제네릭으로 지정
T = TypeVar("T")

class BaseQueryFactory(Generic[T]):
    def __init__(self,conn:Session, model: Type[T]):
        self.conn = conn
        self.model = model
        
    def find_one(self, **filters) -> Optional[T]:
        try:
            return self.conn.query(self.model).filter_by(**filters).one()
        except:
            self.conn.rollback()
            return None
        
    def find_all(self, **filters) -> List[T]:
        try:
            return self.conn.query(self.model).filter_by(**filters).all()
        except:
            self.conn.rollback()
            return None
        
    def find_all_contains(self, **filters) -> List[T]:
        try:
            return self.conn.query(self.model).filter(
                *[getattr(self.model, field_name).contains(value) for field_name, value in filters.items()]
            ).all()
        except:
            self.conn.rollback()
            return None
        
    def find_all_in(self, column_name: str, values: List) -> List[T]:
        """지정된 컬럼이 값 리스트에 포함된 모든 레코드 조회"""
        try:
            if not values:
                return []
            return self.conn.query(self.model).filter(
                getattr(self.model, column_name).in_(values)
            ).all()
        except Exception as e:
            self.conn.rollback()
            return None

    def insert_single_row(self,**data) -> T:
        try:
            instance = self.model(**data)
            self.conn.add(instance)
            self.conn.commit()
            self.conn.refresh(instance)
            return instance
        except Exception as e:
            self.conn.rollback()
            logger.error(f"DB Error {e}")
            raise DataBaseError(message="데이터베이스 에러 발생")
    
    def insert_multi_row(self,data_lst:List[T]) -> T:
        try:
            self.conn.add_all(data_lst)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"DB Error {e}")
            raise DataBaseError(message="데이터베이스 에러 발생")
    
    def update(self, instance: T, **data) -> T:
        for key, value in data.items():
            setattr(instance, key, value)
        self.conn.commit()
        self.conn.refresh(instance)
        return instance
    
    def get_columns_by_names(self, *column_names: str):
        """
        하나 또는 여러 개의 컬럼명을 입력받아 해당 컬럼 객체(들)를 반환
        - 1개: 단일 객체 반환
        - 2개 이상: 리스트로 반환
        """
        columns = [getattr(self.model, name) for name in column_names if hasattr(self.model, name)]
        if len(column_names) == 1:
            return columns[0] if columns else None
        return columns