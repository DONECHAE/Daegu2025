from error.errors import DataBaseError
import json
from typing import List, Type, Any
from pydantic import BaseModel, ValidationError
from pg8000 import connect, Connection
import traceback
from Logger import Logger as logger

def custom_parse(src: Any, dest: Type[BaseModel]) -> BaseModel:
    """
    src 데이터를 dest 타입의 Pydantic 모델로 변환
    """
    try:
        # Pydantic의 .parse_obj 메서드를 사용하여 데이터를 변환
        return dest.parse_obj(src)
    except ValidationError as e:
        # 변환이 실패할 경우 ValidationError를 로깅
        logger.error(traceback.format_exc())
        logger.error(f"src : {src}")
        logger.error(f"dest : {dest}")
        raise e
    except Exception as e:
        # 다른 예외가 발생한 경우도 로깅
        logger.error(traceback.format_exc())
        logger.error(f"Unexpected error occurred during parsing.")
        logger.error(f"src : {src}")
        logger.error(f"dest : {dest}")
        raise e

class BaseQueryFactory:
    def __init__(self,conn:Connection):
        self.conn = conn
        # self.conn: Connection = db_connection_pool() #inject.instance(connect)
        self.curs = self.conn.cursor()

    def row_to_dict(self):
        """convert tuple result to dict with cursor"""

        col_names = [i[0]for i in self.curs.description]
        result = [dict(zip(col_names, row)) for row in self.curs.fetchall()]
        return result

    def find_one(self, query: str, dto_type: Type[BaseModel] = None, json_loads_column: str = None) -> Type[BaseModel]:
        try:
            self.curs.execute(query)
            row = self.row_to_dict()[0]
            # logger.info(f"---> query result count : {row} ")
            logger.info(f"---> result type: {type(row)}")
            if json_loads_column:
                if row[json_loads_column]:
                    row[json_loads_column] = json.loads(
                        '{"value":' + row[json_loads_column] + '}')['value']

                return row
            if dto_type:
                result = custom_parse(src=row, dest=dto_type)
                return result
            else:
                return row
        except Exception as err:
            logger.error(f"\n{'%'*120}\n\n{err}\n")
            logger.error(f"\n{'%'*120}\n\n{query}\n{'%'*120}")
            raise DataBaseError(message=str(err))
        
    def find_all(self, query: str, dto_type: Type[BaseModel] = None, json_loads_column: str = None) -> Type[BaseModel]:
        try:

            self.curs.execute(query)
            rows = self.row_to_dict()
            if not rows:
                rows = []
            # logger.info(f"---> query result : {rows} ")
            logger.info(f"---> result type: {type(rows)}")

            if json_loads_column:
                for row in rows:
                    row[json_loads_column] = json.loads(
                        '{"value":' + row[json_loads_column] + '}')['value']

            if dto_type:
                re_rows = []
                for row in rows:
                    result = custom_parse(src=row, dest=dto_type)
                    re_rows.append(result)
                return re_rows
            else:
                return rows
        except Exception as err:
            logger.error(f"\n{'%'*120}\n\n{err}\n")
            logger.error(f"\n{'%'*120}\n\n{query}\n{'%'*120}")
            raise DataBaseError(message=str(err))
    def insert_update(self, query: str) -> bool:
        try:
            self.curs.execute(query)
        except Exception as err:
            logger.error(f"\n{'%'*120}\n\n{err}\n")
            logger.error(f"\n{'%'*120}\n\n{query}\n{'%'*120}")
            self.conn.rollback()
            logger.info("DataBase rollback Complete!!")
            raise DataBaseError(message=str(err))
        else:
            logger.info("DataBase Commit Complete!!")
            self.conn.commit()
            return True
            
    def insert_update_to_select(self, query: str) -> bool:
        try:
            rows = self.curs.execute(query)
            return rows.fetchall()[0]
        except Exception as err:
            logger.error(f"\n{'%'*120}\n\n{err}\n")
            logger.error(f"\n{'%'*120}\n\n{query}\n{'%'*120}")
            self.conn.rollback()
            raise DataBaseError(message=str(err))