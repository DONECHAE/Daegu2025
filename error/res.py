"""
FastAPI APP 최종 Endpoint 유저가 알아야할 에러가 있는 경우 해당 모듈에서 정의
Error Response 모델 정의
"""

from pydantic import BaseModel

class ErrorResponse(BaseModel):
    detail: str