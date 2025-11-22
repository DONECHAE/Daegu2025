"""
PostgreSQL ORM 모델 정의
스키마: public
"""
from sqlalchemy import Column, Integer, Boolean, Text, Date, ForeignKey, String, ARRAY
from sqlalchemy.orm import relationship
from db.base import Base

# 기업 개황 
class TB_COMPANY(Base):
    __tablename__ = "TB_COMPANY"
    STOCK_CODE = Column(Text, primary_key=True)
    CORP_CODE = Column(Text, nullable=True)
    CORP_NAME = Column(Text, nullable=True)
    CORP_NAME_ENG = Column(Text, nullable=True)
    CORP_CLS = Column(Text, nullable=True)
    CEO_NM = Column(Text, nullable=True)
    JURIR_NO = Column(Text, nullable=True)
    BIZR_NO = Column(Text, nullable=True)
    EMPLOYEE = Column(Text, nullable=True)
    ADRES = Column(Text, nullable=True)
    PHN_NO = Column(Text, nullable=True)
    INDUTY_CODE = Column(Text, nullable=True)
    EST_DT = Column(Date, nullable=True)
    ACC_MT = Column(Text, nullable=True)
    IS_ACTIVE = Column(Boolean, nullable=True)
    IS_CALCULATE = Column(Boolean, nullable=True)
    # 관계 (STOCK_CODE를 참조하는 테이블들)
    disclosures = relationship("TB_DISCLOSURE_INFORMATION", back_populates="company")
    investment_warnings = relationship("TB_INVESTMENT_WARNING", back_populates="company")
    investment_attentions = relationship("TB_INVESTMENT_ATTENTION", back_populates="company")
    embezzlements = relationship("TB_EMBEZZLEMENT", back_populates="company")
    unfaithful_disclosures = relationship("TB_UNFAITHFUL_DISCLOSURE", back_populates="company")
    delistings = relationship("TB_DELISTING", back_populates="company")
    krx_data = relationship("TB_KRX", back_populates="company")

# 단일회사전체재무제표
class TB_FINANCIAL_STATEMENTS(Base):
    __tablename__ = "TB_FINANCIAL_STATEMENTS"
    ID = Column(Integer, primary_key=True)
    CORP_CODE = Column(Text, nullable=True)
    RCEPT_NO = Column(Text, nullable=True)
    REPRT_CODE = Column(Text, nullable=True)
    BSNS_YEAR = Column(Text, nullable=True)
    SJ_DIV = Column(Text, nullable=True)
    SJ_NM = Column(Text, nullable=True)
    ACCOUNT_ID = Column(Text, nullable=True)
    ACCOUNT_NM = Column(Text, nullable=True)
    ACCOUNT_DETAIL = Column(Text, nullable=True)
    THSTRM_NM = Column(Text, nullable=True)
    THSTRM_AMOUNT = Column(Text, nullable=True)
    THSTRM_ADD_AMOUNT = Column(Text, nullable=True)
    FRMTRM_NM = Column(Text, nullable=True)
    FRMTRM_AMOUNT = Column(Text, nullable=True)
    BFEFRMTRM_NM = Column(Text, nullable=True)
    BFEFRMTRM_AMOUNT = Column(Text, nullable=True)
    FRMTRM_Q_NM = Column(Text, nullable=True)
    FRMTRM_Q_AMOUNT = Column(Text, nullable=True)
    FRMTRM_ADD_AMOUNT = Column(Text, nullable=True)
    ORD = Column(Integer, nullable=True)
    CURRENCY = Column(Text, nullable=True)
    FS_DIV = Column(Text, nullable=True)

# 공시정보
class TB_DISCLOSURE_INFORMATION(Base):
    __tablename__ = "TB_DISCLOSURE_INFORMATION"
    ID = Column(Integer, primary_key=True)
    STOCK_CODE = Column(Text, ForeignKey("TB_COMPANY.STOCK_CODE"), nullable=False)
    CORP_CODE = Column(Text, nullable=True)
    CORP_NAME = Column(Text, nullable=True)
    CORP_CLS = Column(Text, nullable=True)
    REPORT_NM = Column(Text, nullable=True)
    RCEPT_NO = Column(Text, nullable=True)
    FLR_NM = Column(Text, nullable=True)
    RCEPT_DT = Column(Date, nullable=True)
    RM = Column(Text, nullable=True)
    OFS_COMMENT = Column(Text, nullable=True)
    CFS_COMMENT = Column(Text, nullable=True)
    CRT_CVT_COMMENT = Column(Text, nullable=True)
    # Relationship to TB_COMPANY
    company = relationship("TB_COMPANY", back_populates="disclosures")

# 투자주의/경고/위험
class TB_INVESTMENT_WARNING(Base):
    __tablename__ = "TB_INVESTMENT_WARNING"
    ID = Column(Integer, primary_key=True)
    STOCK_CODE = Column(Text, ForeignKey("TB_COMPANY.STOCK_CODE"), nullable=False)
    CORP_NAME = Column(Text, nullable=True)
    TYPE = Column(Text, nullable=True)
    POST_DATE = Column(Date, nullable=True)
    DESIGNATED_DATE = Column(Date, nullable=True)
    CANCLE_DATE = Column(Date, nullable=True)
    CATEGORY = Column(Text, nullable=True)
    # Relationship to TB_COMPANY
    company = relationship("TB_COMPANY", back_populates="investment_warnings")

# 투자주의환기종목
class TB_INVESTMENT_ATTENTION(Base):
    __tablename__ = "TB_INVESTMENT_ATTENTION"
    ID = Column(Integer, primary_key=True)
    STOCK_CODE = Column(Text, ForeignKey("TB_COMPANY.STOCK_CODE"), nullable=False)
    CORP_NAME = Column(Text, nullable=True)
    DATE = Column(Date, nullable=True)
    REASON = Column(Text, nullable=True)
    # Relationship to TB_COMPANY
    company = relationship("TB_COMPANY", back_populates="investment_attentions")

# 횡령
class TB_EMBEZZLEMENT(Base):
    __tablename__ = "TB_EMBEZZLEMENT"
    ID = Column(Integer, primary_key=True)
    STOCK_CODE = Column(Text, ForeignKey("TB_COMPANY.STOCK_CODE"), nullable=False)
    CORP_NAME = Column(Text, nullable=True)
    TITLE = Column(Text, nullable=True)
    DATE = Column(Date, nullable=True)
    # Relationship to TB_COMPANY
    company = relationship("TB_COMPANY", back_populates="embezzlements")

# GDP 성장률 추정
class TB_MACROECONOMIC_GDP(Base):
    __tablename__ = "TB_MACROECONOMIC_GDP"
    ID = Column(Integer, primary_key=True)
    VALUE = Column(Text, nullable=True)
    DATE = Column(Date, nullable=True)

# 불성실 공시 
class TB_UNFAITHFUL_DISCLOSURE(Base):
    __tablename__ = "TB_UNFAITHFUL_DISCLOSURE"
    ID = Column(Integer, primary_key=True)
    STOCK_CODE = Column(Text, ForeignKey("TB_COMPANY.STOCK_CODE"), nullable=False)
    CORP_NAME = Column(Text, nullable=True)
    DEMERIT = Column(Text, nullable=True)
    SANCTIONS_AMT = Column(Text, nullable=True)
    OFFICER_CHANGE = Column(Text, nullable=True)
    TYPE = Column(Text, nullable=True)
    DATE = Column(Date, nullable=True)
    REASON = Column(Text, nullable=True)
    # Relationship to TB_COMPANY
    company = relationship("TB_COMPANY", back_populates="unfaithful_disclosures")

# 개인소비지출 물가지수
class TB_PCE_INFLATION(Base):
    __tablename__ = "TB_PCE_INFLATION"
    ID = Column(Integer, primary_key=True)
    VALUE = Column(Text, nullable=True)
    DATE = Column(Date, nullable=True)

# 부도발생
class TB_BANKRUPTCY(Base):
    __tablename__ = "TB_BANKRUPTCY"
    ID = Column(Integer, primary_key=True)
    CORP_CODE = Column(Text, nullable=True)
    RCEPT_NO = Column(Text, nullable=True)
    CORP_CLS = Column(Text, nullable=True)
    CORP_NAME = Column(Text, nullable=True)
    DF_CN = Column(Text, nullable=True)
    DF_AMT = Column(Text, nullable=True)
    DF_BNK = Column(Text, nullable=True)
    DFD = Column(Date, nullable=True)
    DF_RS = Column(Text, nullable=True)

# 프로비저닝(API, URL 관리)
class TB_PROVISION_CONFIG(Base):
    __tablename__ = "TB_PROVISION_CONFIG"
    ID = Column(Integer, primary_key=True)
    MEMO = Column(Text, nullable=True)
    CONFIG_NAME = Column(Text, nullable=True)
    CONFIG_VALUE = Column(Text, nullable=True)
    EXPIRATION_DATE = Column(Date, nullable=True)

# 미국채 10년물 이자율
class TB_TREASURY_SECURITY(Base):
    __tablename__ = "TB_TREASURY_SECURITY"
    ID = Column(Integer, primary_key=True)
    VALUE = Column(Text, nullable=True)
    DATE = Column(Date, nullable=True)
    IS_FRIDAY = Column(Boolean, nullable=True)

# 상장폐지
class TB_DELISTING(Base):
    __tablename__ = "TB_DELISTING"
    ID = Column(Integer, primary_key=True)
    STOCK_CODE = Column(Text, ForeignKey("TB_COMPANY.STOCK_CODE"), nullable=False)
    CORP_NAME = Column(Text, nullable=True)
    DATE = Column(Date, nullable=True)
    REASON = Column(Text, nullable=True)
    RM = Column(Text, nullable=True)
    # Relationship to TB_COMPANY
    company = relationship("TB_COMPANY", back_populates="delistings")

# 한국거래소
class TB_KRX(Base):
    __tablename__ = "TB_KRX"
    ID = Column(Integer, primary_key=True)
    STOCK_CODE = Column(Text, ForeignKey("TB_COMPANY.STOCK_CODE"), nullable=False)
    BAS_DD = Column(Date, nullable=True)
    ISU_NM = Column(Text, nullable=True)
    MKT_NM = Column(Text, nullable=True)
    SECT_TP_NM = Column(Text, nullable=True)
    TDD_CLSPRC = Column(Text, nullable=True)
    CMPPREVDD_PRC = Column(Text, nullable=True)
    FLUC_RT = Column(Text, nullable=True)
    TDD_OPNPRC = Column(Text, nullable=True)
    TDD_HGPRC = Column(Text, nullable=True)
    TDD_LWPRC = Column(Text, nullable=True)
    ACC_TRDVOL = Column(Text, nullable=True)
    ACC_TRDVAL = Column(Text, nullable=True)
    MKTCAP = Column(Text, nullable=True)
    LIST_SHRS = Column(Text, nullable=True)
    # Relationship to TB_COMPANY
    company = relationship("TB_COMPANY", back_populates="krx_data")

# 재무변수
class TB_FINANCIAL_VARIABLE(Base):
    __tablename__ = "TB_FINANCIAL_VARIABLE"

    ID = Column(Integer, primary_key=True, autoincrement=True, index=True)
    CORP_CODE = Column(String, nullable=True)
    RCEPT_NO = Column(String, nullable=True)
    REPRT_CODE = Column(String, nullable=True)
    BSNS_YEAR = Column(String, nullable=True)
    ACCOUNT_NM = Column(String, nullable=True)
    ACCOUNT_AMOUNT = Column(String, nullable=True)  
    IS_LLM = Column(Boolean, nullable=True, default=False)
    IS_COMPLETE = Column(Boolean, nullable=True, default=False)

# 위험정보
class TB_CRT_CVT(Base):
    __tablename__ = "TB_CRT_CVT"

    ID = Column(Integer, primary_key=True, autoincrement=True, index=True)
    CORP_CODE = Column(String, nullable=True)
    REPRT_CODE = Column(String, nullable=True)
    BSNS_YEAR = Column(String, nullable=True)
    CRT = Column(ARRAY(String), nullable=True)  
    CVT = Column(ARRAY(String), nullable=True)

class TB_MANAGEMENT(Base):
    __tablename__ = "TB_MANAGEMENT"
    ID = Column(Integer, primary_key=True, autoincrement=True, index=True)
    CORP_NAME = Column(String, nullable=True)
    DATE = Column(Date, nullable=True)
    REASON = Column(Text, nullable=True)
    
# 신용위험 주석
class TB_CRT_COMMENT(Base):
    __tablename__ = "TB_CRT_COMMENT"

    ID = Column(Integer, primary_key=True, autoincrement=True)
    CRT = Column(String, unique=True, nullable=False)
    FINANCIAL_RED_FLAG = Column(Text, nullable=True)
    SCORING = Column(Text, nullable=True)
    GROUPING = Column(Text, nullable=True)
    COMMENTS = Column(Text, nullable=True)
    ACCOUNT_NM = Column(ARRAY(String), nullable=True)  
    
# 
class TB_CRT_NEWS(Base):
    __tablename__ = "TB_CRT_NEWS"

    ID = Column(Integer, primary_key=True, autoincrement=True)
    CORP_CODE = Column(String, nullable=True)
    REPRT_CODE = Column(String, nullable=True)
    DATE = Column(Date, nullable=True)
    RESULT_BEGINNER = Column(Text, nullable=True) 
    RESULT_PRO = Column(Text, nullable=True)     
    CRT = Column(ARRAY(String), nullable=True)  
    CVT = Column(ARRAY(String), nullable=True)  