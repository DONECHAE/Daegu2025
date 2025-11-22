import requests
from zipfile import ZipFile
from io import BytesIO
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from Logger import logger

"""
OPENDART API 요청 로직 정의(https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001)
"""

# OPENDART - 기업 고유번호 조회(https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018)
def opendart_corp_code(api_key:str):
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": api_key}

    response = requests.get(url, params=params)

    try:
        # ZIP 파일 내의 XML 파일 추출 (기본 이름: CORPCODE.xml)
        with ZipFile(BytesIO(response.content)) as zip_file:
            zip_file.extractall(".")

        # XML 파일 파싱
        tree = ET.parse("CORPCODE.xml")
        root = tree.getroot()

        data = []
        for company in root.findall("list"):
            corp_code = company.find("corp_code").text
            corp_name = company.find("corp_name").text
            stock_code = company.find("stock_code").text
            modify_date = company.find("modify_date").text
            
            data.append([corp_code, corp_name, stock_code, modify_date])  

        corp_code_df = pd.DataFrame(data, columns=["corp_code", "corp_name", "stock_code", "modify_date"])
        # 주식회사만 필터링
        corp_code_df = corp_code_df.loc[corp_code_df['stock_code'] != ' '].reset_index(drop=True)
        # 수정일자를 기준으로 최근 기업 고유번호만 필터링
        corp_code_df = corp_code_df.sort_values('modify_date', ascending=False).drop_duplicates('corp_name', keep='first').reset_index(drop=True)
        return corp_code_df
    except Exception as e:
        logger.error(f"[TB_COMPANY : 기업정보] -----> ERROR : {e}")


# OPENDART - 기업 개황 조회(https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019002)
def opendart_company_api(api_key:str, corp_code:str):
    url = "https://opendart.fss.or.kr/api/company.json"
    params = {
        "crtfc_key": api_key,
        'corp_code': corp_code
    }
    response = requests.get(url, params=params)
    return response.json()

# OPENDART - 부도발생 조회(https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS005&apiId=2020019)
def opendart_bankruptcy_api(api_key:str, corp_code:str):
    url = "https://opendart.fss.or.kr/api/dfOcr.json"
    params = {
        'crtfc_key': api_key,   # 인증키
        'corp_code': corp_code, # 회사 고유번호
        'bgn_de': datetime.today().strftime('%Y%m%d'), # 시작일(최초접수일 - 20240501)
        'end_de': datetime.today().strftime('%Y%m%d'), # 종료일(최초접수일 - 20240501)
    }
    response = requests.get(url, params=params)
    return response.json()

# OPENDART - 공시정보 조회(https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019001)
def opendart_disclosure_api(api_key:str, corp_code:str, pblntf_ty:str="A", pblntf_detail_ty:str="", bgn_de:str=None, end_de:str=None):
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
        'bgn_de': (datetime.today() - timedelta(days=3)).strftime("%Y%m%d"),
        'end_de': datetime.today().strftime("%Y%m%d"),
        'pblntf_ty': pblntf_ty,
        'pblntf_detail_ty':pblntf_detail_ty,
        'last_reprt_at': 'N',
        'page_no': 1,
        'page_count': 100,
    }
    response = requests.get(url, params=params)
    return response.json()

# OPENDART - 단일회사 전체 재무제표 조회(https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020)
def opendart_financial_api(api_key:str, corp_code:str, bsns_year:str, reprt_code:str, fs_div:str):
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

    params = {
        'crtfc_key': api_key,
        'corp_code': corp_code,
        'bsns_year': bsns_year,
        'reprt_code': reprt_code,
        'fs_div': fs_div,
    }
    response = requests.get(url, params=params, timeout=60)
    return response.json()

def dart_report(api_key: str, corp_code: str, key_word: str, bsns_year: str, reprt_code: str = "11011"):
    key_word_map = {
        "직원": "empSttus",
    }

    url = f"https://opendart.fss.or.kr/api/{key_word_map[key_word]}.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
    }
    response = requests.get(url, params=params, timeout=60)
    return response.json()
    







