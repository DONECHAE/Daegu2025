from pydantic import BaseModel
from setting.dto.provision import Provision
import requests

class FinancialSinglAcntAllRequest(BaseModel):
    crtfc_key: str
    corp_code: str
    bsns_year: str
    reprt_code: str
    fs_div: str

""" OpenDART API를 통해 재무 데이터를 가져오고 DB에 적재하는 서비스 클래스 """
class FinancialSinglAcntAll:
   
    def __init__(self, provision:Provision = None) -> None:
 
        self.singlAcntAll_url = provision.OPENDART_FNLTTSINGLACNTALL_URL
        self.openDart_api_key = provision.OPENDART_API_KEY

    """ OpenDART API를 호출하여 재무제표 데이터를 가져오는 메서드 """
    def openDartApi(self, corp_code:str, bsns_year:str, reprt_code:str, fs_div:str) -> FinancialSinglAcntAllRequest:

        params = {
        "crtfc_key": self.openDart_api_key,
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "fs_div": fs_div
        }

        response = requests.get(self.singlAcntAll_url, params=params)

        if response.status_code == 200:
            data = response.json()
            if data['status'] == "000":
                return data.get('list', [])
            else:
                return None
        else:
            return None
