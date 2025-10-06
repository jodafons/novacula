
__all__ = [
    "Provider",
    "APIClient",
    "get_session_api",
]

import requests

from typing import Union, List
from tabulate import tabulate
from urllib.parse import urljoin
from .rest.dataset import DatasetAPIClient


__api_session = None

def get_session_api(host:str=None, token: str=None):
    global __api_session
    if not __api_session:
        __api_session = APIClient(host,token)
    return __api_session



class APIClient:

    def __init__(self, host : str, token : str):
        self.session = requests.Session()
        self.__token = token
        self.host = host
        #res = requests.get(f"{self.host}/status")
        #if res.status_code != 200:
        #    raise ConnectionError
        #payload = {"params_str": schemas.json_encode( {"token":self.__token} ) }
        #res = requests.put(f"{self.host}/remote/user/token", data=payload)
        #if res.status_code != 200:
        #    raise TokenNotValidError
        self.headers = {"Connection": "Keep-Alive", "Keep-Alive": "timeout=1000, max=1000", "token" : self.__token}

    def post(self, path, data, files=None):
        url = urljoin(self.host, path)
        req = self.session.post(url, headers=self.headers, data=data) if not files else self.session.post(url, headers=self.headers, data=data, files=files)
        return (req.status_code, req.reason, req)

    def put(self, path, data, files=None):
        url = urljoin(self.host, path)
        req = self.session.put(url, headers=self.headers, data=data) if not files else self.session.put(url, headers=self.headers, data=data, files=files)
        return (req.status_code, req.reason, req)

    def get(self, path : str, stream : bool=False):
        url = urljoin(self.host, path)
        req = self.session.get(url, headers=self.headers, stream=stream)
        return (req.status_code, req.reason, req)

    def __call__(self):
        return self.session

    def dataset(self) -> DatasetAPIClient:
        return DatasetAPIClient(self)



class Provider:
    r"""
   
    """
    def __init__(
        self, 
        host: str,
        token : str, 
    ):
        self.host = host
        self.__api_client = get_session_api( host, token )

