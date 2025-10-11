
__all__ = [
    "APIClient",
    "get_session_api",
    "set_session_api"
]

import requests

from typing import Union, List
from tabulate import tabulate
from urllib.parse import urljoin
from novacula import schemas
from novacula.exceptions import ConnectionError, TokenNotValidError
from novacula.api.rest.dataset import DatasetAPIClient

__api_session = None


class APIClient:

    def __init__(self, host : str, user : str, access_key : str):
        self.session     = requests.Session()
        self.access_key  = access_key
        self.host        = host
        res = requests.get(f"{self.host}/status")
        if res.status_code != 200:
            raise ConnectionError
        self.headers = {"Connection": "Keep-Alive", "Keep-Alive": "timeout=1000, max=1000", 
                        "access_key" : access_key, 'user': user}
        #res = requests.put(f"{self.host}/remote/user/authenticate", data=payload)
        #if res.status_code != 200:
        #    raise TokenNotValidError


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


def set_session_api( host:str, user : str, access_key : str) -> APIClient:
    global __api_session
    __api_session = APIClient(host,user,access_key)
    return __api_session


def get_session_api() -> APIClient:
    global __api_session
    return __api_session


