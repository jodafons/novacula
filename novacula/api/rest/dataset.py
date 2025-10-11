
__all__ = [
    "APIClient",
    "get_session_api",
]

import requests

from typing import Union, List
from tabulate import tabulate
from urllib.parse import urljoin
from novacula import schemas
from novacula.exceptions import ConnectionError, TokenNotValidError


__api_session = None



class DatasetAPIClient:

    def __init__(self, api_client ):
        self.__api_client = api_client
        
    def create(
        self, 
        name        : str, 
        description : str,
    ):

        payload = {
            'name'         : name,
            'description'  : description,
        }
        payload = {"params_str": schemas.json_encode(payload)}
        res = self.session.put(f"/dataset/options/create", data=payload)

        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        else:
            dataset_id = res[2].json()
            logger.debug(f"Received dataset_id: {dataset_id}")
            return dataset_id

        