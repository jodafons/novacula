
__all__ = [
    "Provider",
    "APIClient",
    "get_session_api",
]

import requests

from typing import Union, List
from tabulate import tabulate
from urllib.parse import urljoin
from maestro import schemas
from maestro.exceptions import RemoteCreationError, ConnectionError, TokenNotValidError


from .rest.dataset import DatasetAPIClient
from .rest.image import ImageAPIClient
from .rest.task import TaskAPIClient
from maestro.api.models.task import Group
from maestro import schemas

__api_session = None

def get_session_api(host:str=None, token: str=None):
    global __api_session
    if not __api_session:
        if not host or not token:
            raise RemoteCreationError
        __api_session = APIClient(host,token)
    return __api_session



class APIClient:

    def __init__(self, host : str, token : str):
        self.session = requests.Session()
        self.__token = token
        self.host = host
        res = requests.get(f"{self.host}/status")
        if res.status_code != 200:
            raise ConnectionError
        payload = {"params_str": schemas.json_encode( {"token":self.__token} ) }
        res = requests.put(f"{self.host}/remote/user/token", data=payload)
        if res.status_code != 200:
            raise TokenNotValidError
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

    def image(self) -> ImageAPIClient:
        return ImageAPIClient(self)

    def dataset(self) -> DatasetAPIClient:
        return DatasetAPIClient(self)

    def task(self) -> TaskAPIClient:
        return TaskAPIClient(self)
    
    


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


    def list_tasks(self, match_with : str="*") -> List[schemas.TaskInfo]:
        """
        
        """
        return self.__api_client.task().list(match_with=match_with)


    def list_datasets(self, match_with : str="*") -> List[schemas.Dataset]:
        """
        
        """
        return self.__api_client.dataset().list(match_with=match_with)
 
 
    def print_tasks( self, match_with : str="*" ) -> str:
        """
        
        """
        tasks = [ [task.task_id, task.name, task.status] for task in self.list_tasks(match_with)]
        tasks = tabulate(tasks, headers= ["id", "name", "type", "status"],  tablefmt="psql")
        print(tasks) 


    def print_datasets( self, match_with : str="*" ) -> str:
        """
        
        """
        datasets = [ [dataset.dataset_id , dataset.name, len(dataset.files), dataset.data_type] for dataset in self.list_datasets(match_with) ]
        datasets = tabulate(datasets, headers= ["id", "name", "files", "type"],  tablefmt="psql")
        print(datasets)   
        
        
    def create( self, tasks : List[schemas.TaskInputs]) -> Group:
        """
        
        """
        return self.__api_client.task().create(tasks)


    def retrieve_tasks( self, match_with : str) -> Group:
        tasks = self.list_tasks(match_with=match_with)
        return Group( [task.task_id for task in tasks]) if len(tasks)>0 else None