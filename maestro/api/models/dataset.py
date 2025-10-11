__all__ = ["Dataset"]


from tabulate import tabulate
from typing import List, Union, Dict
from maestro.api.client import get_session_api
from maestro import schemas


class Dataset:
    '''
  
    '''

    def __init__(
        self,
        name        : str,
        description : str="",
    ) -> None:
        
        self.__api_client = get_session_api()
        self.name         = name
        self.description  = description
        if self.__api_client.dataset().check_existence(name):
            self.dataset_id = self.__api_client.dataset().identity(name)
        else:
            self.dataset_id = None

    def print(self):
        files = self.describe().files 
        headers = ["filename", "md5"]
        table = [ [f['filename'], f['md5']] for f in files ]
        table = tabulate(table, headers=headers, tablefmt="psql")
        print(f"Dataset name : {self.name}"   )
        print(f"dataset id   : {self.dataset_id}")
        print(table)

    def describe(self) -> Union[schemas.Dataset,None]:
        return self.__api_client.dataset().describe(self.name) if self.dataset_id else None
        
    def list(self) -> List[Dict]:
        return self.describe().files if self.dataset_id else []
        
    def create(self) -> str:
        if not self.dataset_id:
            self.dataset_id = self.__api_client.dataset().create( self.name, self.description)
        return self.dataset_id
        
    def upload(self, files : Union[List[str], str] , as_link : bool=False) -> bool:
        return self.__api_client.dataset().upload( self.name, files, as_link=as_link ) if self.dataset_id else False
                   
    def download( self,  targetfolder : str=None, as_link : bool=False ) -> bool:
        return self.__api_client.dataset().download(self.name, targetfolder, as_link=as_link) if self.dataset_id else False
        
    def exist(self) -> bool:
        return not self.dataset_id==None