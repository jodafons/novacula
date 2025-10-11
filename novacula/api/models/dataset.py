__all__ = ["Dataset"]


from tabulate import tabulate
from typing import List, Union, Dict
from novacula.api.client import get_session_api





class Dataset:
    '''
  
    '''

    def __init__(
        self,
        name        : str,
        description : str="",
        permitions  : List[str]=[],
    ) -> None:
        self.__api_client = get_session_api()
        self.name         = name
        self.description  = description



    def create(self) -> str:
        return self.__api_client.dataset(dataset_id).create( self.name, self.description) if not self.dataset_id else self.dataset_id
        
    def upload(self, files : Union[List[str], str] , as_link : bool=False) -> bool:
        return self.__api_client.dataset().upload( self.name, files  ,as_link=as_link)
                   
    def download( self,  targetfolder : str=None, as_link : bool=False ) -> bool:
        return self.__api_client.dataset().download(self.name, targetfolder, as_link=as_link) 
        
