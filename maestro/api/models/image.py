__all__ = ["Image"]


from tabulate import tabulate
from typing import List, Union, Dict
from maestro.api.client import get_session_api
from maestro import schemas


class Image:
    '''
  
    '''  
    def __init__(
        self,
        name        : str,
        filepath    : str,
        description : str="",
        as_link     : bool=False,
    ) -> None:
        
        self.__api_client = get_session_api()
        self.name         = name
        self.description  = description
        self.filepath     = filepath
        if self.__api_client.image().check_existence(name):
            self.image_id = self.__api_client.image().identity(name)
        else:
            self.image_id = self.__api_client.image().create_and_upload( self.name, self.filepath, self.description, 
                                                                         as_link=as_link)
    
   