
__all__ = []

import json
from typing import Dict, Any, Union, List
from pydantic import BaseModel

GB=1024
encoder=json.JSONEncoder
decoder=json.JSONDecoder

def json_encode( obj : Any ) -> str:
    return json.dumps( obj, cls=encoder) 

def json_decode( obj : str ) -> Any:
    return json.loads( obj , cls=decoder)

def json_save( obj, f ):
   json.dump(obj, f, cls=encoder)

def json_load( f ):
   return json.load(f,  cls=decoder)

class User(BaseModel):
    name    : str
    user_id : str=""
    
class Credential(BaseModel):
    token   : str
    user_id : str

class TaskInputs(BaseModel):
    name           : str=""
    input          : str=""
    command        : str=""
    image          : str=""
    outputs        : Dict[str,str]={}
    secondary_data : Dict[str,str]={}
    envs           : Dict[str,str]={}
    binds          : Dict[str,str]={}
    device         : str="cpu"
    memory_mb      : int=5*GB
    gpu_memory_mb  : int=0*GB
    cpu_cores      : int=8
    partition      : str=""
    
    def get_output_data(self, key:str) -> str:
        return f"{self.name}.{self.outputs[key]}"
    
class TaskInfo(BaseModel):
    name           : str=""
    task_id        : str=""
    user_id        : str=""
    partition      : str=""
    status         : str=""
    counts         : Dict[str,int]={}
    jobs           : List[str]=[]
    retry          : int=0
    
class Dataset(BaseModel):
    dataset_id       : str=""
    user_id          : str=""
    name             : str=""
    description      : str=""
    files            : List[ Dict[str, Union[str,float] ]]=[]
    data_type        : str=""

