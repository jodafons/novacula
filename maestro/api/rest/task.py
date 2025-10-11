
__all__ = ["TaskAPIClient"]

import pickle
import json

from typing import List, Dict, Union
from maestro import schemas
from maestro.exceptions import *
from maestro.api.models.task import Group

class TaskAPIClient:

    def __init__(self, session):
        self.session = session

    def list(
        self,
        match_with : str="*",
    ):
        payload = {"match_with":match_with}
        payload = {"params_str" : schemas.json_encode(payload)}
        res = self.session.put( f"/remote/task/options/list" , data=payload) 
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Something went bad. detail: {detail}")
        
        return [ schemas.TaskInfo(**task) for task in res[2].json()]


    def describe(
        self,
        task_id : str,
    ):
        payload = {"task_id":task_id}
        payload = {"params_str" : schemas.json_encode(payload)}
        res = self.session.put( f"/remote/task/options/describe" , data=payload) 
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Something went bad. detail: {detail}")
        
        return schemas.TaskInfo(**res[2].json())

    def status( 
        self, 
        task_id : str
    ) -> str:
        payload = {"task_id":task_id}
        payload = {"params_str" : schemas.json_encode(payload)}
        res = self.session.put( f"/remote/task/options/status" , data=payload) 
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Something went bad. detail: {detail}")
        status = res[2].json()
        return status

    
    def create(
        self,
        tasks : Union[List[schemas.TaskInputs], schemas.TaskInputs],
    ) -> str:
         
        if type(tasks) != list:
            tasks = [tasks]
            
        payload = {
           "tasks" : [ task.model_dump() for task in tasks],
        }

        payload = {"params_str" : schemas.json_encode(payload)}
        res = self.session.put(f"/remote/task/options/create", data=payload ) 
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Something went bad. detail: {detail}")
        task_ids = res[2].json()
        return Group(task_ids)
    
    