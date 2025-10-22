__all__ = [
    "LocalProvider",
    "Session",
]

import os, sys, json

from pathlib import Path
from pprint import pprint
from typing import List, Union, Dict
from novacula import get_context
from novacula.models import Dataset, Image, Task



class LocalProvider:

    def __init__(self, 
                 name: str = "local",
                 path : str = f"{os.getcwd()}/tasks",
                 virtualenv : str=os.environ.get("VIRTUAL_ENV", ""),
                ):
        self.name = name
        self.path = path
        self.virtualenv = virtualenv
        
    def mkdir(self):
        os.makedirs(self.path + "/tasks", exist_ok=True)
        os.makedirs(self.path + "/datasets", exist_ok=True)
        os.makedirs(self.path + "/images", exist_ok=True)
        os.makedirs(self.path + "/scripts", exist_ok=True)
        os.makedirs(self.path + "/db", exist_ok=True)
     

    def __enter__(self):
        
        self.mkdir()
        ctx = get_context()
        ctx.clear()
        return Session( self.path , virtualenv = self.virtualenv)

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class Session:

    def __init__(self, path: str, virtualenv : str=""):
        self.path = path
        self.virtualenv = virtualenv
    
    def run(self, dry_run : bool=False):

        ctx = get_context()

        for dataset in ctx.datasets.values():
            dataset.mkdir( f"{self.path}/datasets" )
        for image in ctx.images.values():
            image.mkdir( f"{self.path}/images" )

        # create all output datasets and substitute their strings to dataset types
        for task in ctx.tasks.values():
            task.mkdir( f"{self.path}/tasks" )
            for key, output in task.outputs_data.items():
                output_name = output['data']
                if type(output_name) == str:
                    if output_name not in ctx.datasets:
                        output_data = Dataset( name=output_name, path=f"{self.path}/datasets/{output_name}", from_task=task )
                        output_data.mkdir( f"{self.path}/datasets" )
                        task.outputs_data[ key ]['data'] = output_data
                    else:
                        task.outputs_data[ key ]['data'] = ctx.datasets[ output_name ]

        # substitute all input/secondary datasets to dataset types
        for task in ctx.tasks.values():
           
            if type(task.input_data) == str:
                if task.input_data not in ctx.datasets:
                    input_data = Dataset( name=task.input_data, path=f"{self.path}/datasets/{task.input_data}" )
                    input_data.mkdir( f"{self.path}/datasets" )
                    task.input_data = input_data
                else:
                    task.input_data = ctx.datasets[ task.input_data ]
            for key, secondary in task.secondary_data.items():
                if type(secondary) == str:
                    if secondary not in ctx.datasets:
                        secondary_data = Dataset( name=secondary, path=f"{self.path}/datasets/{secondary}" )
                        secondary_data.create( f"{self.path}/datasets" )
                        task.secondary_data[ key ] = secondary_data
                    else:
                        task.secondary_data[ key ] = ctx.datasets[ secondary ]
        
        # link all tasks using datasets as link
        for task in ctx.tasks.values():
            task.prev = [data.from_task for data in task.secondary_data.values()]
            task.prev = task.input_data.from_task
            if task.input_data.from_task:
                task.input_data.from_task.next+=[task]
            for secondary in task.secondary_data.values():
                task.prev=secondary.from_task
                if secondary.from_task:
                    secondary.from_task.next=task
                
    
        with open(f"{self.path}/tasks.json", 'w') as f:
            d = { name: task.to_raw() for name, task in ctx.tasks.items() }
            json.dump( d , f , indent=2 )

      