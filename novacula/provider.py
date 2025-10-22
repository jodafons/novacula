__all__ = [
    "LocalProvider",
    "Session",
]

import os, sys, json

from pathlib import Path
from pprint import pprint
from typing import List, Union, Dict
from novacula import get_context, dump
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
        os.makedirs(self.path + "/db", exist_ok=True)
     

    def __enter__(self):
        
        self.mkdir()
        return Session( self.path , virtualenv = self.virtualenv)

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class Session:

    def __init__(self, path: str, virtualenv : str=""):
        self.path = path
        ctx = get_context(clear=True)
        ctx.path = path
        ctx.virtualenv = virtualenv
       
    
    def run(self, dry_run : bool=False):
        ctx = get_context()
        dump( ctx, f"{self.path}/tasks.json" )
        for task in ctx.tasks.values():
            if len(task.prev)==0:
                task( dry_run=dry_run )