__all__ = [
    "Flow",
    "Session",
]

import os, sys, json

from pathlib import Path
from pprint import pprint
from typing import List, Union, Dict
from novacula import get_context, dump
from novacula.models import Dataset, Image, Task
from novacula.db import get_db_service, create_db


class Flow:

    def __init__(self, 
                 name       : str = "local",
                 path       : str = f"{os.getcwd()}/tasks",
                 virtualenv : str=os.environ.get("VIRTUAL_ENV", ""),
        ):
            """
            Initializes a new instance of the class.

            Parameters:
            ----------
            name : str, optional
                The name of the provider. Defaults to "local".
            path : str, optional
                The file path where tasks are located. Defaults to the current working directory followed by '/tasks'.
            virtualenv : str, optional
                The path to the virtual environment. Defaults to the value of the environment variable 'VIRTUAL_ENV'.

            Attributes:
            ----------
            name : str
                The name of the provider.
            path : str
                The file path where tasks are located.
            virtualenv : str
                The path to the virtual environment.
            """
            
            self.name = name
            self.path = path
            self.virtualenv = virtualenv
        
    def mkdir(self):
        os.makedirs(self.path + "/tasks", exist_ok=True)
        os.makedirs(self.path + "/datasets", exist_ok=True)
        os.makedirs(self.path + "/images", exist_ok=True)
        os.makedirs(self.path + "/db", exist_ok=True)
        create_db( f"{self.path}/db/data.db" )

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
        
        # Save tasks to disk
        dump( ctx, f"{self.path}/tasks.json" )
        
        # Execute tasks with no dependencies as entry points
        [task(dry_run=dry_run) for task in ctx.tasks.values() if len(task.prev)==0]