__all__ = [
    "Flow",
    "Session",
]

import os, sys, json
import tempfile

from pathlib import Path
from pprint import pprint
from tabulate import tabulate
from typing import List, Union, Dict
from loguru import logger
from novacula import get_context, dump, get_hash
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
        
    def __enter__(self):
        return Session( self.path , virtualenv = self.virtualenv)

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class Session:

    def __init__(self, path: str, virtualenv : str=""):
        self.path = path
        ctx = get_context(clear=True)
        ctx.path = path
        ctx.virtualenv = virtualenv
    
    def mkdir(self):
        os.makedirs(self.path + "/tasks", exist_ok=True)
        os.makedirs(self.path + "/datasets", exist_ok=True)
        os.makedirs(self.path + "/images", exist_ok=True)
        os.makedirs(self.path + "/db", exist_ok=True)
    
    def run(self, dry_run : bool=False):
        ctx = get_context()
        
        if not os.path.exists(f"{self.path}/tasks.json"):
            self.mkdir()
            create_db( f"{self.path}/db/data.db" )
            # Save tasks to disk
            dump( ctx, f"{self.path}/tasks.json" )

            [image.mkdir() for image in ctx.images.values()]
            [dataset.mkdir() for dataset in ctx.datasets.values()]
            [task.mkdir() for task in ctx.tasks.values()]
            # Execute tasks with no dependencies as entry points
            [task(dry_run=dry_run) for task in ctx.tasks.values() if len(task.prev)==0]
            
        else:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_path = temp_file.name
                dump(ctx, temp_file_path)
                
            temp_hash     = get_hash(temp_file_path)
            original_hash = get_hash(f"{self.path}/tasks.json")

            # Compare hashes
            if original_hash != temp_hash:
                raise Exception("Tasks have changed, you can not proceed with execution. Please create a new Flow instance or delete the current flow directory or rename it.")
            else:
                logger.info("No changes detected in tasks.")
                
            self.print_tasks()
            
        
            
    def print(self):
        self.print_images()
        self.print_tasks()
        self.print_datasets()
                
    def print_datasets(self):
        ctx = get_context()
        pprint({ name : dataset.to_raw() for name, dataset in ctx.datasets.items() })
        
    def print_images(self):
        ctx = get_context()
        pprint({ name : image.to_raw() for name, image in ctx.images.items() })
        
    def print_tasks(self):
        ctx = get_context()
        if not os.path.exists(f"{self.path}/db/data.db"):
            raise Exception("Database does not exist. Have you run the flow yet?")
        
        db_service = get_db_service( f"{self.path}/db/data.db" )
        
        rows  = []
        for task in ctx.tasks.values():
            row = [task.name]
            counts = db_service.fetch_table_from_task(task.name)
            row.extend( [value for value in counts.values()])
            #row.extend([task.retry, task.status])
            rows.append(row)
        cols = ['taskname']
        cols.extend([name for name in counts.keys()])
        #cols.extend(["Retry", "Status"])
        table = tabulate(rows ,headers=cols, tablefmt="psql")
        print(table)
