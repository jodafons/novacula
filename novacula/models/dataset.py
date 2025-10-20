__all__ = [
    "Dataset",
]

import os, sys, json
import subprocess
from pathlib import Path
from pprint import pprint
from typing import List, Union, Dict
from expand_folders import expand_folders
from novacula.db import get_db_service, recreate_db, models
from novacula import setup_logs, symlink
from loguru import logger
import subprocess

class Dataset:
    def __init__(self, 
                 name: str,
                 path: str,
                 from_task : Union[None, 'Task']=None,
                ):
        self.name = name
        self.path = path
        self.from_task = from_task
        global __datasets__
        if name in __datasets__:
            raise RuntimeError(f"a dataset with name {name} already exists inside of this group of tasks.")
        __datasets__[name] = self

    def create(self, basepath : str ):
        dirpath = f"{basepath}/{self.name}"
        os.makedirs( dirpath, exist_ok=True)
        for target in self.files():
            filename = target.split('/')[-1]
            linkpath = f"{dirpath}/{filename}"
            symlink( target, linkpath )   

    def files(self) -> List[str]:
        return sorted(expand_folders(self.path))
        
