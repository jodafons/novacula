__all__ = [
    "Image",
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


class Image:
    def __init__(self,
                 name : str,
                 path : str
                 ):
        self.name = name
        self.path = path
        global __images__
        if name in __images__:
            raise RuntimeError(f"an image with name {name} already exists inside of this group of tasks.")
        __images__[name] = self

    def create(self, basepath : str ):
        dirpath = f"{basepath}/{self.name}"
        os.makedirs( dirpath, exist_ok=True)
        image_name = self.path.split('/')[-1]
        linkpath = f"{dirpath}/{image_name}"
        symlink( self.path, linkpath )   


