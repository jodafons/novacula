__all__ = [
    "Image",
]

import os, sys

from typing import List, Union, Dict
from expand_folders import expand_folders
from novacula import symlink
from novacula.models import get_context



class Image:
    
    def __init__(self,
                     name : str,
                     path : str
                     ):
            """
            Initializes an instance of the Image class.

            Parameters:
            name (str): The name of the image. Must be unique within the group of tasks.
            path (str): The file path to the image.

            Raises:
            RuntimeError: If an image with the same name already exists in the group of tasks.
            """
            
            self.name = name
            self.path = path
            ctx = get_context()
            if name in ctx.images:
                raise RuntimeError(f"an image with name {name} already exists inside of this group of tasks.")
            ctx.images[name] = self

    def mkdir(self, basepath: str):
        """
        Create a directory for the image and create a symbolic link to the image file.

        This method constructs a directory path using the provided basepath and the 
        name of the image instance. It then creates the directory if it does not 
        already exist and creates a symbolic link to the image file within that 
        directory.

        Parameters:
        basepath (str): The base path where the directory will be created.

        Returns:
        None
        """
        
        dirpath = f"{basepath}/{self.name}"
        os.makedirs(dirpath, exist_ok=True)
        image_name = self.path.split('/')[-1]
        linkpath = f"{dirpath}/{image_name}"
        symlink(self.path, linkpath)
    


