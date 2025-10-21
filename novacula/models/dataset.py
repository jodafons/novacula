__all__ = [
    "Dataset",
]

import os, sys, json

from typing import List, Dict, Union
from expand_folders import expand_folders
from novacula import symlink
from novacula.models import get_context

class Dataset:
    
    def __init__(self, 
                 name: str,
                 path: str,
                 from_task : Union[None, 'Task']=None,
                ):
            """
            Initializes a new dataset instance.

            Parameters:
            ----------
            name : str
                The name of the dataset. This must be unique within the group of tasks.
            path : str
                The file path where the dataset is located.
            from_task : Union[None, 'Task'], optional
                An optional task from which this dataset is derived. Default is None.

            Note:
            -----
            This constructor is responsible for setting up the dataset with the provided
            parameters and ensuring that the dataset is ready for use in the application.

            Raises:
            ------
            RuntimeError
                If a dataset with the same name already exists in the group of tasks.

            Notes:
            -----
            This constructor also adds the dataset instance to the global 
            __datasets__ dictionary, ensuring that each dataset name is unique.
            """
            self.name = name
            self.path = path
            self.from_task = from_task
            ctx=get_context()
            if name in ctx.datasets:
                raise RuntimeError(f"a dataset with name {name} already exists inside of this group of tasks.")
            ctx.datasets[name] = self

    def mkdir(self, basepath: str):
            """
            Create a directory and symlink files.

            This method creates a directory at the specified basepath with the name
            of the current instance. It then creates symbolic links for each file
            returned by the `files()` method in the newly created directory.

            Parameters:
            basepath (str): The base path where the directory will be created.

            Comments:
            - The directory will be created if it does not already exist.
            - Symbolic links will be created for each file in the directory.
            """
            dirpath = f"{basepath}/{self.name}"
            os.makedirs(dirpath, exist_ok=True)
            for target in self:
                filename = target.split('/')[-1]
                linkpath = f"{dirpath}/{filename}"
                symlink(target, linkpath)

    def __iter__(self) -> List[str]:
        """ 
        Retrieve a sorted list of file paths from the specified directory.
        
        This method expands the folders in the given path and returns 
        a sorted list of all files found within those folders.
        
        Returns:
            List[str]: A sorted list of file paths.
        """
        self.index = 0
        self.files = sorted(expand_folders(self.path))
        return self
    
    def __next__(self):
        if self.index >= len(self.files):
            raise StopIteration
        path = self.files[self.index]
        self.index += 1
        return path
