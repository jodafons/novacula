__all__ = [
    "Image",
    "Dataset",
    "Task",
    "LocalProvider",
    "Session",

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


__tasks__ = {}
__datasets__ = {}
__images__ = {}
setup_logs("provider", "INFO", save=False, color="red")




class LocalProvider:

    def __init__(self, 
                 name: str = "local",
                 path : str = f"{os.getcwd()}/tasks",
                 virtualenv : str=os.environ.get("VIRTUAL_ENV", ""),
                ):
        self.name = name
        self.path = path
        self.virtualenv = virtualenv
     

    def __enter__(self):
        
        os.makedirs(self.path + "/tasks", exist_ok=True)
        os.makedirs(self.path + "/datasets", exist_ok=True)
        os.makedirs(self.path + "/images", exist_ok=True)
        os.makedirs(self.path + "/scripts", exist_ok=True)
        os.makedirs(self.path + "/db", exist_ok=True)
        global __tasks__, __datasets__
        __tasks__ = {}
        __datasets__ = {}
        return Session( self.path , virtualenv = self.virtualenv)

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class Session:

    def __init__(self, path: str, virtualenv : str=""):
        self.path = path
        self.virtualenv = virtualenv
    
    def run(self, dry_run : bool=False):

        global __tasks__, __datasets__, __images__ 
        for dataset in __datasets__.values():
            dataset.create( f"{self.path}/datasets" )
        for image in __images__.values():
            image.create( f"{self.path}/images" )

        # create all output datasets and substitute their strings to dataset types
        for task in __tasks__.values():
            task.create( f"{self.path}/tasks" )
            for key, output in task.outputs_data.items():
                filename = output['file']
                output_name = output['data']
                if type(output_name) == str:
                    if output_name not in __datasets__:
                        output_data = Dataset( name=output_name, path=f"{self.path}/datasets/{output_name}", from_task=task )
                        output_data.create( f"{self.path}/datasets" )
                        task.outputs_data[ key ]['data'] = output_data
                    else:
                        task.outputs_data[ key ]['data'] = __datasets__[ output_name ]

        # substitute all input/secondary datasets to dataset types
        for task in __tasks__.values():
            if task.input_data == "":
                task.input_data=None
            else:
                if type(task.input_data) == str:
                    if task.input_data not in __datasets__:
                        input_data = Dataset( name=task.input_data, path=f"{self.path}/datasets/{task.input_data}" )
                        input_data.create( f"{self.path}/datasets" )
                        task.input_data = input_data
                    else:
                        task.input_data = __datasets__[ task.input_data ]
            for key, secondary in task.secondary_data.items():
                if type(secondary) == str:
                    if secondary not in __datasets__:
                        secondary_data = Dataset( name=secondary, path=f"{self.path}/datasets/{secondary}" )
                        secondary_data.create( f"{self.path}/datasets" )
                        task.secondary_data[ key ] = secondary_data
                    else:
                        task.secondary_data[ key ] = __datasets__[ secondary ]
        
        # link all tasks using datasets as link
        for task in __tasks__.values():
            [ task.before(data.from_task) for data in task.secondary_data.values()]
            if task.input_data:
                task.before( task.input_data.from_task )
                if task.input_data.from_task:
                    task.input_data.from_task.next(task)
            for secondary in task.secondary_data.values():
                task.before(secondary.from_task )
                if secondary.from_task:
                    secondary.from_task.next(task)
                
    
        with open(f"{self.path}/tasks.json", 'w') as f:
            d = { name: task.to_raw() for name, task in __tasks__.items() }
            json.dump( d , f , indent=2 )

       for task in __tasks__.values():

            # create all scripts and job files
            task.init( virtualenv=self.virtualenv )

            # check if this task is a ready to run task (rank zero)
            if len(task.before_tasks) == 0:
                logger.info( f"submitting task {task.name} to the scheduler." )
                if not dry_run:
                    job_id = subprocess.run( ["sbatch", f"{task.task_path}/scripts/run.sh"] )
                    if len(task.next_tasks) > 0: