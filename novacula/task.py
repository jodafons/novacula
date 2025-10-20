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

class Script:
    def __init__(self, path : str):
        self.path = path
        self.lines = []
    def __add__(self, line : str):
        self.lines.append(line)
        return self

    def dump(self):
        with open (self.path, 'w') as f:
            f.write( "\n".join(self.lines) + "\n" )
            
       

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
        


class Task:
    def __init__(self,
                 name: str,
                 image : Image,
                 command : str,
                 input_data: Union[str, Dataset],
                 outputs: Dict[str,str],
                 partition : str,
                 secondary_data : Dict[str, Union[str, Dataset]] = {},
                 binds : Dict[str,str] = {},
    ):
        self.name = name
        self.image = image
        self.command = command
        self.input_data = input_data if type(input_data)==str else input_data.name
        self.outputs_data = {  key: {"file":value, "data":f"{self.name}.{value}"} for key, value in outputs.items()}
        self.partition = partition
        self.secondary_data = {key:value if type(value)==str else value.name for key, value in secondary_data.items()}
        self.binds = binds
        global __tasks__
        if name in __tasks__:
            raise RuntimeError(f"a task with name {name} already exists inside of this group of tasks.")
        __tasks__[name] = self

        self.next_tasks = []
        self.before_tasks = []
        self.job_id = None

    def next(self, next_task ):
        if (next_task) and (next_task not in self.next_tasks):
            self.next_tasks.append( next_task )

    def before(self, before_task ):
        if (before_task) and (before_task not in self.before_tasks):
            self.before_tasks.append( before_task )



    def create(self, basepath : str ):
        self.task_path = f"{basepath}/{self.name}"
        os.makedirs(self.task_path+"/works", exist_ok=True)
        os.makedirs(self.task_path+"/jobs", exist_ok=True)
        os.makedirs(self.task_path+"/scripts", exist_ok=True)

    def outputs(self, key : str) -> str:
        return f"{self.name}.{self.outputs_data[key]['file']}"


    def init(self , virtualenv : str=""):

        nfiles = len(self.input_data.files())

        for job_id, filepath in enumerate(self.input_data.files()):
            with open( f"{self.task_path}/jobs/job_{job_id}.json", 'w') as f:
                d = {
                    "input_data": filepath,
                    "outputs" : { key : {"name":value['file'], "target":value['data'].path} for key, value in self.outputs_data.items() },
                    "secondary_data": {},
                    "image" : self.image.path,
                    "job_id": job_id,
                    "command"  : self.command,
                    "binds"    : self.binds,
                    "job_name" : "",
                    "task_name" : self.name,
                }
                pprint(d)
                json.dump(d, f, indent=2)

        script  = Script( f"{self.task_path}/scripts/run.sh" )
        script += f"#!/bin/bash"
        script += f"#SBATCH --job-name={self.name}"
        script += f"#SBATCH --partition={self.partition}"
        script += f"#SBATCH --output={self.task_path}/works/job_%a/output.out"
        script += f"#SBATCH --error={self.task_path}/works/job_%a/output.err"
        script += f"#SBATCH --array=0-{nfiles-1}"
        script += f"source {virtualenv}/bin/activate"
        script += f"njob -i {self.task_path}/jobs/job_$SLURM_ARRAY_TASK_ID.json --message-level INFO -o {self.task_path}/works/job_$SLURM_ARRAY_TASK_ID -j $SLURM_ARRAY_JOB_ID"
        #script.submit(dry_run=dry_run)
        script.dump()

 

    def to_raw(self):
        d = {
            "name": self.name,
            "image" : self.image.path,
            "command"  : self.command,
            "input_data": self.input_data.name,
            "outputs" : { key : {"file":value['file'], "data":value['data'].name} for key, value in self.outputs_data.items() },
            "partition" : self.partition,
            "secondary_data" : { key : value.name for key, value in self.secondary_data.items() },
            "binds"    : self.binds,
            "next_tasks"      : [ t.name for t in self.next_tasks ],
            "before_tasks"    : [ t.name for t in self.before_tasks ],
        }
        return d


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

       
