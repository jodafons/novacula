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
                 path: str
                ):
        self.name = name
        self.path = path
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
        self.next = []
        global __tasks__
        if name in __tasks__:
            raise RuntimeError(f"a task with name {name} already exists inside of this group of tasks.")
        __tasks__[name] = self

        self.next = []
        self.before = []
        self.job_id = None


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

    def next(self, job_id , script_path : str):
        script  = Script( f"{self.task_path}/scripts/next.sh" )
        script += f"#!/bin/bash"
        script += f"#SBATCH --job-name={self.name}.next"
        script += f"sbatch {script_path}"
        script.dump()




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

        for task in __tasks__.values():
            task.create( f"{self.path}/tasks" )
            if type(task.input_data) == str and task.input_data != "":
                if task.input_data not in __datasets__:
                    input_data = Dataset( name=task.input_data, path=f"{self.path}/datasets/{task.input_data}" )
                    input_data.create( f"{self.path}/datasets" )
                    task.input_data = input_data
                else:
                    task.input_data = __datasets__[ task.input_data ]
            
            for key, output in task.outputs_data.items():
                filename = output['file']
                output_name = output['data']
                if type(output_name) == str:
                    if output_name not in __datasets__:
                        output_data = Dataset( name=output_name, path=f"{self.path}/datasets/{output_name}" )
                        output_data.create( f"{self.path}/datasets" )
                        task.outputs_data[ key ]['data'] = output_data
                    else:
                        task.outputs_data[ key ]['data'] = __datasets__[ output_name ]

            for key, secondary in task.secondary_data.items():
                if type(secondary) == str:
                    if secondary not in __datasets__:
                        secondary_data = Dataset( name=secondary, path=f"{self.path}/datasets/{secondary}" )
                        secondary_data.create( f"{self.path}/datasets" )
                        task.secondary_data[ key ] = secondary_data
                    else:
                        task.secondary_data[ key ] = __datasets__[ secondary ]


        
        datasets = {'inputs'   : {},'outputs'  : {},}
        
        # NOTE: stage 1, collect all datasets (inputs/outputs)
        for task in __tasks__.values():
            if task.input_data:
                if not task.input_data.name in datasets["inputs"]:
                    datasets['inputs'][task.input_data.name]=[task.name]
                else:
                    datasets["inputs"][task.input_data.name].append(task.name)    
            # datasets outputs
            for key, value in task.outputs_data.items():
                name = value['data'].name
                datasets["outputs"][name]=task.name
            # datasets secondary data
            for key, value in task.secondary_data.items():
                name = value.name
                if not name in datasets["inputs"]:
                    datasets["inputs"][name] = [task.name]
                else:
                    datasets["inputs"][name].append(task.name)

        # NOTE: stage 2: organize all names
        task_names_dependency = {}
        for key in datasets['inputs'].keys():
            if key in datasets['outputs']:
                task_names_dependency[datasets['outputs'][key]] = datasets['inputs'][key]
            else:
                task_names_dependency[key]=datasets['inputs'][key]

        # NOTE: stage 3, create task links
        task_names = { name:task for name, task in __tasks__.items() }
        for task_name, next_tasks in task_names_dependency.items():
            if task_name in task_names:
                task = task_names[ task_name ]
                for next_task_name in next_tasks:
                    if next_task_name in task_names:
                        next_task = task_names[ next_task_name ]
                        task.next.append( next_task )
                        next_task.before.append( task )

        for task in __tasks__.values():
            print([t.name for t in task.before], " --> ", task.name, " --> ", [t.name for t in task.next] )
            task.init( virtualenv=self.virtualenv )
        
        jobs = {}
        for task in __tasks__.values():

            

            if len(task.before) > 0:
                if not any([ before_task.job_id is None for before_task in task.before ]):
                    continue



            else:
                task.submit()





            for before_task in task.before:                
                if not before_task.job_id:
                    break

                
