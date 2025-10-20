__all__ = [
    "Task",
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