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
 

class SBatch:
    def __init__(self, 
                     path       : str,
                     partition  : str=None,
                     output     : str=None,
                     error      : str=None,
                     n_tasks    : int=None,
                     name       : str=None
                    ):
            """
            Initializes a new instance of the class.

            Parameters:
            path (str): The path to the script or job file.
            partition (str, optional): The partition to submit the job to. Defaults to None.
            output (str, optional): The file to write standard output to. Defaults to None.
            error (str, optional): The file to write standard error to. Defaults to None.
            n_tasks (int, optional): The number of tasks for the job array. Defaults to None.
            name (str, optional): The name of the job. Defaults to None.

            This constructor sets up the necessary SLURM directives for job submission.
            """
            self.path = path
            self.lines = [f"#!/bin/bash"]
            if partition:
                self.lines.append( f"#SBATCH --partition={partition}" )
            if output:
                self.lines.append( f"#SBATCH --output={output}" )
            if error:
                self.lines.append( f"#SBATCH --error={error}" )
            if n_tasks is not None:
                self.lines.append( f"#SBATCH --array=0-{n_tasks-1}" )
            if name:
                self.lines.append( f"#SBATCH --job-name={name}" )
        
    def __add__(self, line : str):
        self.lines.append(line)
        return self

    def dump(self):
        with open (self.path, 'w') as f:
            f.write( "\n".join(self.lines) + "\n" )
            
       

class Task:
    
    def __init__(self,
                     name           : str,
                     image          : Image,
                     command        : str,
                     input_data     : Union[str, Dataset],
                     outputs        : Dict[str,str],
                     partition      : str,
                     secondary_data : Dict[str, Union[str, Dataset]] = {},
                     binds          : Dict[str,str] = {},
        ):
            """
            Initializes a new task with the given parameters.

            Parameters:
            - name (str): The name of the task.
            - image (Image): The image associated with the task.
            - command (str): The command to execute for the task.
            - input_data (Union[str, Dataset]): The input data for the task, can be a string or a Dataset object.
            - outputs (Dict[str, str]): A dictionary mapping output names to their corresponding file names.
            - partition (str): The partition to which the task belongs.
            - secondary_data (Dict[str, Union[str, Dataset]], optional): Additional data associated with the task, defaults to an empty dictionary.
            - binds (Dict[str, str], optional): A dictionary for binding parameters, defaults to an empty dictionary.

            Raises:
            - RuntimeError: If a task with the same name already exists in the group of tasks.
            """
            
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


    def mkdir(self, basepath: str):
            """
            Create a directory structure for the task.

            This method creates a main directory for the task at the specified
            base path, along with subdirectories for 'works', 'jobs', and 
            'scripts'. If the directories already exist, they will not be 
            created again.

            Args:
                basepath (str): The base path where the task directory will be created.
            """
            
            self.task_path = f"{basepath}/{self.name}"
            os.makedirs(self.task_path + "/works", exist_ok=True)
            os.makedirs(self.task_path + "/jobs", exist_ok=True)
            os.makedirs(self.task_path + "/scripts", exist_ok=True)


    def output(self, key: str) -> str:
            """
            Generate the output file path based on the provided key.

            This method constructs a string that represents the output file path
            by combining the name of the current instance with the file name
            associated with the given key in the outputs_data dictionary.

            Args:
                key (str): The key used to retrieve the file name from outputs_data.

            Returns:
                str: The constructed output file path in the format 'name.file'.
            """
            return f"{self.name}.{self.outputs_data[key]['file']}"
    

    def __call__(self , virtualenv : str=""):

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

        script = SBatch( f"{self.task_path}/scripts/run.sh"
                        output = f"{self.task_path}/works/job_%a/output.out",
                        error  = f"{self.task_path}/works/job_%a/output.err",
                        n_tasks = nfiles,
                        name   = self.name,
                        partition = self.partition
                        )
        
        script += f"source {virtualenv}/bin/activate"
        script += f"njob -i {self.task_path}/jobs/job_$SLURM_ARRAY_TASK_ID.json --message-level INFO -o {self.task_path}/works/job_$SLURM_ARRAY_TASK_ID -j $SLURM_ARRAY_JOB_ID"
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