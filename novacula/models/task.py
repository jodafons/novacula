__all__ = [
    "Task",
    "load",
    "dump"
]

import os, json, subprocess
import sys, shlex

from pprint import pprint
from typing import Union, Dict, List
from novacula.models import get_context, Context
from novacula.models.image import Image 
from novacula.models.dataset import Dataset
from loguru import logger


class Script:
    def __init__(self, 
                 path : str,
                 args : Dict[str, Union[str, int]] = {}
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
            for key, value in args.items():
                self.lines.append( f"#SBATCH --{key}={value}" )

    def __add__(self, line : str):
        self.lines.append(line)
        return self

    def dump(self):
        with open (self.path, 'w') as f:
            f.write( "\n".join(self.lines) + "\n" )
            
    def submit(self):
        """
        Submits a Slurm batch script using 'sbatch' and returns the Job ID.

        Returns:
            str: The extracted Slurm Job ID, or None if submission failed.
        """
        command = f"sbatch {self.path}"

        try:
            # shlex.split is used to correctly handle paths with spaces, etc.
            result = subprocess.run(
                shlex.split(command),
                capture_output=True,
                text=True,
                check=True  # Raise an exception for non-zero return codes
            )
            # Slurm's sbatch output format is typically: "Submitted batch job 12345"
            output = result.stdout.strip()
            # Extract the job ID (the last word in the output)
            if "Submitted batch job" in output:
                job_id = output.split()[-1]
                return job_id
            else:
                logger.error(f"Submission failed or unexpected sbatch output: {output}")
                return None

        except subprocess.CalledProcessError as e:
            logger.error(f"Error submitting job (Exit Code {e.returncode}):")
            print(e.stderr)
            return None
        except FileNotFoundError:
            logger.error("Error: 'sbatch' command not found. Is Slurm installed and in your PATH?")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            return None




class Task:
    
    def __init__(self,
                     name           : str,
                     image          : Union[str,Image],
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
            self.command = command
            ctx = get_context()

            if type(input_data) == str:
                if input_data not in ctx.datasets:
                    RuntimeError(f"input dataset {input_data} not found in the group of tasks.")
                input_data = ctx.datasets[ input_data ]
            
            if type(image) == str:
                if image not in ctx.images:
                    RuntimeError(f"image {image} not found in the group of tasks.")
                image = ctx.images[ image ]
            
            self.image = image

            if self.name in ctx.tasks:
                raise RuntimeError(f"a task with name {name} already exists inside of this group of tasks.")
            
            self.task_id = len( ctx.tasks )
            ctx.tasks[ self.name ] = self   
            self.input_data = input_data            
            self.partition = partition
            self.binds = binds
            self._next = []
            self._prev = []
            
            for key in outputs.keys():
                if type(outputs[key]) == str:
                    name = f"{self.name}.{outputs[key]}"
                    output_data = Dataset( name=name, 
                                           path=f"{ctx.path}/datasets/{name}", from_task=self )
                    outputs[ key ] = output_data
                    
            for key in secondary_data.keys():
                if type(secondary_data[key]) == str:
                    if secondary_data[key] not in ctx.datasets:
                        RuntimeError(f"secondary dataset {secondary[key]} not found in the group of tasks.")
                    else:
                        secondary_data[ key ] = ctx.datasets[ secondary_data[key] ]

                secondary = secondary_data[key]
                if secondary.from_task:
                    secondary.from_task.next+=[self]
                    self._prev+=[ secondary.from_task ]
        
            if self.input_data.from_task:
                self.input_data.from_task.next+=[self]
                self._prev+=[ self.input_data.from_task ]
            
            self.outputs_data = outputs
            self.secondary_data = secondary_data
            self.path = f"{ctx.path}/tasks/{self.name}"
            
    @property
    def next(self) -> List['Task']:
        return self._next
    
    @property
    def prev(self) -> List['Task']:
        return self._prev
    
    @next.setter 
    def next( self, tasks : Union['Task' , List['Task']] ):
        if type(tasks) != list:
            tasks = [ tasks ]
        for task in tasks:
            if task and (task not in self._next):
                self._next.append( task )
         
    @prev.setter
    def prev( self, tasks : Union['Task' , List['Task']] ):
        if type(tasks) != list:
            tasks = [ tasks ]
        for task in tasks:
            if task and (task not in self._prev):
                self._prev.append( task )
                       

    def mkdir(self):
            """
            Create a directory structure for the task.

            This method creates a main directory for the task at the specified
            base path, along with subdirectories for 'works', 'jobs', and 
            'scripts'. If the directories already exist, they will not be 
            created again.

            Args:
                basepath (str): The base path where the task directory will be created.
            """
            
            os.makedirs(self.path + "/works", exist_ok=True)
            os.makedirs(self.path + "/jobs", exist_ok=True)
            os.makedirs(self.path + "/scripts", exist_ok=True)


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
            return self.outputs_data[key].name
    

    def __call__(self, dry_run : bool=False) -> int:
        
        ctx = get_context()
        self.mkdir()

        nfiles = len(self.input_data)

        for job_id, filepath in enumerate(self.input_data):
            with open( f"{self.path}/jobs/job_{job_id}.json", 'w') as f:
                d = {
                    "input_data": filepath,
                    "outputs" : { key : {"name":value.name.replace(f"{self.name}.",""), "target":value.path} for key, value in self.outputs_data.items() },
                    "secondary_data": {},
                    "image"         : self.image.path,
                    "job_id"        : job_id,
                    "command"       : self.command,
                    "binds"         : self.binds,
                    "job_name"      : "",
                    "task_name"     : self.name,
                }
                json.dump(d, f, indent=2)

        script = Script( f"{self.path}/scripts/run_task_{self.task_id}.sh", 
                        args = {
                            "array"     : f"0-{nfiles-1}",
                            "output"    : f"{self.path}/works/job_%a/output.out",
                            "error"     : f"{self.path}/works/job_%a/output.err",
                            "partition" : self.partition,
                            "job-name"  : self.name,
                        }
                        )
        script += f"source {ctx.virtualenv}/bin/activate"
        script += f"njob -i {self.path}/jobs/job_$SLURM_ARRAY_TASK_ID.json --message-level INFO -o {self.path}/works/job_$SLURM_ARRAY_TASK_ID -j $SLURM_ARRAY_JOB_ID"
        script.dump()

        job_id = script.submit()
        
        
        for task in self._next:
            script = Script( f"{self.path}/scripts/run_task_{task.task_id}.sh",
                            args = {
                                "output"    : f"{self.path}/scripts/output_{task.task_id}.out",
                                "error"     : f"{self.path}/scripts/output_{task.task_id}.err",
                                "job-name"  : f"task_{self.task_id}_to_task_{task.task_id}",
                                "dependency": f"afterok:{job_id}",
                            }
            )
            script += f"source {ctx.virtualenv}/bin/activate"
            script += f"ntask -t {ctx.path}/tasks.json -i {task.task_id}"
            script.dump()
            script.submit()
        
        return int(job_id)
 

    def to_raw(self) -> Dict:
            """
            Converts the current task instance into a raw dictionary representation.

            This method gathers various attributes of the task, including its name,
            image path, command, input data, outputs, partition, secondary data,
            binds, and references to next and previous tasks. The resulting dictionary
            can be used for serialization or other purposes where a raw representation
            of the task is needed.

            Returns:
                dict: A dictionary containing the raw representation of the task.
            """
            
            d = {
                "task_id"           : self.task_id,
                "name"              : self.name,
                "image"             : self.image.name,
                "command"           : self.command,
                "input_data"        : self.input_data.name,
                "outputs"           : { key : value.name.replace(self.name+'.',"") for key, value in self.outputs_data.items() },
                "partition"         : self.partition,
                "secondary_data"    : { key : value.name for key, value in self.secondary_data.items() },
                "binds"             : self.binds,
                "next"              : [ task.name for task in self._next ],
                "prev"              : [ task.name for task in self._prev ],
            }
            return d

    @classmethod
    def from_raw( cls, data : Dict) -> 'Task':
        
        task = Task(
            name = data['name'],
            image = data['image'],
            command = data['command'],
            input_data = data['input_data'],
            outputs = { key : value for key, value in data['outputs'].items() },
            partition = data['partition'],
            secondary_data = { key : value for key, value in data['secondary_data'].items() },
            binds = data['binds'],
        )
        
#
# read and write functions
#
        
def dump( ctx : Context, path : str):
    
    with open(path, 'w') as f:
        d = {
            "datasets":{},
            "images":{},
            "tasks":{},
            "path":ctx.path,
            "virtualenv": ctx.virtualenv
        }
        # step 1: dump all datasets which are not from tasks
        for dataset in ctx.datasets.values():
            if not dataset.from_task:
                d['datasets'][ dataset.name ] = dataset.to_raw()
        # step 2: dump all images
        for images in ctx.images.values():
            d['images'][ images.name ] = images.to_raw()
        
        # step 3: dump all tasks
        for task in ctx.tasks.values():
            d[ 'tasks' ][ task.task_id ] = task.to_raw()
        json.dump( d , f , indent=2 )

      
    
def load( path : str, ctx : Context):
    
    with open( path , 'r') as f:
        data = json.load(f)
        ctx.path = data['path']
        ctx.virtualenv = data['virtualenv']
        
        # step 1: load all datasets which are not from tasks
        for dataset in data['datasets'].values():
            Dataset.from_raw( dataset )
        
        # step 2: load all images
        for image in data['images'].values():
            Image.from_raw( image )

        # step 3: load all tasks
        for task_id, raw in data['tasks'].items():
            Task.from_raw( raw )
            