__all__ = ["get_backend_service"]


import subprocess
import threading
import tempfile
import pyslurm
import os,time
from typing import Dict, List
from loguru import logger

__slurm_service = None


class JobLogger(threading.Thread):
    def __init__(self, job_id : int, workarea : str, job_name : str):
        threading.Thread.__init__(self)
        self.job_id=job_id
        self.workarea=workarea
        self.current_line=0
        self.job_name=job_name
 
    def status(self) -> str:
        job = pyslurm.slurmdb_jobs().get(jobids=[self.job_id])
        return job[self.job_id]["state_str"].lower() if job else None
        
    def run(self):
        while self.status() not in ["failed", "completed", "stopped", "suspended","cancelled"]:
            time.sleep(5)
            self.loop() 
    
    def loop(self):
        if os.path.exists(f"{self.workarea}/output.log"):
            with open( f"{self.workarea}/output.log", "r") as f:
                lines = f.readlines()
                for line in lines[self.current_line::]:
                    if self.job_name in line:
                        print(line.replace("\n",""))
                n=len(lines)-1
                self.current_line= n if n >=0 else 0
                


class SlurmService():
    
    def __init__( self, account : str , reservation : str=None):
        self.account=account
        self.reservation=reservation
        self.jobs={}
        
        
    def has_available( self, partition : str, cpu_cores : int, memory_mb : int ) -> bool:
        nodes = pyslurm.node().get()
        for name, node in nodes.items():
            if partition in node["partitions"]:
                avail_cpus = node["cpus"] - node["alloc_cpus"]
                if avail_cpus < cpu_cores:
                    continue
                avail_memory_mb = node["real_memory"] - node["alloc_mem"]
                if avail_memory_mb < memory_mb:
                    continue
                return True
        return False


    def run( self, 
             command : str, 
             cpus : int, 
             mem : float, 
             partition : str, 
             jobname : str , 
             workarea : str, 
             envs : Dict[str,str]={}, 
             virtualenv : str=None,
             #pre_exec   : str=os.environ["SLURM_PRE_EXEC_COMMAND_TO_APPLY_FOR_ALL_JOBS"],
        ):
        


        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write(f"#!/bin/bash\n")
            #f.write(f"#SBATCH --nodes=1\n")
            f.write(f"#SBATCH --ntasks=1\n")
            #f.write(f"#SBATCH --ntasks-per-node=1\n")
            #f.write(f"#SBATCH --exclusive\n")
            f.write(f"#SBATCH --cpus-per-task={cpus}\n")
            f.write(f"#SBATCH --account={self.account}\n")
            f.write(f"#SBATCH --partition={partition}\n")
            if self.reservation:
                f.write(f"#SBATCH --reservation={self.reservation}\n")
            f.write(f"#SBATCH --job-name={jobname}\n")
            f.write(f"#SBATCH --output={workarea}/output.out\n")
            f.write(f"#SBATCH --error={workarea}/output.err\n")
            f.write(f"#SBATCH --mem-per-cpu={int(mem/cpus)}\n")

            for key, value in envs.items():
                f.write(f"export {key}='{value}'\n")
            if virtualenv:
                f.write(f"source {virtualenv}/bin/activate\n")
            
            f.write(f"export LOGURO_LEVEL='INFO'\n")
            f.write(f"echo Node: $SLURM_JOB_NODELIST\n")
            f.write(f"export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK\n")
            f.write(f"echo OMP_NUM_TREADS: $SLURM_CPUS_PER_TASK\n")
            #f.write(f"{pre_exec}\n")    
            f.write(f"{command} > {workarea}/output.log\n")
            f.write(f"wait\n")
            f.flush()
            # Submit job
            logger.info(f"subimittinh from {f.name}")
            result = subprocess.run(["sbatch", f.name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            tempfilepath = f.name
        
        with open(tempfilepath,'r') as f:
            print(f.read())
            
        if result.returncode==0:
            job_id = int(result.stdout.decode().strip().split()[-1])
            self.jobs[job_id]=JobLogger( job_id, workarea, jobname )
            self.jobs[job_id].start()
        else:
            logger.error(result.stderr.decode())
            return False, result.stderr.decode()   
        result = pyslurm.job().get()[job_id]
        answer = {'job_id':result['job_id'], 'status':result['job_state'].lower()}
        logger.info(f"submited with id {answer['job_id']} and current status {answer['status']}")
        return True, answer

    def partitions(self) -> List[str]:
        return pyslurm.partition().ids()

    def status(self, job_id : int) -> str:
        job = pyslurm.slurmdb_jobs().get(jobids=[job_id])
        return job[job_id]["state_str"].lower() if job else None
        
    def cancel(self, job_id : int) -> bool:
        if self.check_job_existence(job_id):
            pyslurm.slurm_kill_job(job_id, 9, 0)
            return True
        else:
            return False

    def check_job_existence( self , job_id ) -> bool :
        return True if pyslurm.slurmdb_jobs().get(jobids=[job_id]) else False

    def describe(self, job_id : int) -> Dict:
        return pyslurm.slurmdb_jobs().get(jobids=[job_id])[job_id] if self.check_job_existence(job_id) else {}
            

    def cancel_with( self, name : str, status_str : str ):
        jobs = pyslurm.slurmdb_jobs().get()
        for job_id, job in jobs.items():
            if (name in job['jobname']) and (status_str==job['state_str']):
                self.cancel(job_id)            
            
#
# get database service
#
def get_slurm_service(account : str=None):
    global __slurm_service
    if not __slurm_service:
       __slurm_service = SlurmService(account=account)
    return __slurm_service


get_backend_service = get_slurm_service