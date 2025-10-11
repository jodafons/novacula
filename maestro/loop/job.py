#!/usr/bin/env python

import glob
import wandb
import argparse
import traceback
import os, sys

from pprint  import pprint
from time    import sleep
from loguru  import logger
from maestro import Popen, JobStatus, setup_logs, get_io_service, get_db_service
from maestro import MemoryMonitor
from maestro import symlink



         
def run( args ):

    db_service = get_db_service(args.db_string)
    io_service = get_io_service(args.volume)
    job_service= db_service.job(args.job_id)
    job_service.start()
    setup_logs(args.job_name, args.message_level, save=False, color="red")

    if args.wandb_token!="":
        os.environ["WANDB_USER"]=args.wandb_user
        os.environ["WANDB_API_KEY"]=args.wandb_token
        try:
            wandb.login(relogin=True, key=args.wandb_token)
            monitor = wandb.init(project=args.task_name,
                       name=args.job_name,
                       id=args.job_name,
                       entity=args.wandb_user)
        except:
            traceback.print_exc()
            monitor=None

    logger.info("starting...")
    job_service.update_status(JobStatus.RUNNING)

    workarea   = io_service.job(args.job_id).mkdir()
    task       = db_service.task( args.task_id ).fetch_task_inputs()
    command    = task.command
    
    print(command)
    
    os.environ["JOB_WORKAREA"]   = workarea
    os.environ["JOB_ID"]         = args.job_id
    device = os.environ.get("CUDA_VISIBLE_DEVICES","-1")
    logger.info(f"device number is {device}")
     
    logger.info(f"starting env builder for job {args.job_id}...")

  
    logger.info(f"workarea {workarea}...")


    imagepath = io_service.image(args.image_id).path()
    imagename = imagepath.split("/")[-1]
    logger.info(f"using singularity image with name {imagename}.")
    linkpath  = symlink(imagepath, f"{workarea}/{imagename}")
    image     = linkpath

    for key, name in task.secondary_data.items():
        logger.info(f"creating secondary data link for {name} inside of the job workarea.")
        dataset_id = db_service.fetch_dataset_from_name( name )
        basepath = io_service.dataset(dataset_id).basepath
        linkpath = symlink( f"{basepath}" , f"{workarea}/{name}")
        command = command.replace(f"%{key}", linkpath)    

    if task.input!="":
        dataset_id, file_id = args.input.split(":") 
        filename = io_service.dataset(dataset_id).files(with_file_id=True) [file_id]
        logger.info(f"creating input data link for {task.input} inside of the job workarea.")
        name = db_service.dataset(dataset_id).fetch_name()
        basepath = io_service.dataset(dataset_id).basepath
        linkpath = symlink( f"{basepath}/{filename}", f"{workarea}/{name}.{filename}")
        command = command.replace(f"%IN", linkpath)
      
    outputs = []
      
    for key, filename in task.outputs.items():
        name = f"{task.name}.{filename}"
        logger.info(f"creating output data link for {name} inside of the job workarea.")
        dataset_id = db_service.fetch_dataset_from_name(name)
        filepath = f"{workarea}/{args.job_id}.{filename}"
        command = command.replace(f"%{key}", filepath)
        io_service.dataset(dataset_id).mkdir()
        outputs.append( (dataset_id, filepath) )
            
    print(command)
        
    entrypoint = f"{workarea}/entrypoint.sh"
    with open(entrypoint,'w') as f:
        f.write(f"cd {workarea}\n")
        f.write(command)
            
    stop=False
    try:
        binds   = f'--bind {args.volume}:{args.volume}'
        for key,value in task.binds.items():
            binds+= f' --bind {key}:{value}'
        command = f"singularity exec --nv --writable-tmpfs {binds} {image} bash {entrypoint}"
        command = command.replace('  ',' ') 

        envs = {}
        envs["JOB_ID"]               = args.job_id
        envs["JOB_WORKAREA"]         = workarea 
        envs["TF_CPP_MIN_LOG_LEVEL"] = "3"
        envs["CUDA_VISIBLE_ORDER"]   = "PCI_BUS_ID"
        envs["CUDA_VISIBLE_DEVICES"] = os.environ.get("CUDA_VISIBLE_DEVICES","-1")
        envs["OMP_NUM_THREADS"]      = os.environ.get("SLURM_CPUS_PER_TASK", '4')
        envs["SLURM_CPUS_PER_TASK"]  = envs["OMP_NUM_THREADS"]
        envs["SLURM_MEM_PER_NODE"]   = os.environ.get("SLURM_MEM_PER_NODE", '2048')
        envs.update(task.envs)
        
        pprint(envs)
        
        logger.info("🚀 run job!")   
        print(command)
        proc = Popen(command, envs = envs)
        proc.run_async()
        ram = MemoryMonitor()

        while proc.is_alive():
            sleep(5)
            job_service.ping()
            db_status = job_service.fetch_status()
            ok = ram(proc , job_id=args.job_id, log=monitor) 
            if db_status == JobStatus.KILL or not ok:
                proc.kill()
                proc.join()
                if not ok: # stop because some memory condition
                    job_service.update_status(JobStatus.FAILED)
                else: # stop because the user tell it
                    job_service.update_status(JobStatus.KILLED)
                stop=True
    except:
        traceback.print_exc()
        job_service.update_status(JobStatus.FAILED)
        stop=True

    if stop:
        monitor.finish()
        sys.exit(0)


    if proc.status()!="completed":
        logger.error(f"something happing during the job execution. exiting with status {proc.status()}")
        job_service.update_status(JobStatus.FAILED)
        monitor.finish()
        sys.exit(0)
    
    
    logger.info("uploading output files into the storage...")
    for dataset_id, filename in outputs:
        name = db_service.dataset(dataset_id).fetch_name()
        files= glob.glob(filename)
        for filepath in files:
            logger.info(f"saving {filepath} into dataset with name {name}...")
            ok = io_service.dataset( dataset_id ).save( filepath )
            if not ok:
                logger.error(f"something happing during save from {filepath} to dataset with name {name}")
                job_service.stop()
                job_service.update_status(JobStatus.FAILED)
                monitor.finish()
                sys.exit(0)
    

    job_service.update_status(JobStatus.COMPLETED)
    monitor.finish()
    sys.exit(0)







#
# args 
#
def args_parser():
    
    common_parser = argparse.ArgumentParser(description = '', add_help = False)

    common_parser.add_argument('--job-name', action='store', dest='job_name', required = True,
                        help = "the job name.")
    common_parser.add_argument('--task-name', action='store', dest='task_name', required = True,
                        help = "the task name.")
    
    common_parser.add_argument('-l','--message-level', action='store', dest='message_level', required = False, 
                        default="INFO",
                        help = "the message level.")
  
    common_parser.add_argument('-i','--image-id', action='store', dest='image_id', required = True, 
                        help = "the image.")
    
    common_parser.add_argument('-j','--job-id', action='store', dest='job_id', required = True, 
                        help = "the job id.")
    
    common_parser.add_argument('-t','--task-id', action='store', dest='task_id', required = True, 
                        help = "the task id.")
  
    common_parser.add_argument('--input', action='store', dest='input', required = True, 
                        help = "the input id at form dataset_id:file_id.")
  
    common_parser.add_argument('-v','--volume', action='store', dest='volume', required = True, 
                        help = "the volume")
  
    database_parser = argparse.ArgumentParser(description = '', add_help = False)

    database_parser.add_argument('--database-string', action='store', dest='db_string', type=str, required=True,
                                 help = "the database url used to store all tasks and jobs. default can be passed by DB_STRING environ.")

    wandb_parser = argparse.ArgumentParser(description = '', add_help = False)
    
    wandb_parser.add_argument('--wandb-user', action='store', dest='wandb_user', type=str,
                                required=False, default="",
                                help = "the wandb user account.")
  
    wandb_parser.add_argument('--wandb-token', action='store', dest='wandb_token', type=str,
                                required=False, default="",
                                help = "the wandb user token.")


    return [common_parser, database_parser, wandb_parser]
