

__all__ = []

import os
import requests
import threading
import traceback

from loguru          import logger
from time            import sleep, time
from maestro         import schemas, TaskStatus, JobStatus
from maestro.io      import get_io_service
from maestro.db      import models, get_db_service
from maestro         import get_backend_service, get_manager_service
from .task           import TaskScheduler

#
# vanilla scheduler operating as a first in first out (FIFO) queue
#
class SchedulerFIFO(threading.Thread):

    def __init__(
        self,  
    ):
        threading.Thread.__init__(self)
        self.tasks   = {}
        self.__stop  = threading.Event()
        

    def stop(self):
        self.__stop.set()
        while self.is_alive():
            sleep(1)
        for task in self.tasks.values():
            task.stop()



    def run(self):

        self.treat_tasks()

        while (not self.__stop.isSet()):
            sleep(10)
            self.loop()


    def loop(self):

        logger.debug("⌛ schedule loop...")
        self.prepare_tasks()
        self.keep_tasks_alive()
        self.queue_jobs()


    def keep_tasks_alive(self):
        start=time()
        self.tasks = {task_id:task_scheduler for task_id, task_scheduler in self.tasks.items() if task_scheduler.is_alive()}
        logger.debug(f"keep tasks alive in {time()-start} seconds")


    def prepare_tasks(self):
        start=time()
        db_service = get_db_service()
        manager = get_manager_service()
        logger.debug("checking pre-registered tasks...")
        tasks_to_be_registered = []
        
        with db_service() as session:
            tasks_db = session.query( models.Task ).filter(models.Task.status==TaskStatus.PRE_REGISTERED).all()
            tasks_to_be_registered.extend([task_db.task_id for task_db in tasks_db])
            
        for task_id in tasks_to_be_registered:
            user_id = db_service.task(task_id).fetch_owner()
            sc = manager.task(user_id).run_task_group( task_id )
            if sc.isFailure():
                logger.warning(sc.reason())
            
            
        with db_service() as session:
            tasks = session.query(models.Task).filter_by(status=TaskStatus.REGISTERED).all()
            for task in tasks:
                if task.task_id not in self.tasks.keys():
                    scheduler = TaskScheduler( task.task_id )
                    self.tasks[task.task_id] = scheduler
                    scheduler.start()

        logger.debug(f"prepare tasks in {time()-start} seconds")


    def queue_jobs(self):

        start=time()
        db_service    = get_db_service()
        io_service    = get_io_service()
        backend       = get_backend_service()
        procs=10
        try:
  
            with db_service() as session:
                jobs = (session.query(models.Job)\
                               .filter(models.Job.status==JobStatus.ASSIGNED)\
                               #.filter(models.Job.partition==partition)\
                               .filter(models.Job.backend_job_id==-1)\
                               .order_by(models.Job.priority.desc())\
                               .order_by(models.Job.id).limit(procs).all() )
                job_ids = [job_db.job_id for job_db in jobs]
            
            for job_id in job_ids:
                with db_service() as session:
                    job_db = session.query(models.Job).filter(models.Job.job_id==job_id).one()
                    partition = job_db.partition
                    cpus = job_db.reserved_cpu_number
                    memory_mb = job_db.reserved_sys_memory_mb

                    if backend.has_available(partition,cpus,memory_mb ):

                        envs       = {}
                        workarea   = io_service.job(job_id).mkdir()
                        #envs["JOB_ID"]               = job_id
                        #envs["JOB_WORKAREA"]         = workarea
                        envs["CUDA_VISIBLE_ORDER"]   = "PCI_BUS_ID"
                        envs["TF_CPP_MIN_LOG_LEVEL"] = "3"
                        envs["CUDA_VISIBLE_DEVICES"] = "-1" if job_db.device=="cpu" else "0"
                        virtualenv                   = os.environ["VIRTUAL_ENV"]
                        command  = f"cd {workarea}\n"
                        command += f". {virtualenv}/bin/activate\n"
                        command += f"{job_db.command}\n"
                        job_name = f"job-{job_id}"
                        ok, job = backend.run( command    = job_db.command,
                                               cpus       = job_db.reserved_cpu_number,
                                               mem        = job_db.reserved_sys_memory_mb,
                                               partition  = job_db.partition,
                                               jobname    = job_name ,
                                               workarea   = workarea,
                                               envs       = envs,
                                               virtualenv = virtualenv)
                        if ok:
                            job_db.backend_job_id = job['job_id']
                            job_db.backend_state  = backend.status(job_db.backend_job_id)
                            job_db.ping()
                            session.commit()
        except:
            logger.error("🚨 unknown error!")
            traceback.print_exc()

        logger.debug(f"queue jobs in {time()-start}")
        

    def treat_tasks( self ):

        db_service = get_db_service()

        # cancel all jobs in slurm
        backend       = get_backend_service()
        backend.cancel_with("job-", status_str="RUNNING")
        backend.cancel_with("job-", status_str="PENDING")

        with db_service() as session:
            # treat jobs with kill state
            jobs = session.query( models.Job ).filter_by(status=JobStatus.KILL).all()
            for job in jobs:
                if job.status==JobStatus.KILL:
                    job.status=JobStatus.KILLED
                    job.backend_job_id=-1
            session.commit()

        with db_service() as session:
            # treat jobs with kill state
            jobs = session.query( models.Job ).filter_by(status=JobStatus.RUNNING).all()
            for job in jobs:
                job.status=JobStatus.ASSIGNED
                job.backend_job_id=-1
            session.commit()

        with db_service() as session:
            # treat jobs with kill state
            jobs = session.query( models.Job ).filter_by(status=JobStatus.ASSIGNED).all()
            for job in jobs:
                job.backend_job_id=-1
            session.commit()

        with db_service() as session:
            tasks = session.query( models.Task ).filter(models.Task.status!=TaskStatus.COMPLETED).all()
            for task in tasks:
                if task.task_id not in self.tasks.keys():
                    logger.debug(f"recoverning task {task.task_id} with last status {task.status}...")
                    scheduler=TaskScheduler(task.task_id)
                    self.tasks[task.task_id]=scheduler
                    scheduler.start()
     
         

         