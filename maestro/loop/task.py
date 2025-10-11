__all__ = ["TaskSchedule"]

import traceback
import threading

from sqlalchemy import or_, and_
from loguru import logger
from time import sleep
from maestro.db import models, get_db_service
from maestro import JobStatus, TaskState, TaskStatus, task_final_status
from maestro.exceptions import *


def test_job_fail( app, task: models.Task ) -> bool:
  """
    Check if the first job returns fail
  """
  job = task.jobs[0]
  return (job.status == JobStatus.FAILED) or (job.status == JobStatus.BROKEN)
    
 
def test_job_assigned( app, task: models.Task ) -> bool:
  """
    Assigned the fist job to test
  """
  logger.debug("test_job_assigned")
  task.jobs[0].status =  JobStatus.ASSIGNED
  task.jobs[0].backend_job_id = -1
  task.jobs[0].priority = 10 * task.jobs[0].priority # multiply by 10 to force this job to the top of the queue
  return True


def test_job_running( app, task: models.Task ) -> bool:
  """
    Check if the test job still running
  """
  logger.debug(f"exp test with status {task.jobs[0].status}...")
  return task.jobs[0].status == JobStatus.RUNNING


def test_job_completed( app, task: models.Task ) -> bool:
  """
    Check if the test job is completed
  """
  return task.jobs[0].status == JobStatus.COMPLETED


#
# Task
#


def task_registered( app, task: models.Task ) -> bool:

  """
  Force all exps with ASSIGNED status
  """
  logger.debug("task_assigned")
  for job in task.jobs:
      job.status =  JobStatus.REGISTERED
      job.backend_job_id = -1
      job.retry  = 0
  return True


def task_assigned( app, task: models.Task ) -> bool:
  """
  Force all exps with ASSIGNED status
  """
  logger.debug("task_assigned")
  for job in task.jobs:
      job.status =  JobStatus.ASSIGNED
      job.backend_job_id = -1
      job.retry  = 0
  return True


def task_completed( app, task: models.Task ) -> bool:
  """
    Check if all exps into the task are completed
  """
  #logger.debug("task_completed")
  return all([job.status==JobStatus.COMPLETED for job in task.jobs])
  

def task_running( app, task: models.Task ) -> bool:
  """
    Check if any exps into the task is in assigned state
  """
  #logger.debug("task_running")
  return any([ ((job.status==JobStatus.ASSIGNED) or (job.status==JobStatus.RUNNING))  for job in task.jobs])


def task_finalized( app, task: models.Task ) -> bool:
  """
    Check if all exps into the task are completed or failed
  """
  #logger.debug("task_finalized")
  # NOTE: We have exps waiting to be executed here. Task should be in running state  
  return (not task_running(app, task)) and (not all([job.status==JobStatus.COMPLETED for job in task.jobs]) )



def task_killed( app, task: models.Task ) -> bool:
  """
    Check if all exps into the task are killed
  """
  #logger.debug("task_killed")
  return all([job.status==JobStatus.KILLED for job in task.jobs])
  

def task_broken( app, task: models.Task ) -> bool:
  """
    Broken all exps inside of the task
  """
  #logger.debug("task_broken")
  return all([job.status==JobStatus.BROKEN for job in task.jobs])


def retry_failed_jobs( app, task: models.Task ) -> bool:
  """
    Retry all exps inside of the task with failed status
  """
  #logger.debug("retry failed jobs")
  for job in task.jobs:
    if job.status == JobStatus.FAILED:
      if job.retry < 5:
        job.status = JobStatus.ASSIGNED
        job.retry +=1
        job.backend_job_id = -1 

  # NOTE: If we have exps to retry we must keep the current state and dont finalized the task
  return True



def task_removed( app, task: models.Task ):
  """
    Check if task removed
  """
  #logger.debug("task_removed")
  return task.to_remove
  

def task_kill( app, task: models.Task ):
  """
    Kill all exps
  """
  #logger.debug("task_kill")
  for job in task.jobs:
    if job.status == JobStatus.RUNNING:
      job.status = JobStatus.KILL
    else:
      job.status = JobStatus.KILLED
  return True



#
# Triggers
#


def trigger_task_kill( app, task: models.Task ) -> bool:
  """
    Put all exps to kill status when trigger
  """
  #logger.debug("trigger_task_kill")
  if task.state == TaskState.KILL:
    task.state = TaskState.WAITING
    return True
  else:
    return False


def trigger_task_retry( app, task: models.Task ) -> bool:
  """
    Move all exps to registered when trigger is retry given by external order
  """
  if task.state == TaskState.RETRY:

    if task.status == TaskStatus.FINALIZED:
      for job in task.jobs:
        if (job.status != JobStatus.COMPLETED):
          job.status = JobStatus.ASSIGNED
          job.retry  = 0 
          job.backend_job_id = -1

    elif (task.status == TaskStatus.KILLED) or (task.status == TaskStatus.BROKEN):
      for job in task.jobs:
        job.status = JobStatus.REGISTERED
        job.backend_job_id = -1
        job.retry  = 0 

    else:
      logger.error(f"Not expected task status ({task.status})into the task retry. Please check this!")
      return False
    
    task.state = TaskState.WAITING
    return True
  else:
    return False



def trigger_task_delete( app, task: models.Task ) -> bool:
  """
    Put all exps to kill status when trigger
  """
  #logger.debug("trigger_task_delete")
  if task.state == TaskState.DELETE:
    task.remove()
    if task.status == TaskStatus.RUNNING:
      task.kill()
    return True
  else:
    return False



#
# Schedule implementation
# 
class TaskScheduler(threading.Thread):

  def __init__(self, task_id, testing : bool=False):
    threading.Thread.__init__(self)
    #logger.debug("Creating schedule...")
    self.task_id  = task_id
    self.testing  = testing
    self.__stop   = threading.Event()
    self.compile()

    
  def stop(self):
    logger.debug("stopping service")
    self.__stop.set()


  def run(self):
    while (not self.__stop.is_set()):
      sleep(1)
      self.loop()


  def loop(self):


    db_service = get_db_service()
    try:
      with db_service() as session:
        #logger.debug("Treat jobs with status running but not alive into the classical runner.")
        # NOTE: Check if we have some job with running but not alive. If yes, return it to assigne status
        jobs = session.query(models.Job).filter( and_(models.Job.task_id==self.task_id, 
                                                          or_(models.Job.status==JobStatus.RUNNING, 
                                                              models.Job.status==JobStatus.PENDING)) ).with_for_update().all()
        for job_db in jobs:
          if not job_db.is_alive():
            #logger.debug(f"putting back job {job_db.job_id} with status {job_db.status} into the queue...")
            job_db.status = JobStatus.ASSIGNED
            job_db.backend_job_id = -1
            job_db.ping()
        session.commit()
    except:
      traceback.print_exc()
      #logger.error(e)
      return False
    
    task_status = TaskStatus.UNKNOWN

    # Update task states
    try:
      with db_service() as session:
        # NOTE: All tasks assigned to remove should not be returned by the database.
        #task = session().query(Task).filter(Task.status!=TaskStatus.REMOVED).with_for_update().all()
        task = session.query(models.Task).filter(models.Task.task_id==self.task_id).with_for_update().first()
        
        logger.debug(f"task in {task.status} status.")
        # Run all JobStatus triggers to find the correct transiction
        for state in self.states:
          # Check if the current JobStatus is equal than this JobStatus
          if state.source == task.status:
            try:
              res = state( self, task)
              if res:
                logger.debug(f"Moving task from {state.source} to {state.target} state.")
                task.status = state.target
                break
            except:
              logger.error(f"Found a problem to execute the transition from {state.source} to {state.target} state.")
              traceback.print_exc()
              break

        task.ping()
        task_status             = task.status
        session.commit()

 

    except:
      traceback.print_exc()
      return False

    if task_status in task_final_status:
       logger.debug(f"stopping task loop with status {task_status}")
       self.stop()

    return True




  #
  # Compile the JobStatus machine
  #
  def compile(self):

    logger.debug("Compiling all transitions...")

    class Transition:
      def __init__(self, source: TaskStatus , target: TaskStatus , relationship: list ):
        self.source = source
        self.target = target
        self.relationship = relationship
      def __call__(self, app, task: models.Task) -> bool:   
        for func in self.relationship:
          if not func(app, task):
            return False
        return True
    
    
    self.states = [
      Transition( source=TaskStatus.REGISTERED, target=TaskStatus.ASSIGNED   , relationship=[task_registered]),
    ]


    if self.testing:

      states = [
        Transition( source=TaskStatus.ASSIGNED  , target=TaskStatus.TESTING    , relationship=[test_job_assigned]                 ),
        Transition( source=TaskStatus.TESTING   , target=TaskStatus.TESTING    , relationship=[test_job_running]                  ),
        Transition( source=TaskStatus.TESTING   , target=TaskStatus.BROKEN     , relationship=[test_job_fail, task_broken]        ), 
        Transition( source=TaskStatus.TESTING   , target=TaskStatus.RUNNING    , relationship=[test_job_completed, task_assigned] ), 

      ]

      self.states.extend(states)

    else:
      states = [
        Transition( source=TaskStatus.ASSIGNED  , target=TaskStatus.RUNNING    , relationship=[task_assigned]                 ),
      ]
      self.states.extend(states)

    states = [

      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.COMPLETED  , relationship=[task_completed]                           ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.BROKEN     , relationship=[task_broken]                              ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.KILL       , relationship=[trigger_task_kill, task_kill]             ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.RUNNING    , relationship=[retry_failed_jobs, task_running]          ),
      Transition( source=TaskStatus.RUNNING   , target=TaskStatus.FINALIZED  , relationship=[task_finalized]                           ),      
      Transition( source=TaskStatus.FINALIZED , target=TaskStatus.RUNNING    , relationship=[trigger_task_retry]                       ),
      Transition( source=TaskStatus.BROKEN    , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                       ),
      Transition( source=TaskStatus.KILL      , target=TaskStatus.KILLED     , relationship=[task_killed]                              ),
      Transition( source=TaskStatus.KILLED    , target=TaskStatus.REGISTERED , relationship=[trigger_task_retry]                       ),

    ]

    self.states.extend(states)

    logger.debug(f"Schedule with a total of {len(self.states)} nodes into the graph.")

 