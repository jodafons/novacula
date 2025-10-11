__all__ = [
    "Task", 
    "DBTask", 
    "TaskStatus",
    "TaskState",
    "task_final_status",
    ]

import enum 
from datetime import datetime
from typing import List
from sqlalchemy import Column, Integer, String, Enum, Float, DateTime, TEXT
from sqlalchemy.orm import load_only, relationship
from maestro import schemas
from . import Base


minutes=60 # seconds



#
# tasks and jobs
#
class TaskStatus(enum.Enum):

    PRE_REGISTERED = "pre_registered"
    REGISTERED     = "registered"
    ASSIGNED       = "assigned"
    CREATING       = "creating"
    PENDING        = "pending"
    RUNNING        = "running"
    COMPLETED      = "completed"
    FINALIZED      = "finalized"
    FAILED         = "failed"
    KILL           = "kill"
    KILLED         = "killed"
    BROKEN         = "broken"
    REMOVED        = "removed"
    UNKNOWN        = "unknown"

class TaskState(enum.Enum):
    WAITING = "waiting"
    RETRY   = "retry"
    KILL    = "kill"
    DELETE  = "delete"


task_final_status = [TaskStatus.COMPLETED,TaskStatus.KILLED, TaskStatus.FAILED, TaskStatus.BROKEN, TaskStatus.FINALIZED]


    

class Task (Base):

    __tablename__    = 'task'
    id               = Column(Integer, primary_key = True)
    task_id          = Column(String(64))
    user_id          = Column(String(64))
    name             = Column(String, unique=True)
    jobs             = relationship("Job", order_by="Job.id", back_populates="task")
    priority         = Column(Integer, default=1)
    parents_str      = Column(String, default="[]")
    childrens_str    = Column(String, default="[]")
    task_inputs_str  = Column(TEXT, default="{}")
    
    partition        = Column(String)
    status           = Column(Enum(TaskStatus), default=TaskStatus.REGISTERED )
    state            = Column(Enum(TaskState) , default=TaskState.WAITING )
    
    description      = Column(String(64), default="")
    start_time       = Column(DateTime)
    updated_time     = Column(DateTime)


    @property
    def task_inputs(self) -> schemas.TaskInputs:
        return schemas.TaskInputs(**schemas.json_decode(self.task_inputs_str))

    @task_inputs.setter
    def task_inputs(self, task_inputs : schemas.TaskInputs):
        self.task_inputs_str=schemas.json_encode(task_inputs.model_dump())

    @property 
    def parents( self ) -> List[str]:
        return eval(self.parents_str)

    @parents.setter
    def parents( self, parents : List[str]):
        self.parents_str = str(parents)

    def add_parent( self, task_id : str ):
        parents = eval(self.parents_str); parents.append(task_id)
        self.parents_str = str(parents)
    
    def add_child( self, task_id : str ):
        childrens = eval(self.childrens_str); childrens.append(task_id)
        self.childrens_str = str(childrens)

    def __add__ (self, exp):
      self.jobs.append(exp)
      return self
    
    def completed(self):
      return self.status==TaskStatus.COMPLETED

    def kill(self):
      self.state = TaskState.KILL

    def retry(self):
      self.state = TaskState.RETRY

    def reset(self):
      self.state = TaskState.WAITING

    def delete(self):
      self.state = TaskState.DELETE

    def ping(self):
        self.updated_time = datetime.now()

    
class DBTask:

    def __init__(self, task_id : str, session):
      self.task_id = task_id
      self.__session = session

    def check_existence(self):
        session = self.__session()
        try:
           task = session.query( 
                    session.query(Task).filter_by(task_id=self.task_id).exists() 
           ).scalar()
           return task
        finally:
            session.close()  

    def fetch_status(self) -> TaskStatus:
        session = self.__session()
        try:
            fields = [Task.status]
            task = (
                session.query(Task)
                .filter_by(task_id=self.task_id)
                .options(load_only(*fields))
                .one()
            )
            return task.status
        finally:
            session.close()


    def fetch_owner(self):
        session = self.__session()
        try:
            fields = [Task.user_id]
            task = (
                session.query(Task)
                .filter_by(task_id=self.task_id)
                .options(load_only(*fields))
                .one()
            )
            return task.user_id
        finally:
            session.close()


    def fetch_task_inputs(self) -> schemas.TaskInputs:
        session = self.__session()
        try:
            fields = [Task.task_inputs_str]
            task = (
                session.query(Task)
                .filter_by(task_id=self.task_id)
                .options(load_only(*fields))
                .one()
            )
            return task.task_inputs
        finally:
            session.close()

    def fetch_parents(self) -> List[str]:
        session = self.__session()
        try:
            fields = [Task.parents_str]
            task = (
                session.query(Task)
                .filter_by(task_id=self.task_id)
                .options(load_only(*fields))
                .one()
            )
            return task.parents
        finally:
            session.close()
            
    def fetch_name(self):
        session = self.__session()
        try:
            fields = [Task.name]
            task = (
                session.query(Task)
                .filter_by(task_id=self.task_id)
                .options(load_only(*fields))
                .one()
            )
            return task.name
        finally:
            session.close()
