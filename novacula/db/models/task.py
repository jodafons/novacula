__all__ = [
    "Task", 
    "DBTask", 
    "TaskStatus",
    ]

import enum 
from datetime import datetime
from typing import List
from sqlalchemy import Column, Integer, String, Enum, Float, DateTime, TEXT
from sqlalchemy.orm import load_only, relationship
from . import Base


minutes=60 # seconds



#
# tasks and jobs
#
class TaskStatus(enum.Enum):

    ASSIGNED       = "assigned"
    CREATING       = "creating"
    PENDING        = "pending"
    RUNNING        = "running"
    COMPLETED      = "completed"
    FINALIZED      = "finalized"
    FAILED         = "failed"
    


    

class Task (Base):

    __tablename__    = 'task'
    id               = Column(Integer, autoincrement=False, primary_key=True)
    task_id          = Column(String(64))
    name             = Column(String, unique=True)
    jobs             = relationship("Job", order_by="Job.id", back_populates="task")
    parents_str      = Column(String, default="[]")
    childrens_str    = Column(String, default="[]")
    status           = Column(Enum(TaskStatus), default=TaskStatus.ASSIGNED )
    start_time       = Column(DateTime)
    updated_time     = Column(DateTime)



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
    
    def ping(self):
        self.updated_time = datetime.now()

    
class DBTask:

    def __init__(self, task_id : str, session):
      self.task_id = task_id
      self.__session = session

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