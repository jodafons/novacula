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
    id               = Column(Integer, primary_key=True)
    task_id          = Column(Integer)
    name             = Column(String, unique=True)
    jobs             = relationship("Job", order_by="Job.id", back_populates="task")
    status           = Column(Enum(TaskStatus), default=TaskStatus.ASSIGNED )
    start_time       = Column(DateTime, default=datetime.now())
    updated_time     = Column(DateTime, default=datetime.now())

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
     
    def update_status(self, status : TaskStatus):
        session = self.__session()
        try:
            task = session.query(Task).filter_by(task_id=self.task_id).one()
            setattr(task, "status", status)
            task.ping()
            session.commit()
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