__all__ = [
    "Job" , 
    "DBJob", 
    "JobStatus",
    ]

import enum 

from datetime import datetime
from sqlalchemy.orm import load_only, relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Enum

from . import Base
from .task import Task


minutes=60 # seconds




class JobStatus(enum.Enum):

    ASSIGNED    = "assigned"
    PENDING     = "pending"
    RUNNING     = "running"
    COMPLETED   = "completed"
    FAILED      = "failed"
    KILL        = "kill"
    KILLED      = "killed"
    

class Job (Base):

    __tablename__       = 'job'
    id                  = Column(Integer, primary_key=True)
    job_id              = Column(Integer)
    task_id             = Column(Integer)
    retry               = Column(Integer , default=0)
    task                = relationship("Task", back_populates="jobs")
    taskid              = Column(Integer, ForeignKey('task.id'))
    status              = Column(Enum(JobStatus), default=JobStatus.ASSIGNED )
    start_time          = Column(DateTime, default=datetime.now())
    updated_time        = Column(DateTime, default=datetime.now())

    
    def ping(self):
        self.updated_time = datetime.now()
        
    def is_alive(self, timeout=5*minutes):
        return (datetime.now() - self.updated_time).total_seconds() < timeout



class DBJob:

    def __init__(self, task_id : int, job_id : int, session):
        self.job_id = job_id
        self.task_id = task_id
        self.__session = session

    def update_status(self, status : JobStatus):
        session = self.__session()
        try:
            job = session.query(Job).filter_by(task_id=self.task_id, job_id=self.job_id).one()
            setattr(job, "status", status)
            job.ping()
            session.commit()
        finally:
            session.close()

    def fetch_status(self) -> JobStatus:
        session = self.__session()
        try:
            fields = [Job.status]
            job = (
                session.query(Job)
                .filter_by(task_id=self.task_id, job_id=self.job_id).one()
                .options(load_only(*fields))
                .one()
            )
            return job.status
        finally:
            session.close()

    def ping(self):
        session = self.__session()
        try:
            job = session.query(Job).filter_by(task_id=self.task_id, job_id=self.job_id).one()
            job.ping()
            session.commit()
        finally:
            session.close()
            
    def start_clock(self):
        session = self.__session()
        try:
            job = session.query(Job).filter_by(task_id=self.task_id, job_id=self.job_id).one()
            job.start_time = datetime.now()
            session.commit()
        finally:
            session.close()