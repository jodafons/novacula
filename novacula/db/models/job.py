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
    

class Job (Base):

    __tablename__       = 'job'
    id                  = Column(Integer, autoincrement=False, primary_key=True)
    job_id              = Column(String(64))
    task_id             = Column(String(64))
    command             = Column(String , default="")
    workarea            = Column(String)
    timer               = Column(DateTime)
    retry               = Column(Integer , default=0)
    task                = relationship("Task", back_populates="jobs")
    taskid              = Column(Integer, ForeignKey('task.id'))
    partition           = Column(String , default="")
    status              = Column(Enum(JobStatus), default=JobStatus.ASSIGNED )
    partition           = Column(String)
    memory_mb           = Column(Float  , default=0 )
    cpu_cores           = Column(Float  , default=0 )
    start_time          = Column(DateTime)
    updated_time        = Column(DateTime)

    
    def ping(self):
        self.updated_time = datetime.now()
        
    def is_alive(self, timeout=5*minutes):
        return (datetime.now() - self.updated_time).total_seconds() < timeout



class DBJob:

    def __init__(self, job_id : str, session):
        self.job_id = job_id
        self.__session = session


    def update_status(self, status : JobStatus):
        session = self.__session()
        try:
            job = session.query(Job).filter_by(job_id=self.job_id).one()
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
                .filter_by(job_id=self.job_id)
                .options(load_only(*fields))
                .one()
            )
            return job.status
        finally:
            session.close()


    def ping(self):
        session = self.__session()
        try:
            job = session.query(Job).filter_by(job_id=self.job_id).one()
            job.ping()
            session.commit()
        finally:
            session.close()
            
    def start(self):
        session = self.__session()
        try:
            job = session.query(Job).filter_by(job_id=self.job_id).one()
            job.start_time = datetime.now()
            session.commit()
        finally:
            session.close()