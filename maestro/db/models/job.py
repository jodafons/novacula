__all__ = [
    "Job" , 
    "DBJob", 
    "JobStatus",
    "job_status",
    "job_final_status"
    ]

import enum 

from datetime import datetime
from sqlalchemy.orm import load_only, relationship
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Enum

from . import Base
from .task import Task


minutes=60 # seconds




class JobStatus(enum.Enum):

    REGISTERED  = "registered"
    ASSIGNED    = "assigned"
    CREATING    = "creating"
    PENDING     = "pending"
    RUNNING     = "running"
    COMPLETED   = "completed"
    FAILED      = "failed"
    KILL        = "kill"
    KILLED      = "killed"
    BROKEN      = "broken"
    UNKNOWN     = "unknown"

job_status       = [JobStatus.REGISTERED, JobStatus.ASSIGNED, JobStatus.PENDING, JobStatus.RUNNING, JobStatus.FAILED, JobStatus.KILL, JobStatus.KILLED, JobStatus.BROKEN, JobStatus.COMPLETED]
job_final_status = [JobStatus.BROKEN, JobStatus.FAILED, JobStatus.KILLED, JobStatus.COMPLETED]


class Job (Base):

    __tablename__       = 'job'
    id                  = Column(Integer, primary_key = True)
    job_id              = Column(String(64))
    task_id             = Column(String(64))
    user_id             = Column(String(64))
    command             = Column(String , default="")
    workarea            = Column(String)
    timer               = Column(DateTime)
    priority            = Column(Integer , default=1)
    device              = Column(String  , default="cpu")
    priority            = Column(Integer , default=1)
    retry               = Column(Integer , default=0)
    task                = relationship("Task", back_populates="jobs")
    taskid              = Column(Integer, ForeignKey('task.id'))
    partition           = Column(String , default="")
    status              = Column(Enum(JobStatus), default=JobStatus.REGISTERED )
    
    job_index           = Column(Integer, default=0)
    backend_job_id      = Column(Integer, default=-1)
    backend_state       = Column(String(64), default="")

    # job resource reservation
    used_sys_memory_mb       = Column(Float  , default=0 )
    used_gpu_memory_mb       = Column(Float  , default=0 )
    reserved_cpu_number      = Column(Integer, default=4 )
    reserved_time_seconds    = Column(Float  , default=0 )
    reserved_gpu_memory_mb   = Column(Float  , default=0 )
    reserved_sys_memory_mb   = Column(Float  , default=0 )    
    job_type                 = Column(String , default="")

    start_time               = Column(DateTime)
    updated_time             = Column(DateTime)

    
    def ping(self):
        self.updated_time = datetime.now()
        
    def is_alive(self, timeout=5*minutes):
        return (datetime.now() - self.updated_time).total_seconds() < timeout



class DBJob:

    def __init__(self, job_id : str, session):
        self.job_id = job_id
        self.__session = session

    def check_existence(self):
        session = self.__session()
        try:
           job = session.query( 
                    session.query(Task).filter_by(job_id=self.job_id).exists() 
           ).scalar()
           return job
        finally:
            session.close()

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

    def fetch_task(self):
        session = self.__session()
        try:
            fields = [Job.task_id]
            job = (
                session.query(Job)
                .filter_by(job_id=self.job_id)
                .options(load_only(*fields))
                .one()
            )
            return job.task_id
        finally:
            session.close()
    
    def fetch_index(self):
        session = self.__session()
        try:
            fields = [Job.job_index]
            job = (
                session.query(Job)
                .filter_by(job_id=self.job_id)
                .options(load_only(*fields))
                .one()
            )
            return job.job_index
        finally:
            session.close()
    def fetch_owner(self):
        session = self.__session()
        try:
            job = (
                session.query(Job)
                .filter_by(job_id=self.job_id)
                .one()
            )
            return job.task.user_id
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
                
  