__all__ = [
    "get_db_service", 
    "recreate_db",
]


import os

from sqlalchemy     import create_engine
from sqlalchemy.orm import sessionmaker
from datetime       import datetime
from loguru         import logger
from .models        import DBJob, DBTask, Base
from .models        import Job, Task

__db_service = None


#
# DB services
#

class DBService:

    def __init__(self, filename : str):
        self.filename = filename
        path = f'duckdb:///{filename}'
        self.__engine    = create_engine(path)
        self.__session   = sessionmaker(bind=self.__engine)

    def task(self, task_id : str) -> DBTask:
        return DBTask(task_id, self.__session)

    def job(self, job_id : str) -> DBJob:
        return DBJob(job_id, self.__session)

    def __call__(self):
        return self.__session()
    
    def session(self):
        return self.__session()
    
    def engine(self):
        return self.__engine

    def save_task(self, task: Task):
        session = self.session()
        task.start_time = datetime.now()
        try:
            session.add(task)
            session.commit()
        finally:
            session.close()

    def fetch_task_from_name( self,  name : str) -> str:
        session = self.__session()
        try:
           task = session.query(Task).filter_by(name=name).one()
           return task.task_id
        finally:
            session.close() 


def get_db_service( filename: str = "local.db" ) -> DBService:
    global __db_service
    if not __db_service:
        __db_service = DBService(filename)
    return __db_service

def recreate_db( filename: str = "local.db" ):
    db_service = get_db_service(filename)
    Base.metadata.drop_all(db_service.engine())
    Base.metadata.create_all(db_service.engine())
    db_service.session().close()
