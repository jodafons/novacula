__all__ = [
    "get_db_service", 
    "create_db",
]



from typing         import Dict
from sqlalchemy     import create_engine
from sqlalchemy.orm import sessionmaker
from datetime       import datetime
from .models        import DBJob, DBTask, Base
from .models        import Task, job_status

__db_service = None


#
# DB services
#

WAL_ENABLED_CONNECT_ARGS = {
    "uri": True,  # Enables connection string to be treated as a URI
    "pragmas": {
        "journal_mode": "WAL", # Set the journaling mode
        "foreign_keys": "ON"   # Good practice to always enable FKs in SQLite
    }
}


class DBService:

    def __init__(self, db_file : str):
        self.db_file    = db_file
        DATABASE_URL = f"sqlite:///{db_file}"
        self.__engine    = create_engine(DATABASE_URL,
                                         #connect_args=WAL_ENABLED_CONNECT_ARGS,
                                         echo=False)
        self.__session   = sessionmaker(bind=self.__engine)

    def task(self, task_id : int) -> DBTask:
        return DBTask(task_id, self.__session)

    def job(self, task_name : str, job_id : int) -> DBJob:
        return DBJob(task_name, job_id, self.__session)

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
            
    def fetch_table_from_task( self, name : str) -> Dict[str,int]:
        session = self.__session()
        try:
           jobs = session.query(Task).filter_by(name=name).one().jobs
           table = {status.value:0 for status in job_status}
           for job_db in jobs:
               table[job_db.status.value] += 1           
           return table
        finally:
            session.close()


def get_db_service( filename: str = "local.db" ) -> DBService:
    global __db_service
    if not __db_service:
        __db_service = DBService(filename)
    return __db_service

def create_db( filename: str = "local.db" ):
    db_service = get_db_service(filename)
    Base.metadata.drop_all(db_service.engine())
    Base.metadata.create_all(db_service.engine())
    db_service.session().close()
