__all__ = [
    "get_db_service", 
    "recreate_db",
    "create_user"
]


import os

from sqlalchemy     import create_engine
from sqlalchemy.orm import sessionmaker
from datetime       import datetime
from loguru         import logger
from maestro.utils  import random_id
from .models        import DBJob, DBTask, DBDataset, DBUser, Base
from .models        import Job, Task, Dataset, User

__db_service = None


#
# DB services
#

class DBService:

    def __init__(self, db_string : str=os.environ.get("DB_STRING","")):
        self.__engine    = create_engine(db_string, pool_size=50, max_overflow=0)
        self.__session   = sessionmaker(bind=self.__engine,autocommit=False, autoflush=False)
        self.db_string   = db_string

    def task(self, task_id : str) -> DBTask:
        return DBTask(task_id, self.__session)

    def job(self, job_id : str) -> DBJob:
        return DBJob(job_id, self.__session)

    def dataset(self, dataset_id : str) -> DBDataset:
        return DBDataset(dataset_id, self.__session)

    def user(self, user_id : str) -> DBUser:
        return DBUser( user_id, self.__session)


    def __call__(self):
        return self.__session()
    
    def session(self):
        return self.__session()
    
    def engine(self):
        return self.__engine

    def save_user(self, user: User):
        session = self.session()
        try:
            session.add(user)
            session.commit()
        finally:
            session.close()
            
    def save_job(self, job : Job):
        session = self.session()
        try:
            session.add(job)
            session.commit()
        finally:
            session.close()

  
    def save_task(self, task: Task):
        session = self.session()
        task.start_time = datetime.now()
        try:
            session.add(task)
            session.commit()
        finally:
            session.close()
            
    def save_dataset(self, dataset: Dataset):
        session = self.session()
        try:
            session.add(dataset)
            session.commit()
        finally:
            session.close()

    def check_user_existence( self, user_id : str ) -> bool: 
        return self.user(user_id).check_existence()

    def check_task_existence( self, task_id : str ) -> bool:
        return self.task(task_id).check_existence()

    def check_job_existence( self, job_id : str ) -> bool:
        return self.job(job_id).check_existence()

    def check_user_existence_by_name( self,  name : str ) -> bool:
        session = self.__session()
        try:
           user = session.query( 
                    session.query(User).filter_by(name=name).exists() 
           ).scalar()
           return user
        finally:
            session.close()    

    def check_token_existence( self, token : str ) -> bool:
        session = self.__session()
        try:
           user = session.query( 
                    session.query(User).filter_by(token=token).exists() 
           ).scalar()
           return user
        finally:
            session.close()

    def check_task_existence_by_name( self,  name : str ) -> bool:
        session = self.__session()
        try:
           dataset = session.query( 
                    session.query(Task).filter_by(name=name).exists() 
           ).scalar()
           return dataset
        finally:
            session.close()    

    def fetch_task_from_name( self,  name : str) -> str:
        session = self.__session()
        try:
           task = session.query(Task).filter_by(name=name).one()
           return task.task_id
        finally:
            session.close() 
            
    def check_user_existence_by_name( self,  name : str ) -> bool:
        session = self.__session()
        try:
           user = session.query( 
                    session.query(User).filter_by(name=name).exists() 
           ).scalar()
           return user
        finally:
            session.close()    

    def fetch_dataset_from_name( self,  name : str) -> str:
        session = self.__session()
        try:
           dataset = session.query(Dataset).filter_by(name=name).one()
           return dataset.dataset_id
        finally:
            session.close()

    def fetch_user_from_token( self, token : str) -> str:
        session = self.__session()
        try:
           user = session.query(User).filter_by(token=token).one()
           return user.user_id
        finally:
            session.close()

    def fetch_user( self, name : str) -> str:
        session = self.__session()
        try:
           user = session.query(User).filter_by(name=name).one()
           return user.user_id
        finally:
            session.close()         
   
    def check_dataset_existence_by_name( self,  name : str ) -> bool:
        session = self.__session()
        try:
           dataset = session.query( 
                    session.query(Dataset).filter_by(name=name).exists() 
           ).scalar()
           return dataset
        finally:
            session.close()    
   
 
def get_db_service( db_string : str=os.environ.get("DB_STRING","")):
    global __db_service
    if not __db_service:
        __db_service = DBService(db_string)
    return __db_service


def recreate_db():
    db_service = get_db_service()
    Base.metadata.drop_all(db_service.engine())
    Base.metadata.create_all(db_service.engine())
    
def create_user():
    db_service = get_db_service()
    name = os.environ["USER"]
    if not db_service.check_user_existence_by_name(name):
        user_id = random_id()
        #token = random_token()
        token = "fcad6b3734b74e7e901c58903dc416f8c5029cf224714f518f3f04ac379d7b7a"
        user = User(user_id=user_id, name=name, token=token)
        logger.info(f"Creating user {name} with id {user_id}")
        db_service.save_user(user)
        
    users = db_service.session().query(User).all()
    user_id = db_service.fetch_user(name)
    token = db_service.user(user_id).fetch_token()
    print()
    print(f"User {name} ({user_id}) created with token {token}")
    print()