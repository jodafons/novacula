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
from novacula.utils import random_id, random_token
from .models        import DBUser, Base
from .models        import User

__db_service = None


#
# DB services
#

class DBService:

    def __init__(self, db_string : str=os.environ.get("DB_STRING","")):
        self.__engine    = create_engine(db_string, pool_size=50, max_overflow=0)
        self.__session   = sessionmaker(bind=self.__engine,autocommit=False, autoflush=False)
        self.db_string   = db_string

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


    def check_user_existence( self, user_id : str ) -> bool: 
        return self.user(user_id).check_existence()


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
            
    def check_user_existence_by_name( self,  name : str ) -> bool:
        session = self.__session()
        try:
           user = session.query( 
                    session.query(User).filter_by(name=name).exists() 
           ).scalar()
           return user
        finally:
            session.close()    

    def fetch_user_from_token( self, access_token : str) -> str:
        session = self.__session()
        try:
           user = session.query(User).filter_by(access_token=access_token).one()
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
        token = "616b6b83328a45a2a844ecbaeeb76a6b011c26c3f69e46cbaea6f0c715e06590"
        new_user=User()
        new_user.user_id      = user_id
        new_user.token        = token
        new_user.access_key   = "SA29uOFvT5ypG75SQYuZ" #access_key
        new_user.secret_key   = "3OPvVp07nKpKhn6myaYtXztICNPGu3bvLt1v4Mwv" #access_token
        new_user.name         = name
        logger.info(f"Creating user {name} with id {user_id}")
        db_service.save_user(new_user)
        
    users   = db_service.session().query(User).all()
    user_id = db_service.fetch_user(name)
    access_token   = db_service.user(user_id).fetch_token()
    print()
    print(f"User {name} ({user_id}) created with token {access_token}")
    print()