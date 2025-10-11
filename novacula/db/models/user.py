__all__ = ["DBUser", "User"]

import enum

from sqlalchemy import Column, Integer, String
from sqlalchemy import Enum as SQLEnum
from dataclasses import dataclass
from . import Base



@dataclass
class User(Base):

    __tablename__ = "user"
    id            = Column(Integer, primary_key=True)
    user_id       = Column(String(64))
    token         = Column(String, unique=True)
    secret_key    = Column(String)
    access_key    = Column(String)
    name          = Column(String, unique=True)
  
  
class DBUser:
    
    def __init__(self, user_id : str, session):
        self.__session = session
        self.user_id   = user_id

    def check_existence(self):
        session = self.__session()
        try:
           user = session.query( 
                    session.query(User).filter_by(user_id=self.user_id).exists() 
           ).scalar()
           return user
        finally:
            session.close()

    def check_access_key(self, key : str) -> bool:
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           return key==user.access_key
        finally:
            session.close()

    def check_secret_key(  self, token : str) -> bool:
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           return token==user.token
        finally:
            session.close()

    def update_access_key(self, new_key : str):
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           user.access_key=new_key
           session.commit()
        finally:
            session.close()

    def update_secret_key(  self, new_token : str):
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           user.token=new_token
           session.commit()
        finally:
            session.close()


    def fetch_access_key(self) -> str:
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           return user.access_key
        finally:
            session.close()

    def fetch_secret_key(  self) -> str:
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           return user.secret_key
        finally:
            session.close()


    def fetch_token(  self) -> str:
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           return user.token
        finally:
            session.close()


    def fetch_name(self) -> str:
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           return user.name
        finally:
            session.close()

