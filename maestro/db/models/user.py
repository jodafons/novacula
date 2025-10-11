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
    token         = Column(String(64))
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

    def check_token(self, token : str) -> bool:
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           return token==user.token
        finally:
            session.close()

    def update_token(self, new_token : str):
        session = self.__session()
        try:
           user = session.query(User).filter_by(user_id=self.user_id).one()
           user.token=new_token
           session.commit()
        finally:
            session.close()

    def fetch_token(self) -> str:
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

