__all__ = [
           "DBDataset", 
           "Dataset", 
           "File",
           "DatasetType", 
           "FileType",
           ]

import enum

from datetime import datetime
from typing import List
from dataclasses import dataclass
from sqlalchemy.orm import load_only, relationship
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Boolean, Enum


# local package
from . import Base


class DatasetType(enum.Enum):
    FILES      = "files"
    IMAGE      = "image"


@dataclass
class Dataset(Base):
    __tablename__ = "dataset"

    id             = Column(Integer, primary_key=True)
    dataset_id     = Column(String(64))
    user_id        = Column(String(64))
    name           = Column(String)
    updated_time   = Column(DateTime)
    description    = Column(String, default="")
    visible        = Column(Boolean  , default=True)
    data_type      = Column(Enum(DatasetType) , default=DatasetType.FILES)
    files          = relationship("File", order_by="File.id", back_populates="dataset")


class FileType(enum.Enum):
    LINK  = "link"
    DATA  = "data"


@dataclass
class File(Base):
    __tablename__ = "file"
    id            = Column(Integer, primary_key=True)
    file_id       = Column(String(64))
    dataset_id    = Column(String(64))
    file_md5      = Column(String(64))
    name          = Column(String)
    file_type     = Column(Enum(FileType) , default=FileType.DATA)
    path          = Column(String)
    updated_time  = Column(DateTime)     
    dataset       = relationship("Dataset", back_populates="files")
    datasetid     = Column(Integer, ForeignKey('dataset.id'))
  

class DBDataset:

    def __init__(self, dataset_id : str, session):
      self.dataset_id=dataset_id
      self.__session = session

    def check_existence(self):
        session = self.__session()
        try:
           dataset = session.query( 
                    session.query(Dataset).filter_by(dataset_id=self.dataset_id).exists() 
           ).scalar()
           return dataset
        finally:
            session.close()  
            
    def check_file_existence_by_name(self, filename : str):
        session = self.__session()
        try:
           dataset = session.query( 
                    session.query(File).filter_by(dataset_id=self.dataset_id).filter_by(name=filename).exists() 
           ).scalar()
           return dataset
        finally:
            session.close()  

    def fetch_name(self):
        session = self.__session()
        try:
            fields = [Dataset.name]
            dataset = (
                session.query(Dataset)
                .filter_by(dataset_id=self.dataset_id)
                .options(load_only(*fields))
                .one()
            )
            return dataset.name
        finally:
            session.close()

    def fetch_type(self) -> DatasetType:
        session = self.__session()
        try:
            fields = [Dataset.dataset_type]
            dataset = (
                session.query(Dataset)
                .filter_by(dataset_id=self.dataset_id)
                .options(load_only(*fields))
                .one()
            )
            return dataset.dataset_type
        finally:
            session.close()

    def fetch_owner(self):
        session = self.__session()
        try:
            fields = [Dataset.user_id]
            dataset = (
                session.query(Dataset)
                .filter_by(dataset_id=self.dataset_id)
                .options(load_only(*fields))
                .one()
            )
            return dataset.user_id
        finally:
            session.close()

    def get_all_file_ids(self) -> List[str]:
        session = self.__session()
        try:
            dataset = session.query(Dataset).filter_by(dataset_id=self.dataset_id).one()
            return {f.file_id:f.name for f in dataset.files}
        finally:
            session.close()