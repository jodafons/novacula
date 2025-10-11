__all__ = ["get_io_service", "get_auth_service"]

import os
import shutil
import pickle
from minio import Minio

from loguru          import logger
from typing          import Any, Callable, List
from expand_folders  import expand_folders
from novacula        import schemas, random_id, md5checksum


__auth_service = None
__io_service = None




class MinioSession:

    def __init__(self, endpoint: str, access_key : str, secret_key : str ,secure : bool=False):
        self.endpoint = endpoint
        self.secure   = secure
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def is_valid(self) -> bool:
        try:
            self.client.list_buckets()
            return True
        except:
            return False


    def create( self, 
                name        : str, 
                description : str="",
    ) -> str:
        if not self.client.bucket_exists(name):
            dataset_id = random_id()
            tags = Tags()
            tags['dataset_id'] = dataset_id
            tags['description'] = description
            self.client.make_bucket(name, tags=tags)
            return dataset_id
        else: 
            tags = self.client.get_object_tags(name)
            return tags['dataset_id']

    def check_existence_by_name( self, name : str) -> bool:
        return self.client.bucket_exists(name)

    def check_existence( self, dataset_id : str) -> str:
        buckets = self.client.list_buckets()
        for bucket in buckets:
            tags = self.client.get_object_tags(bucket.name)
            if tags.get("dataset_id") == dataset_id:
                return True
        return False

    def check_file_existence( self, name : str, object_name : str) -> bool:
        try:
            self.client.stat_object(name, object_name)
            return True
        except:
            return False

    def save( self, name : str, object_name : str, file_path : str, as_link : bool=False) -> str:
        try:
            file_id = random_id()
            tags = Tags(for_object=True)
            tags['file_id'] = file_id
            taks['syslink'] = 'True' if as_link else 'False'
            if as_link: # if a link, upload a json file with the path
                data_dict = {"path": file_path}
                json_string = json.dumps(data_dict, indent=4)
                data_bytes = json_string.encode('utf-8')
                data_stream = io.BytesIO(data_bytes)
                data_length = len(data_bytes)
                result = self.client.put_object(
                            bucket_name=name,
                            object_name=filename,
                            data=data_stream,
                            length=data_length, # Must provide the exact content length
                            content_type='application/json', # Set the correct MIME type
                            tags=tags,
                        )
            else: # if not a link, upload the file directly
                result = self.client.fput_object(
                    bucket_name=name,
                    object_name=filename,
                    file_path=filepath,
                    content_type='application/octet-stream',
                    # You can add metadata or tags here if needed
                    tags=tags,
                )
            return file_id
        except:
            return None

    def load( self, file_id : str) -> bool:
        try:
            buckets = self.client.list_buckets()
            for bucket in buckets:
                objects = self.client.list_objects(bucket.name, recursive=True)
                for obj in objects:
                    tags = self.client.get_object_tags(bucket.name, obj.object_name)
                    if tags.get("file_id") == file_id:
                        return obj.object_name
            return None
        except:
            return None
    


class IoService:
    def __init__(self, endpoint: str, secure : bool=False):
        self.endpoint = endpoint
        self.secure   = secure

    def session( self, user : str, token : str) -> MinioSession:
        client = MinioSession(
            endpoint=self.endpoint,
            access_key=user,
            secret_key=token,
            secure=self.secure
        )
        return client



#
# get database service
#


def get_io_service( endpoint : str="", secure : bool=False):
    global __io_service
    if not __io_service:
        __io_service = IoService(endpoint, secure)
    return  __io_service

def get_auth_service( endpoint : str="", secure : bool=False):
    global __auth_service
    if not __auth_service:
        __auth_service = AuthService(endpoint, secure)
    return  __auth_service

