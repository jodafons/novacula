
__all__ = ["DatasetAPIClient"]

import os
import io
import json
import shutil
import hashlib
import zipfile
import tempfile

from tqdm import tqdm
from expand_folders import expand_folders
from urllib.parse import urljoin
from typing import List, Union
from loguru import logger


from minio import Minio
from minio.error import S3Error
#from minio.datatypes import ObjectLifeCycle 
#from minio.datatypes import Tagging
from minio.commonconfig import Tags

from novacula.exceptions import DatasetNotFound


class DatasetAPIClient:


    def __init__(self, session ):
        self.session = session
        MINIO_ENDPOINT = "localhost:9000"
        ACCESS_KEY = "minio"
        SECRET_KEY = "minio123456789"
        SECURE_CONN = False  # Set to True if using HTTPS/TLS
        self.client  = client = Minio(
            MINIO_ENDPOINT,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            secure=SECURE_CONN
        )


    def check_existence( 
        self, 
        name : str,
    ) -> bool:
        return self.client.bucket_exists(name)

    def check_file_existence( self, name, filename ) -> bool:
        if not self.check_existence(name):
            return False
        try:
            self.client.stat_object(name, filename)
            return True
        except S3Error as err:
            return False


    def create(
        self, 
        name        : str, 
        description : str="",
        days_to_expiration : int = 30,
    ) -> bool:

        if self.check_existence( name ):
            return False
        #if not name.startswith(f"user.{username}."):
        self.client.make_bucket(name)

   
        
    def upload( 
        self, 
        name       : str,
        files      : Union[List[str], str],
        as_link    : bool=False,
        overwrite  : bool=False,
    ) -> bool:

        if type(files) == str and os.path.exists(files):
            files = expand_folders(files)
        else:
            raise RuntimeError(f"the path {files} does not exist.")

        if not self.client.bucket_exists(name):
            raise DatasetNotFound(f"the dataset {name} does not exist into the server.")

        tags = Tags(for_object=True)
        tags["is_link"] = str(as_link)
        for filepath in tqdm( files , desc = "uploading file...",ncols=100 ):
            filename    = filepath.split('/')[-1]   

            if self.check_file_existence(name, filename):
                stat = self.client.stat_object(name, filename)
                remote_etag = stat.etag.lower() # ETags are typically quoted and case-insensitive in comparison
                local_etag  = hashlib.md5(open(filepath,'rb').read()).hexdigest().lower()
                if remote_etag == local_etag:
                    logger.info(f"the file {filename} already exists into the server. skipping...")
                    continue

            try:
                if as_link: # if a link, upload a json file with the path
                    data_dict = {"path": filepath, "filename": filename}
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
            except S3Error as err:
                RuntimeError(f"Code: {err.code}: {err.message}")
        return True


    def download(
        self, 
        name            : str,
        targetfolder    : str = None,
        as_link         : bool=False,
    ) -> bool:

        if not self.client.bucket_exists(name):
            raise DatasetNotFound(f"the dataset {name} does not exist into the server.")

        if not targetfolder:
            targetfolder = f"{os.getcwd()}/{name}"

        os.makedirs(targetfolder,exist_ok=True)
        objects = self.client.list_objects(name, recursive=True)
        object_names = [obj.object_name for obj in objects]


        for filename in tqdm( object_names , desc = "downloading file...",ncols=100 ):
            try:
                targetpath = f"{targetfolder}/{filename}"

                # Get the tags for the object
                tags = self.client.get_object_tags(name, filename)
                if tags.get("is_link") == "True":
                    # Get the original file path from the link
                    data_dict  = json.loads(self.client.get_object(name, filename).read())
                    targetfile = data_dict.get("path")
                    os.symlink( filename, targetfile )
                else:
                    if os.path.exists(targetpath):
                        stat = self.client.stat_object(name, filename)
                        remote_etag = stat.etag.lower() # ETags are typically quoted and case-insensitive in comparison
                        local_etag  = hashlib.md5(open(targetpath,'rb').read()).hexdigest().lower()
                        if remote_etag == local_etag:
                            logger.info(f"the file {filename} already exists into the target folder. skipping...")
                            continue

                    self.client.fget_object(
                        bucket_name=name,
                        object_name=filename,
                        file_path=targetpath,
                    )
            except S3Error as err:
                RuntimeError(f"Code: {err.code}: {err.message}")
        return True