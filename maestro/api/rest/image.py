__all__ = ["DellRuntimeAPIClient"]


import os
import shutil
import hashlib
import zipfile
import tempfile

from tqdm import tqdm
from expand_folders import expand_folders
from urllib.parse import urljoin
from typing import List, Union
from loguru import logger
from maestro import schemas, md5checksum
from maestro.exceptions import RuntimeError





class ImageAPIClient:


    def __init__(self, session ):
        self.session = session
        
    #
    # This method is used to check the existence of an image.
    #
    def check_existence( 
        self, 
        name : str,
    ) -> bool:
        payload = { 'params_str' : schemas.json_encode({'name':name}) }
        res = self.session.put(f"/remote/image/options/exist", data=payload)
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        else:
            return res[2].json()

    #
    # This method is used to get the identity of an image.
    #
    def identity( 
        self, 
        name : str,
    ) -> str:
        payload = {"name":name}
        payload = {"params_str":schemas.json_encode(payload)}

        res = self.session.put(f"/remote/image/options/identity", data=payload)
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        else:
            return res[2].json()
       
    #
    # 
    # 
    def create_and_upload( 
        self, 
        name           : str,
        filepath       : str,
        description    : str="",
        as_link        : bool=False,
    ) -> str:

        if not os.path.exists(filepath):
            raise RuntimeError(f"the path {filepath} does not exist.")

        zipfolder   = tempfile.mkdtemp()
        zipfilename = f"{zipfolder}/file.zip"
        filename    = filepath.split('/')[-1]
        file_md5    = md5checksum(filepath)
        payload = {'name':name, 'filename':filename}
        payload = {"params_str":schemas.json_encode(payload)}
        
        res = self.session.put(f"/remote/image/options/exist", data=payload)
        if res[0]!=200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        
        exist = res[2].json()
        if exist:
           raise RuntimeError(f"the image with name {name} exist into the database.")
        
        payload = {'name'               :name, 
                   'filename'           :filename, 
                   'filepath'           :filepath,
                   'description'        :description, 
                   'expected_file_md5'  :file_md5}
        if not as_link:
            with zipfile.ZipFile(f"{zipfilename}", "w") as zip:
                zip.write(filepath, filename, compress_type=zipfile.ZIP_DEFLATED)
            with open(zipfilename, 'rb') as f:
                res = self.session.put(f"/remote/image/create_and_upload", data=payload, files={"file":f})
        else:
            res = self.session.put(f"/remote/image/create_and_upload", data=payload)
                
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        
        return res[2].json()