
__all__ = ["DatasetAPIClient"]

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






class DatasetAPIClient:


    def __init__(self, session ):
        self.session = session
        

    def check_existence( 
        self, 
        name : str,
    ) -> bool:
        payload = { 'params_str' : schemas.json_encode({'name':name}) }
        res = self.session.put(f"/remote/dataset/options/exist", data=payload)
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        else:
            return res[2].json()


    def identity( 
        self, 
        name : str,
    ) -> str:
        payload = {"name":name}
        payload = {'params_str':schemas.json_encode(payload)}
        res = self.session.put(f"/remote/dataset/options/identity", data=payload)
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        else:
            return res[2].json()
       

    def describe( 
        self, 
        name : str,
    ) -> schemas.Dataset:
        
        payload = {'name' : name}
        payload = {'params_str':schemas.json_encode(payload)}
        res = self.session.put(f"/remote/dataset/options/describe", data=payload)
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        else:
            return schemas.Dataset(**res[2].json())


    def list( 
        self, 
        match_with : str="*",
    ) -> schemas.Dataset:
        
        payload = {'match_with' : match_with}
        payload = {'params_str':schemas.json_encode(payload)}
        res = self.session.put(f"/remote/dataset/options/list", data=payload)
        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        else:
            return [schemas.Dataset(**d) for d in res[2].json()]


    def create(
        self, 
        name        : str, 
        description : str,
    ):

        payload = {
            'name'         : name,
            'description'  : description,
        }
        payload = {"params_str": schemas.json_encode(payload)}
        res = self.session.put(f"/remote/dataset/options/create", data=payload)

        if res[0] != 200:
            detail = res[2].json()["detail"]
            raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        else:
            dataset_id = res[2].json()
            logger.debug(f"Received dataset_id: {dataset_id}")
            return dataset_id

        
    def upload( 
        self, 
        name       : str,
        files      : Union[List[str], str],
        as_link    : bool=False
    ) -> str:

        
        if type(files) == str and os.path.exists(files):
            files = expand_folders(files)
        else:
            raise RuntimeError(f"the path {files} does not exist.")

        for filepath in tqdm( files , desc = "uploading file...",ncols=100 ):
       
        
            filename    = filepath.split('/')[-1]
            file_md5    = md5checksum(filepath)

            payload = {'name':name, 'filename':filename}
            payload = {"params_str":schemas.json_encode(payload)}
            res = self.session.put(f"/remote/dataset/options/exist", data=payload)
            if res[0]!=200:
                detail = res[2].json()["detail"]
                raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
            
            exist = res[2].json()
            if exist:
                continue
            
            payload = {'expected_file_md5':file_md5, 'filename':filename, 'name':name, 'filepath':filepath}
            if not as_link:
                zipfolder   = tempfile.mkdtemp()
                zipfilename = f"{zipfolder}/file.zip"
            
                with zipfile.ZipFile(f"{zipfilename}", "w") as zip:
                    zip.write(filepath, filename, compress_type=zipfile.ZIP_DEFLATED)

                with open(zipfilename, 'rb') as f:
                    res = self.session.put(f"/remote/dataset/upload", data=payload, files={"file":f})
            else:
                res = self.session.put(f"/remote/dataset/upload", data=payload)

            if res[0] != 200:
                detail = res[2].json()["detail"]
                raise RuntimeError(f"Received {res[0]} as status code. detail: {detail}")
        
        return True


    def download( 
        self, 
        name          : str, 
        targetfolder  : str = None,
        as_link       : bool=False
    ):


        dataset = self.describe(name)
        if not dataset:
            raise RuntimeError(f"dataset with name {name} does not exist.")

        if not targetfolder:
            targetfolder = f"{os.getcwd()}/{name}"

        os.makedirs(targetfolder,exist_ok=True)

        for file_info in tqdm( dataset.files , desc="downloading files...", ncols=100):

            if not as_link:

                filename    = file_info["filename"]
                expected_file_md5 = file_info['md5']
                zipfolder   = tempfile.mkdtemp()
                zipfilename = f"{zipfolder}/file.zip"

                payload = {"filename":filename, "name":name}
                url = urljoin(self.session.host, f"/remote/dataset/download")

                with self.session().put(url, data=payload, stream=True, headers=self.session.headers) as r:
                    if r.status_code!=200:
                        detail = r.json()["detail"]
                        raise RuntimeError(f"Received {r.status_code} as status code. detail: {detail}")
                    with open(zipfilename, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1014):
                            f.write(chunk)

                unpackfolder = tempfile.mkdtemp()
                shutil.unpack_archive(zipfilename, extract_dir=unpackfolder, format='zip')
                os.remove(zipfilename)

                filepath = f"{unpackfolder}/{filename}"
                file_md5 = md5checksum( filepath )

                if file_md5 != expected_file_md5:
                    raise RuntimeError(f"file with name {filename} is corrupted. try again.")

                shutil.move(filepath, f"{targetfolder}/{filename}")
            else:
                targetfile = f"{targetfolder}/{file_info['filename']}"
                os.symlink( file_info['filepath'], targetfile )

        return True

      