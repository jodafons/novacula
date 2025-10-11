
__all__ = ["DatasetManager"]

import os
import shutil
import zipfile
import tempfile
import traceback

from loguru     import logger
from typing     import Dict
from fastapi    import File, UploadFile
from maestro    import StatusCode, symlink
from maestro    import schemas, random_id, md5checksum
from maestro.db import models, get_db_service, DatasetType, FileType
from maestro.io import get_io_service





class DatasetManager:

    def __init__(
        self, 
        user_id : str,
        envs    : Dict[str,str]={}
    ):
        db_service=get_db_service()
        self.user_id=user_id
        self.envs=envs
        self.user_name= db_service.user(user_id).fetch_name()


    def create(
        self, 
        dataset        : schemas.Dataset, 
    )-> StatusCode:

        name       = dataset.name
        db_service = get_db_service()

        if not name.startswith(f'user.{self.user_name}.'):
            reason=f"the name dataset must follow the name rule: 'user.{self.user_name}.DATASET_NAME'"
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)

        if db_service.check_dataset_existence_by_name(name):
            reason=f"dataset with name {name} exist into the database."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)

        dataset_db             = models.Dataset()
        new_id                 = random_id()
        dataset_db.dataset_id  = new_id
        dataset_db.user_id     = self.user_id
        dataset_db.name        = dataset.name
        dataset_db.description = dataset.description
        dataset_db.data_type   = DatasetType.FILES
        db_service.save_dataset(dataset_db)

        io_service = get_io_service()
        io_service.dataset(new_id).mkdir()

        logger.info(f"saving dataset {new_id} into the database and storage...")
        return StatusCode.SUCCESS(new_id)
    

    def upload(
        self, 
        name                : str,
        filename            : str,
        from_filepath       : str,
        expected_file_md5   : str,
        force_overwrite     : bool=False,
        file                : UploadFile=File(None)     
    )-> StatusCode:

        logger.info("uploading dataset into the database...")

        db_service = get_db_service()

        if not db_service.check_dataset_existence_by_name(name):
            reason=f"dataset with name {name} does not exist into the database."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)

        dataset_id = db_service.fetch_dataset_from_name(name)

        io_service = get_io_service()
        
        if not force_overwrite and io_service.dataset(dataset_id).check_existence(filename):
            reason = f"its not possible to upload the file with name {filename}. duplicated filename into the storage."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)
        
        targetfolder  = io_service.dataset(dataset_id).basepath
        
        if file:
        
            zipfolder   = tempfile.mkdtemp()
            zipfilename = f"{zipfolder}/file.zip"
        
            try:
                with open(zipfilename, 'wb+') as f:
                    while contents := file.file.read(1024*1024):
                        f.write(contents)
            except:
                traceback.print_exc()
                reason = f"its not possible to open the sent file..."
            finally:
                file.file.close()

            if not os.path.exists(zipfilename):
                reason = f"its not possible to found the file in cache..."
                logger.error(reason)
                return StatusCode.FAILURE(reason=reason)

            unpackfolder  = tempfile.mkdtemp()

            logger.info(f"unpacking {zipfilename} file into {unpackfolder}...")
            shutil.unpack_archive(zipfilename, extract_dir=unpackfolder, format='zip')
            os.remove(zipfilename)

            filepath = f"{unpackfolder}/{filename}"
            file_md5 = md5checksum( filepath )

           
        else:
            logger.info("setting as link...")
            file_md5      = md5checksum( from_filepath )
            filepath      = from_filepath

        if file_md5 != expected_file_md5:
            reason = f"the md5 hash of the transfed file is different than the expected hash value."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)

        try:
            # preparing to add into the dataset db and io
            with db_service() as session:
                dataset_db          = session.query(models.Dataset).filter_by(dataset_id=dataset_id).one() 
                file_db             = models.File()
                file_db.file_id     = random_id()
                file_db.name        = filename
                file_db.file_md5    = file_md5
                file_db.file_type   = FileType.DATA if file else FileType.LINK
                logger.info(f"saving file {filepath} into db.")
                dataset_db.files.append(file_db)
                session.commit()
        except:
            reason=f"we found an error during the file writting into the storage and db."
            logger.error(reason)
            traceback.print_exc()
            return StatusCode.FAILURE(reason=reason)

        if file:
            shutil.move(filepath, f"{targetfolder}/{filename}")
        else:
            logger.info(f"creating link from {filepath} to {targetfolder}/{filename}...")
            symlink(filepath, f"{targetfolder}/{filename}")

        logger.info(f"saving file {filename} into the database and storage {targetfolder}...")
        return StatusCode.SUCCESS()
    
    
    

    def download(
        self, 
        name        : str, 
        filename    : str,
    ) -> StatusCode:
        
        logger.debug(f"downloading file {filename} from dataset with {name}...")

        db_service = get_db_service()
        io_service = get_io_service()
   
        if not db_service.check_dataset_existence_by_name(name):
            reason=f"dataset with name {name} does not exist into the database."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)

        dataset_id = db_service.fetch_dataset_from_name(name)

        if not io_service.dataset(dataset_id).check_existence(filename):
            reason=f"filename with name {filename} does not exist into the dataset {name}"
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)

        zipfolder   = tempfile.mkdtemp()
        zipfilename = f"{zipfolder}/file.zip"
        basepath    = io_service.dataset(dataset_id).basepath
        filepath    = f"{basepath}/{filename}"

        with zipfile.ZipFile(f"{zipfilename}", "w") as zip:
            zip.write(filepath, filename, compress_type=zipfile.ZIP_DEFLATED)
       
        return StatusCode.SUCCESS(zipfilename)


    def check_existence(
        self,
        name     : str,
        filename : str=None,
    ) -> StatusCode:

        db_service = get_db_service()
        io_service = get_io_service()
        if not db_service.check_dataset_existence_by_name(name):
            reason=f"dataset with name {name} does not exist into the db."
            logger.info(reason)
            return StatusCode.SUCCESS(False)

        dataset_id = db_service.fetch_dataset_from_name(name)
        if filename and not io_service.dataset(dataset_id).check_existence(filename):
            reason=f"filename with name {filename} does not exist into the dataset {name}"
            logger.info(reason)
            return StatusCode.SUCCESS(False)

        return StatusCode.SUCCESS()


    def describe(
        self,
        name        : str,
    ) -> StatusCode:
        
        db_service = get_db_service()
        io_service = get_io_service()

        if not db_service.check_dataset_existence_by_name(name):
            reason=f"dataset with name {name} does not exist into the database."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)
        
        dataset_id = db_service.fetch_dataset_from_name(name)

        dataset = schemas.Dataset()

        folder_path=io_service.dataset(dataset_id).basepath

        with db_service() as session:
            dataset_db           = session.query(models.Dataset).filter_by(dataset_id=dataset_id).one()
            dataset.user_id      = dataset_db.user_id
            dataset.dataset_id   = dataset_db.dataset_id
            dataset.description  = dataset_db.description
            dataset.name         = dataset_db.name
            dataset.data_type    = dataset_db.data_type.value
            files = []
            for file_db in dataset_db.files:
                filepath=f"{folder_path}/{file_db.name}"
                data = {'filename':file_db.name , 'md5': file_db.file_md5, "filepath":filepath}
                files.append(data)
            dataset.files = files
                
        return StatusCode.SUCCESS(dataset)


    def list(
        self,
        match_with="*"
    ) -> StatusCode:
        
        db_service = get_db_service()
        match_with = match_with.replace("*","%")
        names = []
        with db_service() as session:
            datasets_from_db = session.query(models.Dataset).filter(models.Dataset.name.like(match_with)).all()
            for dataset_db in datasets_from_db:
                if (dataset_db.visible):
                    names.append(dataset_db.name)
        datasets=[ self.describe(name).result() for name in names]     
        return StatusCode.SUCCESS(datasets)


    def identity(
        self,
        name     : str,
    ) -> StatusCode:

        db_service = get_db_service()
        
        if not db_service.check_dataset_existence_by_name(name):
            reason=f"dataset with name {name} does not exist into the db."
            logger.info(reason)
            return StatusCode.SUCCESS(False)

        dataset_id = db_service.fetch_dataset_from_name(name)
        
        
        return StatusCode.SUCCESS(dataset_id)