
__all__ = ["ImageManager"]

import os
import shutil
import zipfile
import tempfile
import traceback

from loguru     import logger
from typing     import Dict
from fastapi    import File, UploadFile
from .dataset   import DatasetManager
from maestro    import StatusCode, symlink
from maestro    import schemas, random_id, md5checksum
from maestro.db import models, get_db_service, DatasetType, FileType
from maestro.io import get_io_service





class ImageManager(DatasetManager):

    def __init__(
        self, 
        user_id : str,
        envs    : Dict[str,str]
    ):
        DatasetManager.__init__(self,user_id, envs)
        

    def create_and_upload(
        self, 
        name                : str,
        filename            : str,
        from_filepath       : str,
        expected_file_md5   : str,
        description         : str="",
        file                : UploadFile=File(None)   
    )-> StatusCode:

        db_service = get_db_service()
        io_service = get_io_service()

        dataset_id = random_id()

        if not name.startswith(f'{self.user_name}/'):
            reason=f"the image name must follow the name rule: '{self.user_name}/IMAGE_NAME'"
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)

        if db_service.check_dataset_existence_by_name(name):
            reason=f"dataset with name {name} exist into the database."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)


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


        dataset_db             = models.Dataset()
        dataset_db.user_id     = self.user_id
        dataset_db.dataset_id  = dataset_id
        dataset_db.name        = name
        dataset_db.description = description
        dataset_db.data_type   = DatasetType.IMAGE
        db_service.save_dataset(dataset_db)
        
        
        io_service.image(dataset_id).mkdir()
        targetfolder  = io_service.image(dataset_id).basepath

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
        return StatusCode.SUCCESS(dataset_id)