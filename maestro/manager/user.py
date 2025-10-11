
__all__ = ["UserManager"]

import traceback
from loguru         import logger
from typing         import Dict


from maestro    import StatusCode
from maestro    import schemas, random_id, random_token
from maestro.db import get_db_service, models



class UserManager:

    def __init__(
        self,
        envs : Dict[str,str]
    ):
        self.envs=envs

    def create( self, user : schemas.User ) -> StatusCode:

        db_service            = get_db_service()
        user_id               = random_id()
        token                 = random_token()

        new_user              = models.User()
        new_user.user_id      = user_id
        new_user.name         = user.name
        new_user.token        = token
        try:
            db_service.save_user(new_user)
            logger.info(f"Created a new user {user_id} into the database.")
            credential = schemas.Credential(user_id=user_id, token=token )
            return StatusCode.SUCCESS(credential)
        except:
            traceback.print_exc()
            reason=f"its not possible to create the user"
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)


    def list(self) -> StatusCode: 

        db_service  = get_db_service()
        try:
            with db_service() as session:
                users_db = (session.query(models.User).all())
                users = [schemas.User(user_id=user_db.user_id,
                                      name=user_db.name) for user_db in users_db]
                return StatusCode.SUCCESS(users)
        except:
            traceback.print_exc()
            reason=f"internal problem."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason)


    def get_user_from_token(self, token : str) -> StatusCode:

        db_service  = get_db_service()

        if not db_service.check_token_existence(token):
            reason = f"user associated to this token does not exist"
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason) 
    

        try:
            with db_service() as session:
                user_db = session.query(models.User).filter_by(token=token).one()
                user = schemas.User(user_id=user_db.user_id,
                                    name=user_db.name) 
                return StatusCode.SUCCESS(user)
        except:
            traceback.print_exc()
            reason=f"internal problem."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason) 


    def generate( self, user_id : str ) -> StatusCode:
        db_service = get_db_service()
        if not db_service.check_user_existence(user_id):
            reason = f"user {user_id} does not exist into the database."
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason) 

        token = random_token()
        db_service.user(user_id=user_id).update_token(token)
        credential = schemas.Credential(user_id=user_id, token=token)
        return StatusCode.SUCCESS(credential)


    def check_existence(self, token : str) -> StatusCode:

        db_service  = get_db_service()

        if not db_service.check_token_existence(token):
            reason = f"user associated to this token does not exist"
            logger.error(reason)
            return StatusCode.FAILURE(reason=reason) 
    
        user_id = db_service.fetch_user_from_token(token)
      
        return StatusCode.SUCCESS()