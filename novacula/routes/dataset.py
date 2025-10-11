__all__ = ["dataset_app"]

import re 

from loguru import logger
from typing import Optional
from fastapi import APIRouter, File, UploadFile, Request, Form, HTTPException
from fastapi.responses import RedirectResponse, StreamingResponse 
from novacula import io_service 
from novacula import raise_authentication_failure, raise_http_exception, StatusCode


def create_dataset(
    self, 
    request     : Request, 
    name        : str,
    description : str,
)-> StatusCode:

    name       = dataset.name
    io_service = get_io_service()

    if not name.startswith(f'user.{self.user_name}.'):
        reason=f"the name dataset must follow the name rule: 'user.{self.user_name}.DATASET_NAME'"
        logger.error(reason)
        return StatusCode.FAILURE(reason=reason)
    
    user       = request.headers["user"]
    access_key = request.headers["access_key"]
    io_session = io_service.session(user, access_key)

    if io_session.check_dataset_existence_by_name(name):
        reason=f"dataset with name {name} exist into the database."
        logger.error(reason)
        return StatusCode.FAILURE(reason=reason)

    new_id = io_session.create( name, self.user_name, description=description )
    logger.info(f"saving dataset {new_id} into the database and storage...")
    return StatusCode.SUCCESS(new_id)
    

def list_datasets(
    name="*"
) -> StatusCode:

    io_service = get_io_service()
    user       = request.headers["user"]
    access_key = request.headers["access_key"]
    io_session = io_service.session(user, access_key)
    datasets=[ self.describe(name).result() for name in names]     
    return StatusCode.SUCCESS(datasets)


#
# remote
#


dataset_app = APIRouter()

@dataset_app.put("/dataset/options/{option}", status_code=200, tags=['remote'])
async def options( 
    option     : str,
    request    : Request,
    params_str : str=Form()
): 
    raise_authentication_failure(request)
    params  = schemas.json_decode(params_str)

    if option=="create":
        description  = params['description']
        name = params['name']
        sc = create_dataset( request, name, description )

    elif option=="list":
        match_with = params.get('match_with', "*")
        sc = list_datasets( match_with )
    else:
        raise HTTPException(detail=f"option {option} does not exist into the database service.")

    raise_http_exception(sc)
    return sc.result()