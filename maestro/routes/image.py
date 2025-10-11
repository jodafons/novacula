__all__ = ["image_app"]

import io

from typing             import Optional, List
from fastapi            import APIRouter, File, UploadFile, Request, Form, HTTPException
from fastapi.responses  import RedirectResponse, StreamingResponse 
from maestro.db         import get_db_service, DatasetType
from maestro            import schemas, get_manager_service
from maestro.routes     import remote_app, raise_authentication_failure, raise_http_exception, fetch_user_from_request



image_app = APIRouter()


@image_app.put("/image/options/{user_id}/{option}" , status_code=200, tags=['image'])
async def options( 
    user_id        : str,
    option         : str,
    params_str     : str=Form(),
):
    manager = get_manager_service()
    params  = schemas.json_decode(params_str)
    name    = params['name'] if 'name' in params else ""

    if option=="describe":
        sc   = manager.dataset(user_id).describe(name)

    elif option=="exist":
        sc = manager.dataset(user_id).check_existence(name)

    elif option=="list":
        match_with = params["match_with"]
        sc = manager.dataset(user_id).list(match_with)
        
    elif option=="identity":
        sc = manager.dataset(user_id).identity(name)

    else:
        raise HTTPException(detail=f"option {option} does not exist into the database service.")

    raise_http_exception(sc)
    return sc.result()


@image_app.put("/image/create_and_upload/{user_id}" , status_code=200, tags=['image'])
async  def create_and_upload( 
    user_id                : str,
    name                   : str=Form(),
    filename               : str=Form(),
    filepath               : str=Form(),    
    expected_file_md5      : str=Form(),
    description            : str=Form(""),
    file                   : Optional[UploadFile]=File(None),
) -> str:
    manager = get_manager_service()
    sc = manager.image(user_id).create_and_upload( name, 
                                                   filename, 
                                                   filepath,
                                                   expected_file_md5, 
                                                   description, file=file )
    raise_http_exception(sc)
    return sc.result()


#
# remote
#

@remote_app.put("/remote/image/options/{option}", status_code=200, tags=['remote'])
async def options( 
    option  : str,
    request : Request
): 
    db_service = get_db_service()    
    raise_authentication_failure(request)
    user_id = fetch_user_from_request(request)
    return RedirectResponse(f"/image/options/{user_id}/{option}")


@remote_app.put("/remote/image/create_and_upload", status_code=200, tags=['remote'])
async def create_and_upload( 
    request : Request
) -> bool: 
    raise_authentication_failure(request)
    db_service = get_db_service()
    user_id = fetch_user_from_request(request)
    return RedirectResponse(f"/image/create_and_upload/{user_id}")