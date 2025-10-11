__all__ = ["user_app"]

from fastapi            import APIRouter, HTTPException, Form
from fastapi.responses  import RedirectResponse
from maestro            import schemas, get_manager_service
from maestro.routes     import remote_app, raise_http_exception

user_app      = APIRouter()

@user_app.put("/user/{option}", status_code=200, tags=['user'])
async def user_options(
    option     : str,
    params_str : str=Form()
 ):
    params  = schemas.json_decode(params_str)
    manager = get_manager_service()

    if option=="create":
        user = params['user']
        sc = manager.user().create(user)

    elif option=="list":
        sc = manager.user().list()

    elif option=="token":
        token = params['token']
        sc = manager.user().get_user_from_token(token)

    elif option=="generate":
        user_id = params['user_id']
        sc = manager.user().generate(user_id)

    elif option=="exist":
        token = params['token']
        sc = manager.user().check_existence(token)
    else:
        raise HTTPException(detail=f"option {option} does not exist into the database service.")

    raise_http_exception(sc)
    return sc.result()


#
# remote
#

@remote_app.put("/remote/user/{option}", status_code=200, tags=["remote"])
async def user_options( option : str ):
    return RedirectResponse(f"/user/{option}")