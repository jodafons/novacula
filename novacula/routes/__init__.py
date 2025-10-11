

__all__ = ["raise_http_exception", "raise_authentication_failure"]


import requests
from fastapi import Request, HTTPException
from novacula.io import get_minio_service


def raise_http_exception(sc):
    if sc.isFailure():
        raise HTTPException(detail=sc.reason(), status_code=504)

def raise_authentication_failure( request : Request ):
    user = request.headers["user"]
    access_key = request.headers["access_key"]
    session = get_io_service().session(user, access_key)
    if not session.is_valid():
        raise HTTPException(detail="Not authorized user. the access_key or user is not found", status_code=409)


from . import dataset 
__all__.extend( dataset.__all__ )
from .dataset import *


