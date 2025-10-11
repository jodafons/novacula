#!/usr/bin/env python

import os
import sys
import time
import uvicorn
import argparse
import traceback

from time              import sleep
from loguru            import logger
from fastapi           import FastAPI
from fastapi.responses import PlainTextResponse
from novacula          import setup_logs
from novacula          import get_manager_service, create_user
#from novacula          import get_scheduler_service
#from novacula          import get_backend_service
from novacula          import routes
#from novacula.db       import get_db_service, recreate_db
#from novacula.io       import get_io_service
from novacula          import get_argparser_formatter


def run( args ):
    
    #
    # startup
    #
    setup_logs(args.name,args.message_level)

    #
    # get volume
    #
    args.volume=os.path.abspath(args.volume)
    os.makedirs(args.volume,exist_ok=True)
    logger.info(f"volume path: {args.volume}")
    #get_io_service(args.volume)

    envs = {
        "MINIO_HOST":args.minio_host,
    }

    #
    # get database
    #
    db_booted = False
    #while not db_booted:
    #    try:
    #        logger.info(f"db_string: {args.db_string}")       
    #        get_db_service(args.db_string)
    #        db_booted = True
    #    except:
    #        time.sleep(2)
    #        traceback.print_exc()
    #        logger.warning("waiting for the database...")

    #if args.db_recreate:
    #    logger.info("recreating database...")
    #    recreate_db()
 
    #
    # create app
    #
    app = FastAPI(title=__name__)
    app.include_router(routes.dataset_app)
    app.include_router(routes.user_app)
   


    @app.on_event("startup")
    def startup_event():
        get_io_service(host=args.minio_host)

        

    @app.on_event("shutdown")
    def shutdown_event():
        logger.info("shutdown event...")   
            

    @app.get("/status")
    async def get_status():
        return PlainTextResponse("OK", status_code=200)


    #app_level = "warning"
    app_level = 'info'
    uvicorn.run(app, port=args.port, log_level=app_level, host="0.0.0.0")
                




if __name__ == "__main__":


    formatter_class = get_argparser_formatter()
    parser    = argparse.ArgumentParser(formatter_class=formatter_class)

    parser.add_argument('--port', action='store', dest='port', required = False,  type=int,
                        default=7000,
                        help = "the port endpoint") 
    
    parser.add_argument('-n','--name', action='store', dest='name', required = False, 
                        default="app",
                        help = "the server name.")
    
    parser.add_argument('-l','--message-level', action='store', dest='message_level', required = False, 
                        default="INFO",
                        help = "the message level. default can be passed by ORCH_MESSAGE_LEVEL environ.")
    
    parser.add_argument('-v','--volume', action='store', dest='volume', required = False,
                        default=f"{os.getcwd()}/data", 
                        help = "the volume used to store everything. ") 
    
    parser.add_argument('--database-string', action='store', dest='db_string', type=str,
                                 required=False, default=os.environ.get("DB_STRING",""),
                                 help = "the database url used to store all tasks and jobs. default can be passed by DB_STRING environ.")
    
    parser.add_argument('--database-recreate', action='store_true', dest='db_recreate', 
                                 required=False , 
                                 help = "recreate the postgres SQL database.")     

    parser.add_argument('--minio-host', action='store', dest='minio_host', type=str,
                        required=False, default="localhost:9000",
                        help="the MinIO host URL.")

    parser.add_argument('--airflow-host', action='store', dest='airflow_host', type=str,
                        required=False, default="http://localhost:8080",
                        help="the Airflow host URL.")

    if len(sys.argv)==1:
        print(parser.print_help())
        sys.exit(1)

    args = parser.parse_args()

    run(args)

