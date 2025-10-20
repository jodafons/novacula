__all__ = [
    "setup_logs",
    "get_argparser_formatter",
    "random_id", 
    "random_token", 
    "md5checksum", 
    "symlink",
]


import os
import errno
import uuid
import hashlib
import sys, argparse

from loguru         import logger
from rich_argparse  import RichHelpFormatter





def get_argparser_formatter( standard : bool=False):
    if not standard:
        RichHelpFormatter.styles["argparse.args"]     = "green"
        RichHelpFormatter.styles["argparse.prog"]     = "bold grey50"
        RichHelpFormatter.styles["argparse.groups"]   = "bold green"
        RichHelpFormatter.styles["argparse.help"]     = "grey50"
        RichHelpFormatter.styles["argparse.metavar"]  = "blue"
        return RichHelpFormatter
    else:
        return argparse.HelpFormatter

def setup_logs( name , level, save : bool=False, color="cyan", prefix=""):
    """Setup and configure the logger"""

    logger.configure(extra={"name" : name})
    logger.remove()  # Remove any old handler
    #format="<green>{time:DD-MMM-YYYY HH:mm:ss}</green> | <level>{level:^12}</level> | <cyan>{extra[slurms_name]:<30}</cyan> | <blue>{message}</blue>"
    format=prefix+"<"+color+">{extra[name]:^25}</"+color+"> | <green>{time:DD-MMM-YYYY HH:mm:ss}</green> | <level>{level:^12}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | <blue>{message}</blue>"
    logger.add(
        sys.stdout,
        colorize=True,
        backtrace=True,
        diagnose=True,
        level=level,
        format=format,
    )
    if save:
        output_file = name.replace(':','_').replace('-','_') + '.log'
        logger.add(output_file, 
                   rotation="30 minutes", 
                   retention=3, 
                   format=format, 
                   level=level, 
                   colorize=False)   




def random_id():
    new_uuid = uuid.uuid4()
    return str(new_uuid)[-12:]

def random_token():
    new_uuid = str(uuid.uuid4()) + str(uuid.uuid4())
    return new_uuid.replace('-','')

def md5checksum(fname):
    md5 = hashlib.md5()
    f = open(fname, "rb")
    while chunk := f.read(4096):
        md5.update(chunk)
    return md5.hexdigest()

def symlink(target, linkpath):
    try:
        os.symlink(target, linkpath)
        return linkpath
    except OSError as e:
        if e.errno == errno.EEXIST:
            os.remove(linkpath)
            os.symlink(target, linkpath)
            return linkpath
        else:
            raise e
         
from . import popen 
__all__.extend( popen.__all__ )
from .popen import *

from . import db
__all__.extend( db.__all__ )
from .db import *

from . import task
__all__.extend( task.__all__ )
from .task import *