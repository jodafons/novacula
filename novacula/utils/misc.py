__all__ = [
    "random_id", 
    "random_token", 
    "md5checksum", 
    "symlink",
]


import os
import errno
import uuid
import hashlib


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
         