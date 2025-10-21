__all__ = [ "get_context"]



class Context:
    def __init__(self):
        self.tasks = {}
        self.datasets = {}
        self.images = {}
    def clear(self):
        self.tasks = {}
        self.datasets = {}
        self.images = {}

__context__ = Context()

def get_context(self):
    global __context__
    return __context__
     

from . import task 
__all__.extend( task.__all__ )
from .task import *

from . import dataset
__all__.extend( dataset.__all__ )
from .dataset import *

from . import image
__all__.extend( image.__all__ )
from .image import *
