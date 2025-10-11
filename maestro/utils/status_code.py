__all__ = ["StatusCode"]

from copy import copy
 
class StatusObj(object):

  _status = 1

  def __init__(self, sc):
    self._status = sc
    self._value  = True
    self._reason = ""

  def isFailure(self):
    if self._status < 1:
      return True
    else:
      return False
    
  def result(self, key : str=None):
     return self._value if not key else self._value[key]
  
  def reason(self):
     return self._reason
    
  def __call__(self, value=True, reason : str=""):
     
    obj = copy(self)
    obj._value = value
    obj._reason = reason
    return obj

  def __eq__(self, a, b):
    if a.status == b.status:
      return True
    else:
      return False

  def __ne__(self, a, b):
    if a.status != b.status:
      return True
    else:
      return False

  @property
  def status(self):
    return self._status



# status code enumeration
class StatusCode(object):
  """
    The status code of something
  """
  SUCCESS = StatusObj(1)
  FAILURE = StatusObj(0)
  FATAL   = StatusObj(-1)