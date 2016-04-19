from attributes import primarykey, dimension, spacetime_property

class PCCMeta(type):
  def __new__(cls, name, bases, dict):
    result = super(PCCMeta, cls).__new__(cls, name, bases, dict)
    result.__dimensions__ = set()
    result.__dimensionmap__ = {}
    result.__primarykey__ = None
    values = []
    for base in bases:
      values.extend([getattr(base, attr) 
                for attr in dir(base) 
                if isinstance(getattr(base, attr), spacetime_property)])
    for value in values:
      if hasattr(value, "_dimension"):
        result.__dimensions__.add(value)
        result.__dimensionmap__[value._name] = value
      if hasattr(value, "_primarykey") and getattr(value, "_primarykey") != None:
        result.__primarykey__ = value
    return result

def pcc_set(actual_class):
  class _set(actual_class):
    __metaclass__ = PCCMeta
    __PCC_BASE_TYPE__ = True
    __dependent_type__ = True
    __pcc_bases__ = set()
    __ENTANGLED_TYPES__ = []
    __start_tracking__ = False
    
    def __init__(self, *args, **kwargs):
      self._primarykey = None
      self.__start_tracking__ = False
      return super(_set, self).__init__(*args, **kwargs)
    
    @staticmethod
    def Class():
      return actual_class
  return _set
