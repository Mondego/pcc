from attributes import primarykey, dimension, spacetime_property

def PCCMeta(cooked_cls):
  class _PCCMeta(type):
    def __new__(cls, name, bases, dict):
      result = super(_PCCMeta, cls).__new__(cls, name, bases, dict)
      result.__dimensions__ = set()
      result.__dimensionmap__ = {}
      result.__dimensions_name__ = set()
      result.__primarykey__ = None
      cooked_cls.__dimensions__ = set()
      cooked_cls.__dimensions_name__ = set()
      cooked_cls.__dimensionmap__ = {}
      cooked_cls.__primarykey__ = None
      values = set()
      for attr in dir(cooked_cls):
        try:
          if isinstance(getattr(cooked_cls, attr), spacetime_property):
            values.add(getattr(cooked_cls, attr))
        except AttributeError:
          continue
      
      for base in bases:
        values.update(set([getattr(base, attr) 
                  for attr in dir(base) 
                  if isinstance(getattr(base, attr), spacetime_property)]))
      for value in values:
        if hasattr(value, "_dimension"):
          result.__dimensions__.add(value)
          result.__dimensionmap__[value._name] = value
          result.__dimensions_name__.add(value._name)
          cooked_cls.__dimensions__.add(value)
          cooked_cls.__dimensionmap__[value._name] = value
          cooked_cls.__dimensions_name__.add(value._name)
        if hasattr(value, "_primarykey") and getattr(value, "_primarykey") != None:
          result.__primarykey__ = value
          cooked_cls.__primarykey__ = value
      return result
  return _PCCMeta

def pcc_set(actual_class):
  class _set(actual_class):
    __realname__ = actual_class.__name__
    __metaclass__ = PCCMeta(actual_class)
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
