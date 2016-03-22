import copy, gc

def Parameterize(cl):
  if len(cl.mro()) < 2:
    raise TypeError("Parameter type must derive from some type")
  class _Parameterize(cl):
    __dependent_type__ = True
    def __init__(s, *args, **kwargs):
      super(_Parameterize, s).__init__(*args, **kwargs)
      if "params" not in kwargs:
        raise TypeError("Parameter needed to initialize Parameterized type")
      try:
        if not cl.__dependent_type__:
          raise TypeError("Can only parameterize other dependent types")
      except AttributeError as e:
        raise TypeError("Can only parameterize other dependent types")
      s.params = kwargs["params"]
      
      
    def _queryparams(s):
      return tuple(list(super(_Parameterize, s)._queryparams()) + list(s.params))

  return _Parameterize