
class parameterize(object):
  def __init__(self, *types):
    self._types = types

  def __call__(self, pcc_class):
    # parameterize should be on pcc classes only
    if len(pcc_class.mro()) < 2:
      raise TypeError("Parameter type must derive from some type")
    class _Parameterize(pcc_class):
      __parameter_types__ = self._types
      __dependent_type__ = True
      __PCC_BASE_TYPE__ = False
      __start_tracking__ = False
      
      def __init__(s, *args, **kwargs):
        super(_Parameterize, s).__init__(*args, **kwargs)
        if "params" not in kwargs:
          raise TypeError("Parameter needed to initialize Parameterized type")
        try:
          if not pcc_class.__dependent_type__:
            raise TypeError("Can only parameterize other dependent types")
        except AttributeError as e:
          raise TypeError("Can only parameterize other dependent types")
        s.params = kwargs["params"]
      
      
      def _queryparams(s):
        return tuple(list(super(_Parameterize, s)._queryparams()) + list(s.params))

    return _Parameterize