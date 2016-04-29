from set import pcc_set, PCCMeta
class projection(object):
  def __init__(self, of_class):
    # Class that it is going to be a projection of.
    self.type = of_class

  def __call__(self, actual_class):
    # actual_class the class that is being passed from application.
    
    # The pcc projection class being cooked right here. 
    class _Projection(actual_class):
      __metaclass__ = PCCMeta(actual_class)
      __dependent_type__ = True
      __ENTANGLED_TYPES__ = [self.type]
      __PCC_BASE_TYPE__ = False
      __pcc_bases__ = set([self.type]).union(
          actual_class.__pcc_bases__ 
          if hasattr(actual_class, "__pcc_bases__") else 
          set())
      __start_tracking__ = False
      __pcc_projection__ = True
      
      __dimensions__ = actual_class.__dimensions__ if hasattr(actual_class, "__dimensions__") else set()

      
      def __init__(s, *args, **kwargs):
        s._dataframe_universe = kwargs["universe"] if "universe" in kwargs else None
        s._nomerge = kwargs["nomerge"] if "nomerge" in kwargs else False
        s._items = []
        
      def All(s):
        return s._items

      @staticmethod
      def Class():
        # Not sure if this should be exposed, 
        # as then people can create objects fromt this
        # useful for inheriting from class directly though.
        return actual_class

      def __create__(s, base_set_obj):
        if hasattr(base_set_obj, "__dimensions__"):
          class _dummy(object):
            pass
          new_obj = _dummy()
          new_obj.__class__ = actual_class
          fields = new_obj.__dimensions__
          for field in fields:
            setattr(new_obj, field._name, getattr(base_set_obj, field._name))
        else:
          new_obj = s._dataframe_universe._change_type(base_set_obj, actual_class)
          fields = new_obj.__dict__.keys()
          for field in fields:
            if field not in actual_class.FIELDS and "pcc_tracked_changes" not in field:
              delattr(new_obj, field)
          new_obj._pcc_tracked_changes.clear()
        return new_obj
        
      def __enter__(s, *args):
        s.create_snapshot()
        return s

      def __exit__(s, *args):
        return s.merge() if not s._nomerge else None

      def merge(s):
        s._dataframe_universe.merge()

      def create_snapshot(s):
        s._items = [s.__create__(item) 
                    for item in s._dataframe_universe.getcopies()]
       
    return _Projection