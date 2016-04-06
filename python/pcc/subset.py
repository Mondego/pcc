
class subset(object):
  def __init__(self, of_class):
    # Class that it is going to be a subset of.
    self.type = of_class

  def __call__(self, actual_class):
    # actual_class the class that is being passed from application.
    if len(actual_class.mro()) < 1 and self.type not in set(actual_class.mro()[1:]):
      raise TypeError("Subset type must derive from type" + str(self.type))

    # The pcc subset class being cooked right here. 
    class _Subset(object):
      __dependent_type__ = True
      
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

      @staticmethod
      def __invariant__(*args, **kwargs):
        return actual_class.__invariant__(*args, **kwargs)

      def __create__(s, base_set_obj):
        return s._dataframe_universe._change_type(base_set_obj, actual_class)
        
      def __enter__(s, *args):
        s.create_snapshot()
        return s

      def __exit__(s, *args):
        return s.merge() if not s._nomerge else None

      def _queryparams(s):
        return (s._dataframe_universe.getcopies(),)

      def merge(s):
        s._dataframe_universe.merge()

      def create_snapshot(s):
        s._items = [s.__create__(item) 
                    for item in actual_class.__query__(*s._queryparams())]
          
    return _Subset