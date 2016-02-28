import copy, gc

class Subset(object):
  def __init__(self, arg):
    self.type = arg

  def __call__(self, cl):
    self.cl = cl
    if len(cl.mro()) < 1 and type not in set(cl.mro()[1:]):
      raise TypeError("Subset type must derive from type" + str(self.type))
    class _Subset(self.cl):
      __dependent_type__ = True
      def __init__(s, *args, **kwargs):
        universe = kwargs["universe"] if "universe" in kwargs else None
        if not universe:
          universe = s.__getfromgc__()
        s._original = universe
        s._cl = cl
        s._copyrelation = {}
        s._universe = []
        s._items = []
        

      def All(s):
        return s._items

      @staticmethod
      def __invariant__(*args, **kwargs):
        # Tricky line. Self is top level class. Not _Permutation!
        return self.cl.__invariant__(*args, **kwargs)
        
      def __getfromgc__(s):
        typemap = set()
        for item in gc.get_objects():
          if type(item) is self.type:
            typemap.add(item)
        return list(typemap)

      def __enter__(s, *args):
        s.CreateSnapShot()
        return s

      def __exit__(s, *args):
        return s.Merge()

      def _queryparams(s):
        return (s._universe,)

      def Merge(s):
        try:
          for item in s._copyrelation:
            item.__dict__ = s._copyrelation[item].__dict__
        except TypeError as e:
          raise TypeError("Immutable collections cannot be merged")

      def CreateSnapShot(s):
        for item in s._original:
          new_item = copy.deepcopy(item)
          s._copyrelation[item] = new_item
          s._universe.append(new_item)
        s._items = s._cl.__query__(*s._queryparams())
        for item in s._items:
          item.__class__ = self.cl


    return _Subset