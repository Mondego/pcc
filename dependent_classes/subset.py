import copy, gc

class Subset(object):
  def __init__(self, arg):
    self.type = arg

  def __call__(self, cl):
    self.cl = cl
    if cl.mro() < 1 and type not in set(cl.mro()[1:]):
      raise TypeError("Subset type must derive from type" + str(self.type))
    class _Subset(self.cl):
      def __init__(s, *args, **kwargs):
        universe = kwargs["universe"] if "universe" in kwargs else None
        if not universe:
          universe = s.__getfromgc__()
        s._original = universe
        s.cl = cl
        s.copyrelation = {}
        s.universe = []
        for item in universe:
          new_item = copy.deepcopy(item)
          s.copyrelation[item] = new_item
          s.universe.append(new_item)
        s.items = s.cl.__query__(s.universe)
        for item in s.items:
          item.__class__ = self.cl

      def All(s):
        return s.items

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
        return s

      def __exit__(s, *args):
        return s.Merge()

      def Merge(s):
        try:
          for item in s.copyrelation:
            item.__dict__ = s.copyrelation[item].__dict__
        except TypeError as e:
          raise TypeError("Immutable collections cannot be merged")


    return _Subset