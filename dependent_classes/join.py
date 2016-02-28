import copy
import gc

class Join(object):
  def __init__(self, *args):
    self.types = args

  def __call__(self, cl):
    self.cl = cl
    class _Join(self.cl):
      def __init__(s, *args, **kwargs):
        universe = kwargs["universe"] if "universe" in kwargs else None
        if not universe:
          universe = s.__getfromgc__()
        s._original = universe
        s.cl = cl
        s.copyrelation = {}
        s.universe = []
        for collection in universe:
          new_collect = []
          for item in collection:
            new_item = copy.deepcopy(item)
            s.copyrelation[item] = new_item
            new_collect.append(new_item)
          s.universe.append(new_collect)
        s.items = s.cl.__query__(*s.universe)

      def All(s):
        return s.items

      @staticmethod
      def Create(*args, **kwargs):
        # Tricky line. Self is top level class. Not _Permutation!
        return self.cl(*args, **kwargs) if self.cl.__invariant__(*args, **kwargs) else None

      @staticmethod
      def __invariant__(*args, **kwargs):
        # Tricky line. Self is top level class. Not _Permutation!
        return self.cl.__invariant__(*args, **kwargs)
        
      def __getfromgc__(s):
        typemap = {}
        for item in gc.get_objects():
          if type(item) in self.types:
            typemap.setdefault(item.__class__, set()).add(item)
        return [list(typemap[t]) for t in self.types]

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


    return _Join