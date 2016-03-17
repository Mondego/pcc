import copy
import gc

class Join(object):
  def __init__(self, *args):
    self.types = args

  def __call__(self, cl):
    self.cl = cl
    
    class _Container(object):
      def __init__(s):
        s.__pcc_touched = set()

      def __setattr__(s, arg, value):
        if arg != "_Container__pcc_touched":
          s.__pcc_touched.add(arg)
        return super(_Container, s).__setattr__(arg, value)
      pass

    class _Join(self.cl):
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
        s.CreateSnapShot()
        return s

      def __exit__(s, *args):
        return s.Merge()

      def Merge(s):
        try:
          for item in s._copyrelation:
            item.__dict__ = s._copyrelation[item].__dict__
        except TypeError as e:
          raise TypeError("Immutable collections cannot be merged")

      def _queryparams(s):
        return s._universe

      def CreateSnapShot(s):
        for collection in s._original:
          new_collect = []
          for item in collection:
            new_item = _Container()
            new_item.__dict__.update(copy.deepcopy(item.__dict__))
            s._copyrelation[item] = new_item
            new_collect.append(new_item)
          s._universe.append(new_collect)
        s._items = s._cl.__query__(*s._queryparams())

    return _Join