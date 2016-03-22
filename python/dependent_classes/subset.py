import copy, gc, types


      

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
        s._startTrack = False
        universe = kwargs["universe"] if "universe" in kwargs else None
        if not universe:
          universe = s.__getfromgc__()
        s._original = universe
        s._cl = cl
        s._copyrelation = {}
        s._universe = []
        s._items = []
        s._objectchanges = []
        
      def __setattr__(s, arg, value):
        if "_startTrack" not in arg and s._startTrack and "_objectchanges" not in arg:
          s._objectchanges.append(arg)
        return super(_Subset, s).__setattr__(arg, value)
        
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
            for arg in s._copyrelation[item]._objectchanges:
              item.__dict__[arg] = s._copyrelation[item].__dict__[arg]
        except TypeError as e:
          raise TypeError("Immutable collections cannot be merged" + e.message)

      def CreateSnapShot(s):
        for item in s._original:
          new_item = copy.deepcopy(item)
          new_item.__class__ = s.__class__
          new_item._startTrack = True
          new_item._objectchanges = []
          #new_item.__setattr__ = types.MethodType(__setattr__, new_item, self.cl)
          s._copyrelation[item] = new_item
          s._universe.append(new_item)
        s._items = s._cl.__query__(*s._queryparams())
          


    return _Subset