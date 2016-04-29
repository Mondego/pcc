class dataframe(object):
  def __init__(self, container, retain_types = False, additional = []):
    self._retain_types = retain_types
    self._real_duplicate_map = {}
    self._duplicate_real_map = {}
    self._recurse_map = {
        "primitive": lambda x: x,
        "collection": self._track_collection,
        "object": self._track_object,
        "dictionary": self._track_dictionary
      }
    self._copy_universe = self._track_collection(container)
    self._track_collection(additional)

  def _change_type(self, obj, totype):
    new_obj = self._get_empty_tracking_container(totype)()
    new_obj.__dict__.update(obj.__dict__)
    original = self._duplicate_real_map[obj]
    self._duplicate_real_map[new_obj] = original
    self._real_duplicate_map[original] = new_obj
    return new_obj
    
  def _get_empty_tracking_container(self, oftype):
    class _container(oftype):
      def __init__(s):
        s._pcc_tracked_changes = set()
      def __setattr__(s, arg, value):
        if ("_pcc_tracked_changes" not in arg 
          and "__dict__" not in arg
          and "__class__" not in arg):
          s._pcc_tracked_changes.add(arg)
        return super(_container, s).__setattr__(arg, value)

    return _container

  def _track_collection(self, container):
    return container.__class__([self._recurse_map[dataframe._get_type(obj)](obj) for obj in container])

  def _track_object(self, obj):
    if obj not in self._real_duplicate_map:
      
      container = self._get_empty_tracking_container(
          type(obj) if self._retain_types else object)

      newobj = container()
      self._real_duplicate_map[obj] = newobj
      self._duplicate_real_map[newobj] = obj
      newobj.__dict__.update(self._track_dictionary(obj.__dict__))
      return newobj
    else:
      return self._real_duplicate_map[obj]

  def _track_dictionary(self, dictionary):
    return dict(
        [(self._recurse_map[dataframe._get_type(k)](k), 
          self._recurse_map[dataframe._get_type(v)](v)) 
         for k,v in dictionary.items()]
      )

  @staticmethod
  def _get_type(obj):
    # both iteratable/dictionary + object type is messed up. Won't work.
    try:
      if dict in type(obj).mro():
        return "dictionary"
      if hasattr(obj, "__iter__"):
        return "collection"
      if len(set([float, int, str, unicode, type(None)]).intersection(set(type(obj).mro()))) > 0:
        return "primitive"
      if hasattr(obj, "__dict__"):
        return "object"
    except TypeError, e:
      return "unknown"
    return "unknown"

      
  def getcopies(self):
    return self._copy_universe

  def merge(self):
    for original, copy in self._real_duplicate_map.items():
      for change in copy._pcc_tracked_changes:
        original.__dict__[change] = copy.__dict__[change]
      copy._pcc_tracked_changes.clear()
