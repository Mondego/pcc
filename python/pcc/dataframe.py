from pcc.attributes import spacetime_property
class dataframe(object):
  def __init__(self, lock = None):
    self.__lock = lock
    self.__object_store = {}
    self.__real_duplicate_map = {}
    self.__duplicate_real_map = {}
    self.__real_duplicate_same_type = {}
    self.__recurse_map = {
        "primitive": lambda x: x,
        "collection": self.__track_collection,
        "object": self.__track_object,
        "dictionary": self.__track_dictionary
      }
    return

  def __enter__(self):
    return self.enter()

  def enter(self):
    if self.__lock:
      try:
        self.__lock.acquire()
      except AttributeError:
        raise AttributeError("Dataframe did not get a lock object as parameter")
    return self

  def __exit__(self, *args):
    self.exit()
  
  def exit(self):
    if self.__lock:
      self.__lock.release()
    self.merge()

  def merge(self):
    for original, copies in self.__real_duplicate_map.items():
      for copy in copies:
        for change in copy._pcc_tracked_changes:
          setattr(original, change, getattr(copy, change))
        copy._pcc_tracked_changes.clear()

  def add(self, tp, *args, **kwargs):
    if not isinstance(tp, type):
      raise SyntaxError("%s is not a type" % tp)
    if len(args) < 1:
      raise SyntaxError("No objects of type %s" % tp.__name__)
    copy_args = self.__track_collection(args)
    params = tuple()
    if hasattr(tp, "__parameter_types__"):
      try:
        params = kwargs["params"]
      except KeyError:
        raise TypeError("Cannot create %s without params. Pass params using params keyword argument" % tp.__realname__)

    self.__object_store[tp] = tp.__create_pcc__(self.__change_type, *copy_args, param = params) if hasattr(tp, "__create_pcc__") else copy_args[0] if len(copy_args) == 1 else copy_args
    return self.__object_store[tp]

  def __track_collection(self, container):
    return container.__class__([self.__recurse_map[self.__get_type(obj)](obj) for obj in container])

  def __get_type(self, obj):
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

  def __track_object(self, obj):
    if obj not in self.__real_duplicate_same_type:
      container = self.__get_empty_tracking_container(type(obj))
      newobj = container()
      self.__real_duplicate_map[obj] = set([newobj])
      self.__real_duplicate_same_type[obj] = newobj
      self.__duplicate_real_map[newobj] = obj
      original_tp = obj.__class__ 
      if hasattr(obj, "__dimensions__"):
        container.__dimensions__ = original_tp.__dimensions__
        container.__dimensions_name__ = original_tp.__dimensions_name__
      else:
        for attr in dir(original_tp):
          if type(getattr(original_tp, attr)) == spacetime_property:
            container.__dimensions__.add(getattr(original_tp, attr))
            container.__dimensions_name__.add(attr)
      for dimension in container.__dimensions__:    
        value = getattr(obj, dimension._name)
        setattr(newobj, dimension._name, self.__recurse_map[self.__get_type(value)](value))
      newobj._start_tracking = True
      return newobj
    else:
      return self.__real_duplicate_same_type[obj]

  def __track_dictionary(self, dictionary):
    return dict(
        [(self.__recurse_map[self.__get_type(k)](k), 
          self.__recurse_map[self.__get_type(v)](v)) 
         for k,v in dictionary.items()]
      )

  def __change_type(self, obj, totype):
    container = self.__get_empty_tracking_container(totype)
    container.__dimensions__ = totype.__dimensions__
    container.__dimensions_name__ = totype.__dimensions_name__
    new_obj = container()
    for dimension in container.__dimensions_name__.intersection(obj.__class__.__dimensions_name__):
      setattr(new_obj, dimension, getattr(obj, dimension))
    new_obj._start_tracking = True
    original = self.__duplicate_real_map[obj]
    self.__duplicate_real_map[new_obj] = original
    self.__real_duplicate_map[original].add(new_obj)
    return new_obj
    
  def __get_empty_tracking_container(self, oftype):
    class _container(oftype):
      __dimensions__ = set()
      __dimensions_name__ = set()
      __original_class__ = oftype
      def __init__(s):
        s._pcc_tracked_changes = set()
        s._start_tracking = False

      def __getattr__(s, arg):
        try:
          return super(_container, s).__getattr__(arg)
        except AttributeError:
          raise AttributeError("Class %s has no attribute %s" % (oftype.__name__, arg))

      def __setattr__(s, arg, value):
        if "__dimensions" not in arg and arg in s.__class__.__dimensions_name__ and s._start_tracking:
          s._pcc_tracked_changes.add(arg)
        return super(_container, s).__setattr__(arg, value)

    return _container
