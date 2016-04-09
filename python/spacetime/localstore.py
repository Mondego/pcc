class localstore(object):
  def __init__(self):
    # type -> list of objects
    self.objects = {}
    self.changes = {}
    self.id_object_map = {}

  def setchanges(self, new, mod, deleted):
    for t, objlist in new.items():
      for obj in objlist:
        obj._bind_tracklist(self.changes)
        self.objects.setdefault(t, {}).update(dict([(obj._primarykey, obj)]))
        self.id_object_map[obj._primarykey] = obj

    for t in mod:
      for id in mod[t]:
        self.objects[t][id].update(mod[t][id])

    for t in deleted:
      for id in mod[t]:
        self.objects[t].pop(id)
        if t in self.changes and id in self.changes[t]:
          self.changes[t].pop(id)

  def clearchangelist(self):
    self.changes = {}

  def getchanges(self):
    return self.changes
    
  def get(self, t):
    if hasattr(t, "_isPrimaryKey"):
      try:
        return self.id_object_map[t]
      except KeyError:
        raise KeyError("No object with id %s", t.__name__)
    else:
      try:
        return self.objects[t]
      except KeyError:
        raise KeyError("No object of type %s", t.__name__)

  def put(self, obj):
    if not hasattr(obj, "_primarykey"):
      raise TypeError("Put requires object of set type")
    self.changes.setdefault(obj._actualclass, {}).setdefault(obj._primarykey, []).extend(
        [(arg, value) for arg, value in obj.__dict__.items()])

  def delete(self, obj):
    if not hasattr(obj, "_primarykey"):
      raise TypeError("Delete requires object of set type")
    if obj._primarykey in self.id_object_map:
      raise RuntimeError("Cannot delete object %s that is not present", obj._primarykey)
    self.changes.setdefault(obj._actualclass, {}).setdefault(obj._primarykey, []).append(("*", "Delete"))
