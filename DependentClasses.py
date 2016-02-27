
''' Part of framework?'''

import gc, inspect

def Subset(arg):
  return Join(arg)

class Join(object):
  def __init__(self, *args):
    self.args = args

  def __call__(self, *args, **kwargs):
    if len(kwargs) == 0:
      self.cl = args[0]
    
    the_args = self.args
    class _Join(self.cl):
      def __init__(self, *args, **kwargs):
        def universe_slicer(obj_list = None):
          if obj_list == None:
            gc.collect()
            obj_list = gc.get_objects()
          typemap = {}
          types = set(the_args)
          for obj in obj_list:
            try:
              if obj.__class__ in types:
                typemap.setdefault(obj.__class__, set()).add(obj)
            except AttributeError:
              continue

          return typemap
        self.universe = universe_slicer
        super(_Join, self).__init__(*args, **kwargs)
#        try:
#          if len(kwargs) > 0:
#            trial_param = {}
#            for k in kwargs:
#              trial_param.setdefault(kwargs[k].__class__, set()).add(kwargs[k])
#            items = self.Query(lambda : trial_param)
#            print items
#            if self not in set(items):
#              raise AttributeError(str.format("Object {0} does not fit the membership condition for {1}", obj, obj.__class__))
#        except RuntimeError:
          # all good. should go recursively
#          return 

      def __call__(self, from_gc = True):
        return self.Query(self.universe)
    return _Join




''' Actual Developer definitions'''

class Car(object):
  def __init__(self, id, position):
    self.id = id
    self.position = position

  def moveleft(self):
    self.position = (self.position[0] - 1, self.position[1])

  def moveright(self):
    self.position = (self.position[0] + 1, self.position[1])

  def moveup(self):
    self.position = (self.position[0], self.position[1] + 1)

  def movedown(self):
    self.position = (self.position[0], self.position[1] - 1)

class Person(object):
  def __init__(self, pid, position):
    self.pid = pid
    self.position = position

  def moveleft(self):
    self.position = (self.position[0] - 1, self.position[1])

  def moveright(self):
    self.position = (self.position[0] + 1, self.position[1])

  def moveup(self):
    self.position = (self.position[0], self.position[1] + 1)

  def movedown(self):
    self.position = (self.position[0], self.position[1] - 1)


@Join(Car, Person)
class CarAndPersonNextToEachOther(object):
  def __init__(self, *args, **kwargs):
    if "Car" in kwargs:
      self.car = kwargs["Car"]
    if "Person" in kwargs:
      self.person = kwargs["Person"]

  def avoid_each_other(self):
    self.person.moveup()

  def Query(self, universe_slice):
    return [CarAndPersonNextToEachOther(Car = c, Person = p) 
     for c in universe_slice()[Car] 
     for p in universe_slice()[Person]
     if abs(c.position[0] - p.position[0]) < 2 and c.position[1] == p.position[1]]

@Subset(Car)
class CarAtPos3(object):
  def __init__(self, *args, **kwargs):
    if "Car" in kwargs:
      self.car = kwargs["Car"]
    
  def say_hi(self):
    print "Hi! I'm at position 3", self.car.id

  def Query(self, universe_slice):
    return [CarAtPos3(Car = c) 
     for c in universe_slice()[Car] 
     if c.position[0] == 3]



# Example using specific tracked objects
car1 = Car(0, (0,0))
person1 = Person(0, (10, 0))
while car1.position[0] != 10 and person1.position[0] != 0:
  for cp in CarAndPersonNextToEachOther()([car1, person1]):
    cp.avoid_each_other()

  for c3 in CarAtPos3()():
    c3.say_hi()
  car1.moveright()
  person1.moveleft()
  print car1.position, person1.position


# Example 2: Using global state
car1 = Car(0, (0,0))
person1 = Person(0, (10, 0))
while car1.position[0] != 10 and person1.position[0] != 0:
  for cp in CarAndPersonNextToEachOther()():
    cp.avoid_each_other()

  for c3 in CarAtPos3()():
    c3.say_hi()
  car1.moveright()
  person1.moveleft()
  print car1.position, person1.position


# Trying to create a new object of Join type that does not meet condition.
try:
  cp = CarAtPos3(Car = Car(0, (3,0)))
  print cp
except AttributeError as e:
  print "Failed to create CarAtPos3 with Car(0, (3,0))"
try:
  cp = CarAtPos3(Car = Car(0, (2,0)))
  print cp
except AttributeError as e:
  print e, "Failed to create CarAtPos3 with Car(0, (2,0))"
