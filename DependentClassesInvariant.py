# The preparation for Joins
import copy, gc

def Static(func):
  class Func(object):
    def __init__(self):
      self.func = func
    
    def __call__(self, *args, **kwargs):
      return self.func(*args, **kwargs)

  return Func()

class Join(object):
  def __init__(self, *args):
    self.types = args

  def __call__(self, cl):
    types = self.types
    class _Join(object):
      def __init__(s, universe = None):
        if not universe:
          universe = s.__getfromgc__(types)
        s._original = universe
        s.cl = cl
        self.cl = cl
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

      @Static
      def Create(*args, **kwargs):
        # Tricky line. Self is top level class. Not _Permutation!
        return self.cl(*args, **kwargs) if self.cl.__invariant__(*args, **kwargs) else None

      @Static
      def __invariant__(*args, **kwargs):
        # Tricky line. Self is top level class. Not _Permutation!
        return self.cl.__invariant__(*args, **kwargs)
        
      def __getfromgc__(s, types):
        typemap = {}
        for item in gc.get_objects():
          if type(item) in types:
            typemap.setdefault(item.__class__, set()).add(item)
        return [list(typemap[t]) for t in types]

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

# This part down is the application

class Transaction(object):
  def __init__(self, card, amount):
    self.card = card
    self.amount = amount

  def flag(self):
    print "Whoa " + str(self.card) + "! You can't spend that much!"

class Card(object):
  def __init__(self, id, owner):
    self.id = id
    self.owner = owner
    self.holdstate = False

  def hold(self):
    self.holdstate = True
    print "Hold it card " + str(self.id) + "!"

class Person(object):
  def __init__(self, id, name):
    self.id = id
    self.name = name

  def notify(self):
    print "Hey " + str(self.name) + "! Your card is shadyyy!" 

@Join(Person, Card, Transaction)
class RedAlert(object):
  def __init__(self, p, c, t):
    self.p = p
    self.c = c
    self.t = t

  @Static
  def __query__(persons, cards, transactions):
    return [RedAlert.Create(p, c, t) 
     for p in persons 
     for c in cards 
     for t in transactions
     if RedAlert.__invariant__(p, c, t)]

  @Static
  def __invariant__(p, c, t):
    return c.owner == p.id and t.card == c.id and t.amount > 2000

  def Protect(self):
    self.t.flag()
    self.c.hold()
    self.p.notify()


p1 = Person(0, "Vishnu")
c1p1 = Card(0, 0)
c2p1 = Card(1, 0)
p2 = Person(1, "Indira")
c1p2 = Card(2, 1)
t1 = Transaction(1, 100)
t2 = Transaction(2, 1000)
t3 = Transaction(0, 10000)

with RedAlert(([p1, p2], [c1p1, c2p1, c1p2], [t1, t2, t3])) as ras:
  for ra in ras.All():
    ra.Protect()

for c in [c1p1, c2p1, c1p2]:
  if c.holdstate:
    print "Card " + str(c.id) + " is under hold"


if not RedAlert.Create(p1, c1p1, t1):
  print "Creating a incorrect RedAlert did not work"


# Requirements: Constructor and __invariant__ functions
# of Join class (RedAlert) must have same arguments