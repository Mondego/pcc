from dependent_classes.join import Join
from dependent_classes.subset import Subset

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

  @staticmethod
  def __query__(persons, cards, transactions):
    return [RedAlert.Create(p, c, t) 
     for p in persons 
     for c in cards 
     for t in transactions
     if RedAlert.__invariant__(p, c, t)]

  @staticmethod
  def __invariant__(p, c, t):
    return c.owner == p.id and t.card == c.id and t.amount > 2000

  def Protect(self):
    self.t.flag()
    self.c.hold()
    self.p.notify()

@Subset(RedAlert)
class RedAlertWithPersonVishnu(RedAlert):
  @staticmethod
  def __query__(redalerts):
    return [ra 
     for ra in redalerts
     if RedAlertWithPersonVishnu.__invariant__(ra)]

  @staticmethod
  def __invariant__(ra):
    return ra.p.name == "Vishnu"

p1 = Person(0, "Vishnu")
c1p1 = Card(0, 0)
c2p1 = Card(1, 0)
p2 = Person(1, "Indira")
c1p2 = Card(2, 1)
p3 = Person(2, "Bramha")
c1p3 = Card(3, 2)
t1 = Transaction(1, 100)
t2 = Transaction(2, 1000)
t3 = Transaction(0, 10000)
#Also RedAlert Card but not Vishnu's
t4 = Transaction(3, 10000)

with RedAlert(universe = ([p1, p2], [c1p1, c2p1, c1p2], [t1, t2, t3])) as ras:
  with RedAlertWithPersonVishnu(universe = ras.All()) as rawpvs:
    for ra in rawpvs.All():
      ra.Protect()

for c in [c1p1, c2p1, c1p2]:
  if c.holdstate:
    print "Card " + str(c.id) + " is under hold"
