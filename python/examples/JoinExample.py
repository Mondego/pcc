from pcc.join import join
from pcc.dataframe import dataframe

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

@join(Person, Card, Transaction)
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
     if RedAlert.__predicate__(p, c, t)]

  @staticmethod
  def __predicate__(p, c, t):
    return c.owner == p.id and t.card == c.id and t.amount > 2000

  def Protect(self):
    #Functions Dont work!! Have to manipulate variables directly.
    #self.t.flag()
    #self.c.hold()
    #self.p.notify()
    self.c.holdstate = True


p1 = Person(0, "Vishnu")
c1p1 = Card(0, 0)
c2p1 = Card(1, 0)
p2 = Person(1, "Indira")
c1p2 = Card(2, 1)
t1 = Transaction(1, 100)
t2 = Transaction(2, 1000)
t3 = Transaction(0, 10000)

with RedAlert(universe = dataframe(([p1, p2], [c1p1, c2p1, c1p2], [t1, t2, t3]))) as ras:
  for ra in ras.All():
    ra.Protect()

for c in [c1p1, c2p1, c1p2]:
  if c.holdstate:
    print "Card " + str(c.id) + " is under hold"

try:
  RedAlert.Create(p1, c1p1, t1)
except TypeError:
  print "Creating a incorrect RedAlert did not work"


# Requirements: Constructor and __predicate__ functions
# of Join class (RedAlert) must have same arguments