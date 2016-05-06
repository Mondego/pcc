from pcc.join import join
from pcc.subset import subset
from pcc.dataframe import dataframe
from pcc.attributes import dimension

class Transaction(object):
  @dimension(int)
  def card(self):
    return self._card

  @card.setter
  def card(self, value):
    self._card = value

  @dimension(int)
  def amount(self):
    return self._amount

  @amount.setter
  def amount(self, value):
    self._amount = value

  def __init__(self, card, amount):
    self.card = card
    self.amount = amount

  def flag(self):
    print "Whoa " + str(self.card) + "! You can't spend that much!"

class Card(object):
  @dimension(int)
  def id(self):
    return self._id

  @id.setter
  def id(self, value):
    self._id = value
    
  @dimension(bool)
  def holdstate(self):
    return self._holdstate

  @holdstate.setter
  def holdstate(self, value):
    self._holdstate = value

  @dimension(str)
  def owner(self):
    return self._owner

  @owner.setter
  def owner(self, value):
    self._owner = value
    
  def __init__(self, id, owner):
    self.id = id
    self.owner = owner
    self.holdstate = False

  def hold(self):
    self.holdstate = True
    print "Hold it card " + str(self.id) + "!"

class Person(object):
  @dimension(int)
  def id(self):
    return self._id

  @id.setter
  def id(self, value):
    self._id = value

  @dimension(str)
  def name(self):
    return self._name

  @name.setter
  def name(self, value):
    self._name = value

  def __init__(self, id, name):
    self.id = id
    self.name = name

  def notify(self):
    print "Hey " + str(self.name) + "! Your card is shadyyy!" 

@join(Person, Card, Transaction)
class RedAlert(object):
  @dimension(Person)
  def p(self):
    return self._p

  @p.setter
  def p(self, value):
    self._p = value

  @dimension(Card)
  def c(self):
    return self._c

  @c.setter
  def c(self, value):
    self._c = value

  @dimension(Transaction)
  def t(self):
    return self._t

  @t.setter
  def t(self, value):
    self._t = value

  def __init__(self, p, c, t):
    self.p = p
    self.c = c
    self.t = t

  @staticmethod
  def __predicate__(p, c, t):
    return c.owner == p.id and t.card == c.id and t.amount > 2000

  def Protect(self):
    self.t.flag()
    self.c.hold()
    self.p.notify()

@subset(RedAlert)
class RedAlertWithPersonVishnu(RedAlert.Class()):
  @staticmethod
  def __predicate__(ra):
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

with dataframe() as df:
  ras = df.add(RedAlert, [p1, p2], [c1p1, c2p1, c1p2], [t1, t2, t3])
  rawpvs = df.add(RedAlertWithPersonVishnu, ras)
  for ra in rawpvs:
    ra.Protect()

for c in [c1p1, c2p1, c1p2]:
  if c.holdstate:
    print "Card " + str(c.id) + " is under hold"




