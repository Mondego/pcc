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

  @dimension(float)
  def suspicious(self):
    return self._suspicious

  @suspicious.setter
  def suspicious(self, value):
    self._suspicious = value

  def __init__(self, card, amount):
    self.card = card
    self.amount = amount
    self.suspicious = False

  def declare(self):
    print str(self.card) + "/" + str(self.amount) + ": This transaction is " + ("suspicious" if self.suspicious else " not suspicious")


@subset(Transaction)
class HighValueTransaction(Transaction):
  @staticmethod
  def __predicate__(t):
    return t.amount > 2000

  def flag(self):
    self.suspicious = True


t1 = Transaction(1, 100)
t2 = Transaction(2, 1000)
t3 = Transaction(0, 10000)

with dataframe() as df:
  for hvt in df.add(HighValueTransaction, [t1, t2, t3]):
    hvt.flag()

for t in [t1, t2, t3]:
  t.declare()
