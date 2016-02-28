from dependent_classes.subset import Subset

class Transaction(object):
  def __init__(self, card, amount):
    self.card = card
    self.amount = amount
    self.suspicious = False

  def declare(self):
    print str(self.card) + "/" + str(self.amount) + ": This transaction is " + ("suspicious" if self.suspicious else " not suspicious")


@Subset(Transaction)
class HighValueTransaction(Transaction):
  @staticmethod
  def __query__(transactions):
    return [t 
     for t in transactions
     if HighValueTransaction.__invariant__(t)]

  @staticmethod
  def __invariant__(t):
    return t.amount > 2000

  def flag(self):
    self.suspicious = True


t1 = Transaction(1, 100)
t2 = Transaction(2, 1000)
t3 = Transaction(0, 10000)

with HighValueTransaction(universe = [t1, t2, t3]) as hvts:
  for hvt in hvts.All():
    hvt.flag()

for t in [t1, t2, t3]:
  t.declare()
