from __future__ import absolute_import

from rtypes.pcc.attributes import dimension, primarykey
from rtypes.pcc.types.set import pcc_set

class Customer(object):
    pass

@pcc_set
class Transaction(object):
    @primarykey(str)
    def oid(self): return self.__oid
    @oid.setter
    def oid(self, v): self.__oid = v

    @dimension(str)
    def name(self): return self.__n
    @name.setter
    def name(self, v): self.__n = v

    @dimension(int)
    def amount(self): return self.__a
    @amount.setter
    def amount(self, v): self.__a = v

    def __eq__(self, other):
        return ((self.oid == other.oid)
                and (self.name == other.name)
                and (self.amount == other.amount))

    def __init__(self, name, amount):
        self.oid = name
        self.name = name
        self.amount = amount

@pcc_set
class TransactionHistory(object):
    def __init__(self, name, count):
        self.oid = name
        self.name = name
        self.count = count

    @primarykey(str)
    def oid(self): return self.__oid
    @oid.setter
    def oid(self, v): self.__oid = v

    @dimension(str)
    def name(self): return self.__n
    @name.setter
    def name(self, v): self.__n = v

    @dimension(int)
    def count(self): return self.__c
    @count.setter
    def count(self, v): self.__c = v

@pcc_set
class TransactionList(object):
    def __init__(self, name):
        self.oid = name
        self.history = []

    @primarykey(str)
    def oid(self): return self.__oid
    @oid.setter
    def oid(self, v): self.__oid = v

    @dimension(list)
    def history(self): return self.__h
    @history.setter
    def history(self, v): self.__h = v
