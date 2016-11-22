from dataframe import dataframe_new
from set import pcc_set
from impure import impure
from subset import subset
from join import join
from attributes import dimension, primarykey

@pcc_set
class BaseSet(object):
    @primarykey(str)
    def p1(self): return self._p1
    @p1.setter
    def p1(self, v): self._p1 = v
    @dimension(str)
    def p2(self): return self._p2
    @p2.setter
    def p2(self, v): self._p2 = v
    @dimension(str)
    def p3(self): return self._p3
    @p3.setter
    def p3(self, v): self._p3 = v

@subset(BaseSet)
class Subset(object):
    @staticmethod
    def __predicate__(bs): return bs.p2 == "Subset"

@join(BaseSet, BaseSet)
class JoinSet(object):
    @dimension(BaseSet)
    def bs1(self): return self._bs1
    @bs1.setter
    def bs1(self, v): self._bs1 = v
    @dimension(BaseSet)
    def bs2(self): return self._bs2
    @bs2.setter
    def bs2(self, v): self._bs2 = v
    def __init__(self, bs1, bs2):
        self.bs1 = bs1
        self.bs2 = bs2

    @staticmethod
    def __predicate__(bs1, bs2): return bs1==bs2

df = dataframe_new()
df.add_types([BaseSet, Subset])
df.add_type(JoinSet)

print df.object_manager.current_state
print df.object_manager.object_map

b1 = BaseSet()
b1.p1 = "a"
b1.p2 = "Base"
b1.p3 = "c"
b2 = BaseSet()
b2.p1 = "x"
b2.p2 = "Subset"
b2.p3 = "z"
b3 = BaseSet()
b3.p1 = "m"
b3.p2 = "Base"
b3.p3 = "o"
df.extend(BaseSet, [b1, b2])
df.append(BaseSet, b3)
print df.object_manager.current_state
print df.object_manager.object_map
print"##################"
print df.get(JoinSet)

b3.p2 = "Subset"
print df.object_manager.current_state
print df.object_manager.object_map
print"##################"
df.delete(BaseSet, b3)
print df.object_manager.current_state
print df.object_manager.object_map
print"##################"
