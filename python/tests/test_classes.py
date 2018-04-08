from __future__ import absolute_import

from rtypes.pcc.attributes import dimension, primarykey, predicate
from rtypes.pcc.types.set import pcc_set
from rtypes.pcc.types.subset import subset
from rtypes.pcc.types.join import join
from rtypes.pcc.types.projection import projection
from rtypes.pcc import THIS

class NonPCC(object):
    pass

@pcc_set
class SmallBase(object):
    @primarykey(str)
    def oid(self):
        return self._oid
    @oid.setter
    def oid(self, v):
        self._oid = v
    @dimension(str)
    def sprop1(self):
        return self._sp1
    @sprop1.setter
    def sprop1(self, v):
        self._sp1 = v

    def __init__(self, oid, sprop1):
        self.oid = oid
        self.sprop1 = sprop1

@pcc_set
class SmallIntBase(object):
    def __repr__(self):
        return "{0}, {1}".format(self.oid, self.iprop1)

    def __eq__(self, obj):
        return self.oid == obj.oid and self.iprop1 == obj.iprop1

    def __hash__(self):
        return hash((self.oid, self.iprop1))

    @primarykey(str)
    def oid(self):
        return self._oid

    @oid.setter
    def oid(self, v):
        self._oid = v

    @dimension(int)
    def iprop1(self):
        return self._ip1

    @iprop1.setter
    def iprop1(self, v):
        self._ip1 = v

    def __init__(self, oid, iprop):
        self._oid = oid
        self._ip1 = iprop

# pylint: disable=E1101,E0213
@subset(SmallIntBase)
class SubsetOddInt(object):
    def __repr__(self):
        return "{0}, {1}".format(self.oid, self.iprop1)

    def __eq__(self, obj):
        return self.oid == obj.oid and self.iprop1 == obj.iprop1

    def __hash__(self):
        return hash((self.oid, self.iprop1))

    @predicate(SmallIntBase.iprop1)
    def __predicate__(iprop1):
        return iprop1 % 2

@pcc_set
class LargeBase(object):
    @primarykey(str)
    def oid(self):
        return self._oid

    @oid.setter
    def oid(self, v):
        self._oid = v

    @dimension(str)
    def prop1(self):
        return self._p1

    @prop1.setter
    def prop1(self, v):
        self._p1 = v

    @dimension(list)
    def prop2(self):
        return self._p2

    @prop2.setter
    def prop2(self, v):
        self._p2 = v

    @dimension(NonPCC)
    def prop3(self):
        return self._p3

    @prop3.setter
    def prop3(self, v):
        self._p3 = v

    @dimension(SmallBase)
    def prop4(self):
        return self._p4

    @prop4.setter
    def prop4(self, v):
        self._p4 = v

    def func1(self):
        return self.prop1

    def __init__(self, oid, prop1):
        self.oid = oid
        self.prop1 = prop1

@pcc_set
class InheritedSmallBase(SmallBase):
    pass

class NonPCCInheritedSmallBase(SmallBase):
    pass

@subset(LargeBase)
class SubsetLargeBase(object):
    @staticmethod
    def __predicate__(lb):
        return lb.prop1 == "subset"

    def func2(self):
        return self.prop2

@subset(LargeBase)
class InheritedSubsetLargeBase(LargeBase):
    @staticmethod
    def __predicate__(lb):
        return lb.prop1 == "subset"

    def func2(self):
        return self.prop2


@projection(LargeBase, LargeBase.oid, LargeBase.prop1, LargeBase.prop2)
class ProjectLargeBase(object):
    def func2(self):
        return self.prop2

@subset(THIS)
@join(SB=SmallBase, LB=LargeBase)
class JoinSmallAndLargeBase(object):
    @predicate(THIS.SB.oid, THIS.SB.sprop1, THIS.LB.oid, THIS.LB.prop1)
    def __predicate__(sb_oid, sb_sprop1, lb_oid, lb_prop1):
        return sb_oid == lb_oid and sb_sprop1 == lb_prop1

@projection(THIS, THIS.SB.oid, THIS.LB.prop1)
@subset(THIS)
@join(SB=SmallBase, LB=LargeBase)
class ProjectedJoinSmallAndLargeBase(object):
    @predicate(THIS.SB.oid, THIS.SB.sprop1, THIS.LB.oid, THIS.LB.prop1)
    def __predicate__(sb_oid, sb_sprop1, lb_oid, lb_prop1):
        return sb_oid == lb_oid and sb_sprop1 == lb_prop1

@projection(THIS, THIS.SB1.oid, THIS.SB1.sprop1)
@subset(THIS)
@join(SB1=SmallBase, SB2=SmallBase)
class ProjectedJoinSmallAndSmallBase(object):
    @predicate(THIS.SB1.oid, THIS.SB1.sprop1, THIS.SB2.oid, THIS.SB2.sprop1)
    def __predicate__(sb1_oid, sb1_sprop1, sb2_oid, sb2_sprop1):
        return sb1_oid == sb2_oid and sb1_sprop1 == sb2_sprop1
