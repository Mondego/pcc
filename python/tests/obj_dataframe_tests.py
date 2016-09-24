from __future__ import absolute_import

from pcc.attributes import dimension, primarykey
from pcc.set import pcc_set
from pcc.subset import subset
from pcc.impure import impure
from pcc.dataframe import dataframe
from pcc.parameter import parameter, ParameterMode
from pcc.join import join
from pcc.projection import projection
    
import unittest

def _load_edge_nodes():
    @pcc_set
    class Node(object):
        @primarykey(int)
        def oid(self):
            return self._id

        @oid.setter
        def oid(self, value):
            self._id = value
    
        @dimension(float)
        def pagerank(self):
            return self._pagerank

        @pagerank.setter
        def pagerank(self, value):
            self._pagerank = value

        def __init__(self, oid, pagerank):
            self.oid, self.pagerank = oid, pagerank

    @pcc_set
    class Edge(object):
        @primarykey(int)
        def oid(self):
            return self._id

        @oid.setter
        def oid(self, value):
            self._id = value
    
        @dimension(Node)
        def start(self):
            return self._start

        @start.setter
        def start(self, value):
            self._start = value

        @dimension(Node)
        def end(self):
            return self._end

        @end.setter
        def end(self, value):
            self._end = value
    
        def __init__(self, n1, n2):
            self.start, self.end = (n1, n2)

    return Node, Edge

def _CreateNodesAndEdges():
    Node, Edge = _load_edge_nodes()
    nodes = list()
    edges = list()
    for i in range(4):
        nodes.append(Node(i, 0.25))

    edges.append(Edge(nodes[0],nodes[1]))
    edges.append(Edge(nodes[0],nodes[2]))
    edges.append(Edge(nodes[0],nodes[3]))
    edges.append(Edge(nodes[1],nodes[2]))
    edges.append(Edge(nodes[3],nodes[2]))
    return Node, Edge, nodes, edges

def _subset_classes():
    
    @pcc_set
    class Transaction(object):
        @primarykey(str)
        def oid(self): return self._id

        @oid.setter
        def oid(self, value): self._id = value

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
    return Transaction, HighValueTransaction, [t1,t2,t3]

def _CreateInAndOutEdgeTypes(Edge, Node):
    @parameter(Node, mode = ParameterMode.Singleton)
    @subset(Edge)
    class InEdge(Edge):
        @staticmethod
        def __predicate__(e, n):
            return e.end.oid == n.oid

    @parameter(Node, mode = ParameterMode.Singleton)
    @subset(Edge)
    class OutEdge(Edge):
        @staticmethod
        def __predicate__(e, n):
            return e.start.oid == n.oid
    return InEdge, OutEdge

def _join_example_data():
    @pcc_set
    class Transaction(object):
        @primarykey(str)
        def oid(self): return self._id

        @oid.setter
        def oid(self, value): self._id = value

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

        @dimension(bool)
        def flagged(self):
            return self._flagged

        @flagged.setter
        def flagged(self, value):
            self._flagged = value

        def __init__(self, card, amount):
            self.card = card
            self.amount = amount
            self.flagged = False

        def flag(self):
            self.flagged = True

    @pcc_set
    class Card(object):
        @primarykey(int)
        def oid(self):
            return self._id

        @oid.setter
        def oid(self, value):
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
        
        def __init__(self, oid, owner):
            self.oid = oid
            self.owner = owner
            self.holdstate = False

        def hold(self):
            self.holdstate = True

    @pcc_set
    class Person(object):
        @primarykey(int)
        def oid(self):
            return self._id

        @oid.setter
        def oid(self, value):
            self._id = value

        @dimension(str)
        def name(self):
            return self._name

        @name.setter
        def name(self, value):
            self._name = value

        def __init__(self, oid, name):
            self.oid = oid
            self.name = name

        def notify(self):
            pass

    @join(Person, Card, Transaction)
    class RedAlert(object):
        @primarykey(str)
        def oid(self): return self._id

        @oid.setter
        def oid(self, value): self._id = value

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
            return c.owner == p.oid and t.card == c.oid and t.amount > 2000

        def Protect(self):
            self.t.flag()
            self.c.hold()
            self.p.notify()

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
    return Person, Card, Transaction, RedAlert, [p1, p2, p3], [c1p1, c2p1, c1p2, c1p3], [t1, t2, t3, t4]

def _CreateProjectionTypesAndObjects():
    @pcc_set
    class Car(object):
        @primarykey(int)
        def oid(self): return self._id
        @oid.setter
        def oid(self, value): self._id = value
        @dimension(int)
        def owner(self): return self._owner
        @owner.setter
        def owner(self, value): self._owner = value
        @dimension(int)
        def location(self): return self._location
        @location.setter
        def location(self, value): self._location = value
        @dimension(int)
        def velocity(self): return self._velocity
        @velocity.setter
        def velocity(self, value): self._velocity = value

        def __init__(self, id, owner, location, velocity):
            (self.id, self.owner, self.location, self.velocity) = (id, owner, location, velocity)

        def change_owner(self, owner, license):
            self.owner, self.license = owner, license

        def details(self):
            return (self.id, self.owner, self.location, self.velocity)

    @projection(Car, Car.oid, Car.location, Car.velocity)
    class CarForPedestrian(object):
        pass

    car1 = Car(1, "Murugan", "himalaya", 299792458)
    car2 = Car(2, "Shiva", "himalaya", 299792459)

    return Car, CarForPedestrian, [car1, car2] 



class Test_dataframe_object_tests(unittest.TestCase):
    def test_base_set_addition(self):
        Node, Edge, nodes, edges = _CreateNodesAndEdges()
        df = dataframe()
        df.add_types([Node, Edge])
        df.extend(Node, nodes)
        df.extend(Edge, edges)
        new_nodes = df.get(Node)
        new_edges = df.get(Edge)
        self.failUnless(len(new_nodes) == 4)
        self.failUnless(len(new_edges) == 5)
    
    def test_pure_subset_get(self):
        Transaction, HighValueTransaction, ts = _subset_classes()
        df = dataframe()
        df.add_types([Transaction, HighValueTransaction])
        df.extend(Transaction, ts)
        self.assertTrue(len(df.object_map[HighValueTransaction.__realname__]) == 1)
        hvts = df.get(HighValueTransaction)
        self.assertTrue(len(hvts) == 1)
        for hvt in hvts:
            hvt.flag()
        self.assertTrue(len(df.get(Transaction)) == 3)
        self.assertTrue([t.suspicious for t in df.get(Transaction)].count(False) == 2)
        self.assertTrue([t.suspicious for t in df.get(Transaction)].count(True) == 1)    

    def test_impure_subset_get(self):
        Transaction, HighValueTransaction, ts = _subset_classes()
        HighValueTransaction = impure(HighValueTransaction)
        df = dataframe()
        df.add_types([Transaction, HighValueTransaction])
        df.extend(Transaction, ts)
        self.assertTrue((HighValueTransaction.__realname__ in df.object_map) == False)
        hvts = df.get(HighValueTransaction)
        self.assertTrue(len(hvts) == 1)
        for hvt in hvts:
            hvt.flag()
        self.assertTrue(len(df.get(Transaction)) == 3)
        self.assertTrue([t.suspicious for t in df.get(Transaction)].count(False) == 2)
        self.assertTrue([t.suspicious for t in df.get(Transaction)].count(True) == 1)    

    def test_parameter_supplied(self):
        Node, Edge, nodes, edges = _CreateNodesAndEdges()
        InEdge, OutEdge = _CreateInAndOutEdgeTypes(Edge, Node)
        df = dataframe()
        df.add_types([Node, Edge, InEdge, OutEdge])
        df.extend(Node, nodes)
        df.extend(Edge, edges)
        self.assertTrue((OutEdge.__realname__ in df.object_map) == False)
        self.assertTrue((InEdge.__realname__ in df.object_map) == False)
        self.assertTrue(len(df.get(OutEdge, (nodes[0],))) == 3)
        self.assertTrue(isinstance(df.get(OutEdge, (nodes[0],))[0], OutEdge.Class()))
        self.assertTrue(len(df.get(InEdge, (nodes[0],))) == 0) 

    def test_join_get(self):
        Person, Card, Transaction, RedAlert, persons, cards, transactions = _join_example_data()
        df = dataframe()
        df.add_types([Person, Card, Transaction, RedAlert])
        df.extend(Person, persons)
        df.extend(Card, cards)
        df.extend(Transaction, transactions)
        self.assertTrue((RedAlert.__realname__ in df.object_map) == False)
        self.assertTrue(len(df.get(RedAlert)) == 2)
        for ra in df.get(RedAlert):
            ra.Protect()
        self.assertTrue(transactions[2].flagged == True)
        self.assertTrue(transactions[0].flagged == False)
        self.assertTrue(cards[0].holdstate == True)
        self.assertTrue(cards[1].holdstate == False)
    
    def test_multi_level_subset_get(self):
        @pcc_set
        class Car(object):
            @primarykey(str)
            def oid(self): return self._id

            @oid.setter
            def oid(self, value): self._id = value

            @dimension(int)
            def velocity(self): return self._velocity

            @velocity.setter
            def velocity(self, value): self._velocity = value

            @dimension(str)
            def color(self): return self._color

            @color.setter
            def color(self, value): self._color = value

            def __init__(self, vel, col):
                self.velocity = vel
                self.color = col

        @subset(Car)
        class ActiveCar(Car.Class()):
            @staticmethod
            def __predicate__(c):
                return c.velocity != 0

        @subset(ActiveCar)
        class RedActiveCar(Car.Class()):
            @staticmethod
            def __predicate__(ac):
                return ac.color == "RED"

        cars = [Car(0, "BLUE"), Car(0, "RED"), Car(1, "GREEN"), Car(1, "RED"), Car(2, "RED")]
        df = dataframe()
        df.add_types([Car, ActiveCar, RedActiveCar])
        df.extend(Car, cars)
        self.assertTrue(len(df.get(RedActiveCar)) == 2)
        self.assertTrue(len(df.get(ActiveCar)) == 3)
        df.get(RedActiveCar)[0].velocity = 0
        self.assertTrue(len(df.get(RedActiveCar)) == 1)
        self.assertTrue(len(df.get(ActiveCar)) == 2)

    def test_projection_get(self):
        Car, CarForPedestrian, cars = _CreateProjectionTypesAndObjects()
        df = dataframe()
        df.add_types([Car, CarForPedestrian])
        df.extend(Car, cars)
        self.assertTrue(len(df.object_map[CarForPedestrian.__realname__]) == 2)
        cars_p = df.get(CarForPedestrian)
        self.assertTrue(len(cars_p) == 2)
        for c in cars_p:
            self.assertTrue(hasattr(c, "location"))
            self.assertTrue(hasattr(c, "velocity"))
            self.assertTrue(hasattr(c, "oid"))
            self.assertFalse(hasattr(c, "owner"))

    