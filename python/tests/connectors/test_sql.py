import unittest, json
import datetime
import uuid
from mysql.connector import MySQLConnection
from mysql.connector.errors import Error
from rtypes.connectors.sql import RTypesMySQLConnection

from rtypes.pcc.attributes import dimension, primarykey, count, namespace
from rtypes.pcc.types.set import pcc_set
from rtypes.pcc.types.subset import subset
from rtypes.pcc.types.impure import impure
from rtypes.pcc.types.parameter import parameter, ParameterMode
from rtypes.pcc.types.join import join
from rtypes.pcc.types.projection import projection
from rtypes.pcc import this
from rtypes.dataframe import dataframe

@pcc_set
class BasicTable(object):
    def __eq__(self, obj):
        return (
            (self.oid, self.prop1,
             self.prop2, self.prop3) == (
                 obj.oid, obj.prop1, obj.prop2, obj.prop3))

    def __repr__(self):
        return "%s, %s, %s, %s" % (
            str(self.oid), str(self.prop1), str(self.prop2), str(self.prop3))

    @primarykey(str)
    def oid(self):
        return self.__oid
    @oid.setter
    def oid(self, v):
        self.__oid = v

    @dimension(str)
    def prop1(self):
        return self.__p1
    @prop1.setter
    def prop1(self, v):
        self.__p1 = v

    @dimension(int)
    def prop2(self):
        return self.__p2
    @prop2.setter
    def prop2(self, v):
        self.__p2 = v

    @dimension(datetime.date)
    def prop3(self):
        return self.__p3
    @prop3.setter
    def prop3(self, v):
        self.__p3 = v

    def __init__(self, oid, p1, p2, p3):
        self.oid = oid
        self.prop1 = p1
        self.prop2 = p2
        self.prop3 = p3


@subset(BasicTable)
class EvenSubsetTable(object):
    @staticmethod
    def __predicate__(t):
        return t.prop2 % 2 == 0

@projection(this, this.oid, this.prop1)
@subset(BasicTable)
class EvenSubsetTableProjection(object):
    @staticmethod
    def __predicate__(t):
        return t.prop2 % 2 == 0


class test_sql(unittest.TestCase):
    def setUp(self):
        cred = json.load(open("tests/connectors/CRED.json"))
        self.pcc_connection = RTypesMySQLConnection(
            user=cred["user"], password=cred["password"],
            host="127.0.0.1", database="rtypes_test")
        self.sql_connection = MySQLConnection(
            user=cred["user"], password=cred["password"],
            host="127.0.0.1", database="rtypes_test")
        (self.uuid1, self.uuid2, self.uuid3,
         self.uuid4, self.uuid5, self.uuid6) = (
            str(uuid.uuid4()), str(uuid.uuid4()),
            str(uuid.uuid4()), str(uuid.uuid4()),
            str(uuid.uuid4()), str(uuid.uuid4()))
        self.df = None

    def cleanup_tables(self):
        cur = self.sql_connection.cursor()
        for tblname in set(["BasicTable",
                            "EvenSubsetTable", "EvenSubsetTableProjection"]):
            try:
                cur.execute("DROP TABLE {0};".format(tblname))
            except Error as err:
                pass
        self.sql_connection.commit()

    def test_sql(self):
        self.cleanup_tables()
        self.df = dataframe(external_db=self.pcc_connection)
        self.sql_basic_type_insert()
        self.sql_basic_objs_insert()
        self.sql_basic_objs_read()
        self.sql_basic_objs_modify()
        self.sql_basic_objs_delete()
        self.sql_subset_objs_modify()
        # self.sql_composition_objs_modify()

    def sql_basic_type_insert(self):
        self.df.add_types([BasicTable])
        self.df.push()
        cur = self.sql_connection.cursor()
        cur.execute("DESCRIBE BasicTable;")
        rows = cur.fetchall()
        self.assertDictEqual(
            {"oid": ("oid", "varchar(1000)", "NO", "PRI", None, ""),
             "prop1": ("prop1", "text", "YES", "", None, ""),
             "prop2": ("prop2", "int(11)", "YES", "", None, ""),
             "prop3": ("prop3", "datetime", "YES", "", None, "")},
            {pid: (pid, tp, is_null, key, default, extra)
             for pid, tp, is_null, key, default, extra in rows})
        cur.close()

    def sql_basic_objs_insert(self):
        self.df.extend(
            BasicTable,
            [BasicTable(self.uuid1, "o1", 1, datetime.datetime(1989, 10, 28)),
             BasicTable(self.uuid2, "o2", 2, datetime.datetime(1990, 04, 17)),
             BasicTable(self.uuid3, "o3", 3, datetime.datetime(2017, 11, 11))])
        self.df.push()
        cur = self.sql_connection.cursor()
        cur.execute(
            "SELECT oid, prop1, prop2, prop3 FROM BasicTable;")
        rows = cur.fetchall()
        self.assertSetEqual(
            set([(self.uuid1, "o1", 1, datetime.datetime(1989, 10, 28)),
             (self.uuid2, "o2", 2, datetime.datetime(1990, 04, 17)),
             (self.uuid3, "o3", 3, datetime.datetime(2017, 11, 11))]),
            set(rows))
        cur.close()

    def sql_basic_objs_read(self):
        cur = self.sql_connection.cursor()
        cur.execute(
            "INSERT INTO BasicTable (oid, prop1, prop2, prop3) "
            "VALUES (%s, %s, %s, %s);", (
                self.uuid4, "o4", 4, datetime.datetime(2018, 1, 1)))
        self.sql_connection.commit()
        cur.close()
        self.df.pull()
        bt_objs = self.df.get(BasicTable)
        self.assertEqual(4, len(bt_objs))
        self.assertListEqual(
            [BasicTable(self.uuid1, "o1", 1, datetime.datetime(1989, 10, 28)),
             BasicTable(self.uuid2, "o2", 2, datetime.datetime(1990, 04, 17)),
             BasicTable(self.uuid3, "o3", 3, datetime.datetime(2017, 11, 11)),
             BasicTable(self.uuid4, "o4", 4, datetime.datetime(2018, 1, 1))],
            sorted(bt_objs, key=lambda x: x.prop2))
        
    def sql_basic_objs_modify(self):
        obj = self.df.get(BasicTable, oid=self.uuid1)
        obj.prop2 = 10
        self.df.push()
        cur = self.sql_connection.cursor()
        cur.execute(
            "SELECT oid, prop1, prop2, prop3 FROM BasicTable;")
        rows = cur.fetchall()
        self.assertSetEqual(
            set([(self.uuid1, "o1", 10, datetime.datetime(1989, 10, 28)),
             (self.uuid2, "o2", 2, datetime.datetime(1990, 04, 17)),
             (self.uuid3, "o3", 3, datetime.datetime(2017, 11, 11)),
             (self.uuid4, "o4", 4, datetime.datetime(2018, 1, 1))]), set(rows))
        cur.close()

    def sql_basic_objs_delete(self):
        self.df.delete(BasicTable, self.df.get(BasicTable, oid=self.uuid4))
        self.df.push()
        self.sql_connection.reconnect()
        cur = self.sql_connection.cursor()
        cur.execute(
            "SELECT oid, prop1, prop2, prop3 FROM BasicTable;")
        rows = cur.fetchall()
        self.assertSetEqual(
            set([(self.uuid1, "o1", 10, datetime.datetime(1989, 10, 28)),
             (self.uuid2, "o2", 2, datetime.datetime(1990, 04, 17)),
             (self.uuid3, "o3", 3, datetime.datetime(2017, 11, 11))]),
            set(rows))
        cur.close()

    def sql_subset_objs_modify(self):
        self.df.add_type(EvenSubsetTable)
        self.df.push()
        cur1 = self.sql_connection.cursor()
        cur1.execute("DESCRIBE EvenSubsetTable;")
        rows1 = cur1.fetchall()
        self.assertDictEqual(
            {"oid": ("oid", "varchar(1000)", "NO", "", None, ""),
             "prop1": ("prop1", "text", "YES", "", None, ""),
             "prop2": ("prop2", "int(11)", "YES", "", None, ""),
             "prop3": ("prop3", "datetime", "YES", "", None, "")},
            {pid: (pid, tp, is_null, key, default, extra)
             for pid, tp, is_null, key, default, extra in rows1})
        cur1.close()
        self.sql_connection.reconnect()
        cur2 = self.sql_connection.cursor()
        cur2.execute(
            "SELECT oid, prop1, prop2, prop3 FROM EvenSubsetTable;")
        rows2 = cur2.fetchall()
        self.assertSetEqual(
            set([(self.uuid1, "o1", 10, datetime.datetime(1989, 10, 28)),
             (self.uuid2, "o2", 2, datetime.datetime(1990, 04, 17))]),
            set(rows2))
        objs1 = self.df.get(EvenSubsetTable)
        self.assertEqual(2, len(objs1))
        obj = self.df.get(EvenSubsetTable, oid=self.uuid1)
        self.assertIsNotNone(obj)
        obj.prop2 = 9
        cur2.close()
        self.sql_connection.reconnect()
        cur3 = self.sql_connection.cursor()
        cur3.execute(
            "SELECT oid, prop1, prop2, prop3 FROM EvenSubsetTable;")
        rows3 = cur3.fetchall()
        self.assertSetEqual(
            set([(self.uuid1, "o1", 10, datetime.datetime(1989, 10, 28)),
             (self.uuid2, "o2", 2, datetime.datetime(1990, 04, 17))]),
            set(rows3))
        objs = self.df.get(EvenSubsetTable)
        self.assertEqual(1, len(objs))
        self.assertEqual(self.uuid2, objs[0].oid)
        self.df.push()
        cur3.close()
        self.sql_connection.reconnect()
        cur4 = self.sql_connection.cursor()
        cur4.execute(
            "SELECT oid, prop1, prop2, prop3 FROM EvenSubsetTable;")
        rows4 = cur4.fetchall()
        self.assertSetEqual(
            set([(self.uuid2, "o2", 2, datetime.datetime(1990, 04, 17))]),
            set(rows4))
        cur4.close()
        self.sql_connection.reconnect()
        cur5 = self.sql_connection.cursor()
        cur5.execute(
            "INSERT INTO EvenSubsetTable (oid, prop1, prop2, prop3) "
            "VALUES (%s, %s, %s, %s);",
            (self.uuid5, "o5", 6, datetime.datetime(2017, 04, 17)))
        self.sql_connection.commit()
        cur5.close()
        self.df.pull()
        objs2 = self.df.get(EvenSubsetTable)
        self.assertEqual(2, len(objs2))
        self.assertSetEqual(
            set([self.uuid2, self.uuid5]), set(obj.oid for obj in objs2))

    def sql_composition_objs_modify(self):
        self.df.add_type(EvenSubsetTableProjection)
        self.df.push()
        cur1 = self.sql_connection.cursor()
        cur1.execute("DESCRIBE EvenSubsetTableProjection;")
        rows1 = cur1.fetchall()
        self.assertDictEqual(
            {"oid": ("oid", "varchar(1000)", "NO", "", None, ""),
             "prop1": ("prop1", "text", "YES", "", None, "")},
            {pid: (pid, tp, is_null, key, default, extra)
             for pid, tp, is_null, key, default, extra in rows1})
        cur1.close()
        self.sql_connection.reconnect()
        cur2 = self.sql_connection.cursor()
        cur2.execute(
            "SELECT oid, prop1 FROM EvenSubsetTableProjection;")
        rows2 = cur2.fetchall()
        self.assertSetEqual(
            set([(self.uuid2, "o2"), (self.uuid5, "o5")]),
            set(rows2))
        objs1 = self.df.get(EvenSubsetTableProjection)
        self.assertEqual(2, len(objs1))
        obj1 = self.df.get(EvenSubsetTableProjection, oid=self.uuid5)
        self.assertIsNotNone(obj1)
        self.assertEqual(self.uuid5, obj1.oid)
        self.assertEqual("o5", obj1.prop1)
        self.failUnlessRaises(AttributeError, obj1.prop2)
        self.failUnlessRaises(AttributeError, obj1.prop3)
        obj2 = self.df.get(EvenSubsetTable, oid=self.uuid5)
        self.assertIsNotNone(obj2)
        obj2.prop2 = 5
        cur2.close()
        self.sql_connection.reconnect()
        cur3 = self.sql_connection.cursor()
        cur3.execute(
            "SELECT oid, prop1 FROM EvenSubsetTableProjection;")
        rows3 = cur3.fetchall()
        self.assertSetEqual(
            set([(self.uuid2, "o2"), (self.uuid5, "o5")]),
            set(rows3))
        objs = self.df.get(EvenSubsetTableProjection)
        self.assertEqual(1, len(objs))
        self.assertEqual(self.uuid2, objs[0].oid)
        self.df.push()
        cur3.close()
        self.sql_connection.reconnect()
        cur4 = self.sql_connection.cursor()
        cur4.execute(
            "SELECT oid, prop1 FROM EvenSubsetTableProjection;")
        rows4 = cur4.fetchall()
        self.assertSetEqual(
            set([(self.uuid2, "o2")]),
            set(rows4))
        cur4.close()
        self.sql_connection.reconnect()
        cur5 = self.sql_connection.cursor()
        cur5.execute(
            "INSERT INTO BasicTable (oid, prop1, prop2, prop3) "
            "VALUES (%s, %s, %s, %s);",
            (self.uuid6, "o6", 6, datetime.datetime(2017, 10, 10)))
        self.sql_connection.commit()
        cur5.close()
        self.df.pull()
        objs2 = self.df.get(EvenSubsetTableProjection)
        self.assertEqual(2, len(objs2))
        self.assertSetEqual(
            set([self.uuid2, self.uuid6]), set(obj.oid for obj in objs2))
