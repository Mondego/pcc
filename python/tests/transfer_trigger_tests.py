from __future__ import absolute_import

from rtypes.pcc.triggers import trigger, TriggerProcedure, TriggerTime
from rtypes.pcc.triggers import TriggerAction, BlockAction
from tests.trigger_test_classes import Transaction, TransactionHistory, TransactionList
from tests.trigger_test_classes import Customer

from rtypes.dataframe.trigger_manager import TriggerManager
from rtypes.dataframe import dataframe

import unittest

####################################################
################### Triggers Here ##################
####################################################


def create_triggers():

    @trigger(Customer, TriggerTime.before, TriggerAction.create, 1)
    def before_create(dataframe, new, old, current):
        pass

    @trigger(Customer, TriggerTime.after, TriggerAction.create, 1)
    def after_create(dataframe, new, old, current):
        pass

    @trigger(Customer, TriggerTime.before, TriggerAction.read, 1)
    def before_read(dataframe, new, old, current):
        pass

    @trigger(Customer, TriggerTime.after, TriggerAction.read, 1)
    def after_read(dataframe, new, old, current):
        pass

    @trigger(Customer, TriggerTime.before, TriggerAction.update, 1)
    def before_update(dataframe, new, old, current):
        pass

    @trigger(Customer, TriggerTime.after, TriggerAction.update, 1)
    def after_update(dataframe, new, old, current):
        pass

    @trigger(Customer, TriggerTime.before, TriggerAction.delete, 1)
    def before_delete(dataframe, new, old, current):
        pass

    @trigger(Customer, TriggerTime.after, TriggerAction.delete, 1)
    def after_delete(dataframe, new, old, current):
        pass

    return before_create, after_create, before_read, after_read, \
        before_update, after_update, before_delete, after_delete

def trigger_kinds():
    return ["bc", "ac", "br", "ar", "bu",
            "au", "bd", "bd"]

def trigger_kinds_seprate():
    return [(TriggerTime.before, TriggerAction.create), (TriggerTime.after, TriggerAction.create), (TriggerTime.before, TriggerAction.read),
            (TriggerTime.after, TriggerAction.read), (TriggerTime.before, TriggerAction.update), (TriggerTime.after, TriggerAction.update),
            (TriggerTime.before, TriggerAction.delete),(TriggerTime.after, TriggerAction.delete)]


####################################################
############## Unit Tests Start Here ###############
####################################################


class Test_trigger_transfer_test(unittest.TestCase):

    def test_trigger_types(self):
        """ Test if each kind of trigger has the correct attributes """

        # Create an instance of every possible type of trigger
        before_create, after_create, before_read, after_read, before_update, \
            after_update, before_delete, after_delete = create_triggers()

        # Perfrom tests that chech type, time, action, and priority
        self.assertTrue(before_create.pcc_type == Customer)
        self.assertTrue(before_create.time == TriggerTime.before)
        self.assertTrue(before_create.action == TriggerAction.create)
        self.assertTrue(before_create.priority == 1)

        self.assertTrue(after_create.pcc_type == Customer)
        self.assertTrue(after_create.time == TriggerTime.after)
        self.assertTrue(after_create.action == TriggerAction.create)
        self.assertTrue(after_create.priority == 1)

        self.assertTrue(before_read.pcc_type == Customer)
        self.assertTrue(before_read.time == TriggerTime.before)
        self.assertTrue(before_read.action == TriggerAction.read)
        self.assertTrue(before_read.priority == 1)

        self.assertTrue(after_read.pcc_type == Customer)
        self.assertTrue(after_read.time == TriggerTime.after)
        self.assertTrue(after_read.action == TriggerAction.read)
        self.assertTrue(after_read.priority == 1)

        self.assertTrue(before_update.pcc_type == Customer)
        self.assertTrue(before_update.time == TriggerTime.before)
        self.assertTrue(before_update.action == TriggerAction.update)
        self.assertTrue(before_update.priority == 1)

        self.assertTrue(after_update.pcc_type == Customer)
        self.assertTrue(after_update.time == TriggerTime.after)
        self.assertTrue(after_update.action == TriggerAction.update)
        self.assertTrue(after_update.priority == 1)

        self.assertTrue(before_delete.pcc_type == Customer)
        self.assertTrue(before_delete.time == TriggerTime.before)
        self.assertTrue(before_delete.action == TriggerAction.delete)
        self.assertTrue(before_delete.priority == 1)

        self.assertTrue(after_delete.pcc_type == Customer)
        self.assertTrue(after_delete.time == TriggerTime.after)
        self.assertTrue(after_delete.action == TriggerAction.delete)
        self.assertTrue(after_delete.priority == 1)

    def test_trigger_manager_single_add(self):
        """ Test if adding a trigger using .add_trigger works for each kind of trigger """

        before_create, after_create, before_read, after_read, before_update, \
        after_update, before_delete, after_delete = create_triggers()

        TM = TriggerManager()

        TM.add_trigger(before_create)
        self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
        self.assertTrue("bc" in TM.trigger_map[Customer])
        self.assertTrue(before_create in TM.trigger_map[Customer]["bc"])

        TM.add_trigger(after_create)
        self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
        self.assertTrue("ac" in TM.trigger_map[Customer])
        self.assertTrue(after_create in TM.trigger_map[Customer]["ac"])

        TM.add_trigger(before_read)
        self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
        self.assertTrue("br" in TM.trigger_map[Customer])
        self.assertTrue(before_read in TM.trigger_map[Customer]["br"])

        TM.add_trigger(after_read)
        self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
        self.assertTrue("ar" in TM.trigger_map[Customer])
        self.assertTrue(after_read in TM.trigger_map[Customer]["ar"])

        TM.add_trigger(before_update)
        self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
        self.assertTrue("bu" in TM.trigger_map[Customer])
        self.assertTrue(before_update in TM.trigger_map[Customer]["bu"])

        TM.add_trigger(after_update)
        self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
        self.assertTrue("au" in TM.trigger_map[Customer])
        self.assertTrue(after_update in TM.trigger_map[Customer]["au"])

        TM.add_trigger(before_delete)
        self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
        self.assertTrue("bd" in TM.trigger_map[Customer])
        self.assertTrue(before_delete in TM.trigger_map[Customer]["bd"])

        TM.add_trigger(after_delete)
        self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
        self.assertTrue("ad" in TM.trigger_map[Customer])
        self.assertTrue(after_delete in TM.trigger_map[Customer]["ad"])

    def test_trigger_manager_multiple_add(self):
        """ Test if adding multiple triggers using .add_triggers works for 
            all kinds of triggers """

        TM = TriggerManager()

        for t_kind in trigger_kinds():

            TM.add_triggers(list(create_triggers()))
            # Check if the pcc_type was addded and that their is only one of them
            self.assertTrue(Customer in TM.trigger_map and len(TM.trigger_map) == 1)
            # Check that this kind of trigger is associated with the pcc_type
            self.assertTrue(t_kind in TM.trigger_map[Customer])
            # Check that this kind of trigger has a TriggerProcedure in it's list
            self.assertTrue(TriggerProcedure == type(TM.trigger_map[Customer][t_kind][0]))

    def test_trigger_manager_execute_trigger(self):
        TM = TriggerManager()
        
        TM.add_triggers(create_triggers())
        
        for time, action in trigger_kinds_seprate():
            self.assertTrue(
                TM.execute_trigger(Customer, time, action, None, None, None, None))
    
    def test_trigger_manager_remove(self):
        TM = TriggerManager()

        TM.add_triggers(create_triggers())

        for time, action in trigger_kinds_seprate():
            self.assertTrue(TM.trigger_map[Customer][time + action] != [])

        for trigger in create_triggers():
            TM.remove_trigger(trigger)

        for time, action in trigger_kinds_seprate():
            self.assertTrue(TM.trigger_map[Customer][time + action] == [])

    """
    Notes:
        Read:
            - Cannot read data of type x within a trigger attached to type x
            - After read triggers dont execute unless the read is valid
                - read isn't valid if you are reading an object that doesn't exist
        Update:
            - Before:
                - inside the trigger: cannot change dimensions that are being changed in
                  the actual update action that activated the trigger

    Trigger test should be valid for Create, Read, Update, and Delete (CRUD) actions
    Should check that:
        Before:
            1) Trigger arguments: old, new, current - are the correct values
            2) Using BlockAction prevents the CRUD action from taking place (only when before)
            3) Before triggers can alter demensions of an object that is being CRUD
            4) Any trigger can access/change [similar] objects in dataframe
            5) Any trigger can access/change [different] objects in dataframe
            6) Any trigger can insert objects into the dataframe
            7) That overlapping triggers execute in the right order
        After
            1) Trigger arguments: old, new, current - are the correct values
            2) Using BlockAction doesn't do anything to the object of concern
            3) After triggers cannot alter demensions of an object that is being CRUD
            4) Any trigger can access/change [similar] objects in dataframe
            5) Any trigger can access/change [different] objects in dataframe
            6) Any trigger can insert objects into the dataframe
            7) That overlapping triggers execute in the right order
   """

    #######################
    #### before create ####

    def test_bc_1(self):
        """
        Tests that before create trigger arguments are the correct values
        dataframe == dataframe object
        new == object trigger is dealing with
        old == None
        current == None
        """
        results = list()
        @trigger(Transaction, TriggerTime.before, TriggerAction.create)
        def before_create(dataframe, new, old, current):
            # Need to use this type of assert because this is executes outside this class
            results.append(("before_create", dataframe, new, old, current))

        df = dataframe()
        df.add_trigger(before_create)
        df.add_type(Transaction)
        obj = Transaction("Robert", 10)
        df.append(Transaction, obj)

        self.assertListEqual([("before_create", df, obj, None, None)], results)

    def test_bc_2(self):
        """
        Tests that before create triggers can block creates using BlockAction
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.create)
        def before_create(datatframe, new, old, current):
            if new.name == "Robert":
                raise BlockAction

        df = dataframe()
        df.add_trigger(before_create)
        df.add_type(Transaction)
        self.assertListEqual([], df.get(Transaction)) # Nothing before creation of object
        df.append(Transaction, Transaction("Robert", 10))
        self.assertEqual([], df.get(Transaction)) # Nothing after creation of object

    def test_bc_3(self):
        """
        Tests that before create triggers can alter an object before it is inserted
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.create)
        def before_create(datatframe, new, old, current):
            if new.name == "Robert":
                new.amount = 1000

        df = dataframe()
        df.add_trigger(before_create)
        df.add_type(Transaction)

        df.append(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Robert").amount, 1000)

    def test_bc_4(self):
        """
        Tests that before create triggers can [access|change]
        objects in dataframe of the [same] type
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.create)
        def before_create(dataframe, new, old, current):
            if new.name == "Robert":
                jake = dataframe.get(Transaction, "Jake")
                jake.amount = 1000

        df = dataframe()
        df.add_trigger(before_create)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Jake", 10))
        df.append(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Jake").amount, 1000)

    def test_bc_5(self):
        """
        Tests that before create triggers can [access|change]
        objects in dataframe of a [different] type
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.create)
        def before_create(dataframe, new, old, current):
            log = dataframe.get(TransactionHistory, "Robert")
            log.count += 1

        df = dataframe()
        df.add_trigger(before_create)
        df.add_types([Transaction, TransactionHistory])
        
        df.append(TransactionHistory, TransactionHistory("Robert", 0))

        df.append(Transaction, Transaction("Robert", 10))

        self.assertEqual(df.get(TransactionHistory, "Robert").count, 1)

    def test_bc_6(self):
        """
        Tests that before create triggers can insert objects into the dataframe
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.create)
        def before_create(dataframe, new, old, current):
            dataframe.append(TransactionHistory, TransactionHistory(new.name, 0))

        df = dataframe()
        df.add_trigger(before_create)
        df.add_types([Transaction, TransactionHistory])

        df.append(Transaction, Transaction("Robert", 10))

        self.assertTrue(isinstance(df.get(TransactionHistory, "Robert"),
                                   TransactionHistory))

    def test_bc_7(self):
        """
        Tests that overlapping before create triggers execute in the right order
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.create, 0)
        def before_create_0(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(0)

        @trigger(Transaction, TriggerTime.before, TriggerAction.create, 1)
        def before_create_1(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(1)

        @trigger(Transaction, TriggerTime.before, TriggerAction.create, 2)
        def before_create_2(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(2)

        @trigger(Transaction, TriggerTime.before, TriggerAction.create, 3)
        def before_create_3(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(3)

        @trigger(Transaction, TriggerTime.before, TriggerAction.create, 4)
        def before_create_4(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(4)

        df = dataframe()
        df.add_triggers([before_create_1,
                         before_create_0,
                         before_create_3,
                         before_create_2,
                         before_create_4])

        df.add_types([Transaction, TransactionList])

        df.append(TransactionList, TransactionList("Robert"))
        df.append(Transaction, Transaction("Robert", 0))

        self.assertEqual(df.get(TransactionList, "Robert").history, [0, 1, 2, 3, 4])

    #######################
    #### after create #####

    def test_ac_1(self):
        """
        Tests that after create trigger arguments are the correct values
        dataframe == dataframe object
        new == object trigger is dealing with
        old == None
        current == object trigger is dealing with
        """
        result = list()

        @trigger(Transaction, TriggerTime.after, TriggerAction.create)
        def after_create(dataframe, new, old, current):
            result.append(("after_create", dataframe, new, old, current))

        df = dataframe()
        df.add_trigger(after_create)
        df.add_type(Transaction)
        obj = Transaction("Robert", 10)
        df.append(Transaction, obj)
        self.assertListEqual([("after_create", df, obj, None, obj)], result)

    def test_ac_2(self):
        """
        Tests taht raising BlockAction in a after create triggers does nothing concern
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.create)
        def after_create(datatframe, new, old, current):
            if new.name == "Robert":
                raise BlockAction

        df = dataframe()
        df.add_trigger(after_create)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Robert", 10))
        self.assertTrue(df.get(Transaction, "Robert"), Transaction("Robert", 10))

    def test_ac_3(self):
        """
        After create triggers cannot alter demensions of an object that is being CRUD
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.create)
        def after_create(datatframe, new, old, current):
            new.amount = 1000

        df = dataframe()
        df.add_trigger(after_create)
        df.add_type(Transaction)
        test = Transaction("Robert", 10)
        df.append(Transaction, test)
        self.assertTrue(df.get(Transaction, "Robert") == test)

    def test_ac_4(self):
        """
        Tests that after create triggers can [access|change]
        objects in dataframe of the [same] type
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.create)
        def after_create(dataframe, new, old, current):
            if new.name == "Robert":
                jake = dataframe.get(Transaction, "Jake")
                jake.amount = 1000

        df = dataframe()
        df.add_trigger(after_create)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Jake", 10))
        df.append(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Jake").amount, 1000)

    def test_ac_5(self):
        """
        Tests that after create triggers can [access|change]
        objects in dataframe of a [different] type
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.create)
        def after_creeate(dataframe, new, old, current):
            log = dataframe.get(TransactionHistory, "Robert")
            log.count += 1

        df = dataframe()
        df.add_trigger(after_creeate)
        df.add_types([Transaction, TransactionHistory])
        
        df.append(TransactionHistory, TransactionHistory("Robert", 0))

        df.append(Transaction, Transaction("Robert", 10))

        self.assertEqual(df.get(TransactionHistory, "Robert").count, 1)

    def test_ac_6(self):
        """
        Tests that after create triggers can insert objects into the dataframe
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.create)
        def after_create(dataframe, new, old, current):
            dataframe.append(TransactionHistory, TransactionHistory(new.name, 0))

        df = dataframe()
        df.add_trigger(after_create)
        df.add_types([Transaction, TransactionHistory])

        df.append(Transaction, Transaction("Robert", 10))

        self.assertTrue(isinstance(df.get(TransactionHistory, "Robert"),
                                   TransactionHistory))

    def test_ac_7(self):
        """
        Tests that overlapping after create triggers execute in the right order
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 0)
        def after_create_0(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(0)

        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 1)
        def after_create_1(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(1)

        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 2)
        def after_create_2(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(2)

        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 3)
        def after_create_3(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(3)

        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 4)
        def after_create_4(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(4)

        df = dataframe()
        df.add_triggers([after_create_1,
                         after_create_0,
                         after_create_3,
                         after_create_2,
                         after_create_4])

        df.add_types([Transaction, TransactionList])

        df.append(TransactionList, TransactionList("Robert"))
        df.append(Transaction, Transaction("Robert", 0))

        self.assertEqual(df.get(TransactionList, "Robert").history, [0, 1, 2, 3, 4])

    #######################
    ##### before read #####

    def test_br_1(self):
        """
        Tests that before read trigger arguments are the correct values
        dataframe == dataframe object
        new == None
        old == None
        current == None
        """
        result = list()
        @trigger(Transaction, TriggerTime.before, TriggerAction.read)
        def before_read(dataframe, new, old, current):
            result.append(("before_read", dataframe, new, old, current))

        df = dataframe()
        df.add_trigger(before_read)
        df.add_type(Transaction)
        obj = Transaction("Robert", 10)
        df.append(Transaction, obj)
        test = df.get(Transaction, "Robert")
        self.assertListEqual([("before_read", df, None, None, None)], result)

    def test_br_2(self):
        """
        Tests that before read triggers can block reads using BlockAction
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.read)
        def before_read(datatframe, new, old, current):
            raise BlockAction

        df = dataframe()
        df.add_trigger(before_read)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Robert", 10))
        test = df.get(Transaction)
        self.assertEqual(test, None)

    def test_br_5(self):
        """
        Tests that before read triggers can [access|change]
        objects in dataframe of a [different] type
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.read)
        def before_read(dataframe, new, old, current):
            log = dataframe.get(TransactionHistory, "Robert")
            log.count += 1

        df = dataframe()
        df.add_trigger(before_read)
        df.add_types([Transaction, TransactionHistory])
        
        df.append(TransactionHistory, TransactionHistory("Robert", 0))

        df.get(Transaction)

        self.assertEqual(df.get(TransactionHistory, "Robert").count, 1)

    def test_br_6(self):
        """
        Tests that before read triggers can insert objects into the dataframe
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.read)
        def before_read(dataframe, new, old, current):
            dataframe.append(TransactionHistory, TransactionHistory("Robert", 0))

        df = dataframe()
        df.add_trigger(before_read)
        df.add_types([Transaction, TransactionHistory])

        df.get(Transaction)
        self.assertTrue(isinstance(df.get(TransactionHistory, "Robert"),
                                   TransactionHistory))

    def test_br_7(self):
        """
        Tests that overlapping before read triggers execute in the right order
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.read, 0)
        def before_r_0(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(0)

        @trigger(Transaction, TriggerTime.before, TriggerAction.read, 1)
        def before_r_1(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(1)

        @trigger(Transaction, TriggerTime.before, TriggerAction.read, 2)
        def before_r_2(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(2)

        @trigger(Transaction, TriggerTime.before, TriggerAction.read, 3)
        def before_r_3(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(3)

        @trigger(Transaction, TriggerTime.before, TriggerAction.read, 4)
        def before_r_4(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(4)

        df = dataframe()
        df.add_triggers([before_r_1,
                         before_r_0,
                         before_r_3,
                         before_r_2,
                         before_r_4])

        df.add_types([Transaction, TransactionList])

        df.append(TransactionList, TransactionList("Robert"))
        df.get(Transaction)
        self.assertEqual(df.get(TransactionList, "Robert").history, [0, 1, 2, 3, 4])

    #######################
    ##### after read ######

    def test_ar_1(self):
        """
        Tests that after create trigger arguments are the correct values
        dataframe == dataframe object
        new == None
        old == object trigger is dealing with
        current == object trigger is dealing with
        """
        result = list()
        @trigger(Transaction, TriggerTime.after, TriggerAction.read)
        def after_read(dataframe, new, old, current):
            result.append(("after_read", dataframe, new, old, current))

        df = dataframe()
        df.add_trigger(after_read)
        df.add_type(Transaction)
        obj = Transaction("Robert", 10)
        df.append(Transaction, obj)
        test = df.get(Transaction)
        self.assertListEqual([("after_read", df, None, obj, obj)], result)

    def test_ar_2(self):
        """
        Tests that rasing Block Action in after read triggers does nothing
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.read)
        def after_read(datatframe, new, old, current):
            if old.name == "Robert":
                raise BlockAction

        df = dataframe()
        df.add_trigger(after_read)
        df.add_type(Transaction)
        test_1 = Transaction("Robert", 10)
        df.append(Transaction, test_1)
        self.assertEqual(df.get(Transaction, "Robert"), test_1)

    def test_ar_3(self):
        """
        After read triggers cannot alter demensions of an object that is being CRUD
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.read)
        def after_read(datatframe, new, old, current):
            old.amount = 100
            current.amount = 100

        df = dataframe()
        df.add_trigger(after_read)
        df.add_type(Transaction)
        test = Transaction("Robert", 10)
        df.append(Transaction, test)
        self.assertTrue(df.get(Transaction, "Robert") == test)

    def test_ar_5(self):
        """
        Tests that after create triggers can [access|change]
        objects in dataframe of a [different] type
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.read)
        def after_read(dataframe, new, old, current):
            log = dataframe.get(TransactionHistory, "Robert")
            log.count += 1

        df = dataframe()
        df.add_trigger(after_read)
        df.add_types([Transaction, TransactionHistory])
        
        df.append(TransactionHistory, TransactionHistory("Robert", 0))

        df.append(Transaction, Transaction("Robert", 10))

        df.get(Transaction, "Robert")

        self.assertEqual(df.get(TransactionHistory, "Robert").count, 1)

    def test_ar_6(self):
        """
        Tests that after read triggers can insert objects into the dataframe
        After read triggers only activate if the get is valid, if you are getting an
        object that doesn't exist the ar trigger wont activate
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.read)
        def after_read(dataframe, new, old, current):
            dataframe.append(TransactionHistory, TransactionHistory("Robert", 0))

        df = dataframe()
        df.add_trigger(after_read)
        df.add_types([Transaction, TransactionHistory])

        df.append(Transaction, Transaction("Robert", 10))

        df.get(Transaction, "Robert")

        self.assertTrue(isinstance(df.get(TransactionHistory, "Robert"),
                                   TransactionHistory))

    def test_ar_7(self):
        """
        Tests that overlapping after read triggers execute in the right order
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.read, 0)
        def ar_0(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(0)

        @trigger(Transaction, TriggerTime.after, TriggerAction.read, 1)
        def ar_1(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(1)

        @trigger(Transaction, TriggerTime.after, TriggerAction.read, 2)
        def ar_2(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(2)

        @trigger(Transaction, TriggerTime.after, TriggerAction.read, 3)
        def ar_3(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(3)

        @trigger(Transaction, TriggerTime.after, TriggerAction.read, 4)
        def ar_4(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(4)

        df = dataframe()
        df.add_triggers([ar_1, ar_0, ar_3, ar_2, ar_4])

        df.add_types([Transaction, TransactionList])

        df.append(TransactionList, TransactionList("Robert"))
        df.append(Transaction, Transaction("Robert", 10))
        df.get(Transaction)
        self.assertEqual(df.get(TransactionList, "Robert").history, [0, 1, 2, 3, 4])

    #######################
    #### before delete ####

    def test_bd_1(self):
        """
        Tests that before delete trigger arguments are the correct values
        dataframe == dataframe object
        new == None
        old == object trigger is dealing with
        current == object trigger is dealing with
        """
        result = list()
        @trigger(Transaction, TriggerTime.before, TriggerAction.delete)
        def before_delete(dataframe, new, old, current):
            result.append(("before_delete", dataframe, new, old, current))

        df = dataframe()
        df.add_trigger(before_delete)
        df.add_type(Transaction)
        obj = Transaction("Robert", 10)
        df.append(Transaction, obj)
        df.delete(Transaction, obj)
        self.assertListEqual([("before_delete", df, None, obj, obj)], result)

    def test_bd_2(self):
        """
        Tests that before delete triggers can block deletes using BlockAction
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.delete)
        def before_delete(datatframe, new, old, current):
            if old.name == "Robert":
                raise BlockAction

        df = dataframe()
        df.add_trigger(before_delete)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Robert", 10))
        df.delete(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Robert"), Transaction("Robert", 10))

    # You can change the object, but it is going to be deleted right after...
    # Unless you have a block, the delete will remove the object
    # a --> w/o BlockAction
    # b --> w/ BlockAction
    def test_bd_a(self):
        """
        Tests that before delete triggers can alter an object before it is inserted
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.delete)
        def before_delete(datatframe, new, old, current):
            if old.name == "Robert":
                old.amount = 1000

        df = dataframe()
        df.add_trigger(before_delete)
        df.add_type(Transaction)

        df.append(Transaction, Transaction("Robert", 10))
        df.delete(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Robert"), None)

    def test_bd_3_b(self):
        """
        Tests that before delete triggers can alter an object before it is inserted
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.delete)
        def before_delete(datatframe, new, old, current):
            if old.name == "Robert":
                old.amount = 1000
                raise BlockAction

        df = dataframe()
        df.add_trigger(before_delete)
        df.add_type(Transaction)

        df.append(Transaction, Transaction("Robert", 10))
        df.delete(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Robert"), Transaction("Robert", 10))

    def test_bd_4(self):
        """
        Tests that before delete triggers can [access|change]
        objects in dataframe of the [same] type
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.delete)
        def before_delete(dataframe, new, old, current):
            if old.name == "Robert":
                jake = dataframe.get(Transaction, "Jake")
                jake.amount = 1000

        df = dataframe()
        df.add_trigger(before_delete)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Jake", 10))
        df.append(Transaction, Transaction("Robert", 10))
        df.delete(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Jake").amount, 1000)

    def test_bd_5(self):
        """
        Tests that before delete triggers can [access|change]
        objects in dataframe of a [different] type
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.delete)
        def before_delete(dataframe, new, old, current):
            log = dataframe.get(TransactionHistory, "Robert")
            log.count += 1

        df = dataframe()
        df.add_trigger(before_delete)
        df.add_types([Transaction, TransactionHistory])
        
        df.append(TransactionHistory, TransactionHistory("Robert", 0))
        df.append(Transaction, Transaction("Robert", 10))

        df.delete(Transaction, Transaction("Robert", 10))

        self.assertEqual(df.get(TransactionHistory, "Robert").count, 1)

    def test_bd_6(self):
        """
        Tests that before delete triggers can insert objects into the dataframe
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.create)
        def before_delete(dataframe, new, old, current):
            dataframe.append(TransactionHistory, TransactionHistory(new.name, 0))

        df = dataframe()
        df.add_trigger(before_delete)
        df.add_types([Transaction, TransactionHistory])

        df.append(Transaction, Transaction("Robert", 10))
        df.delete(Transaction, Transaction("Robert", 10))
        self.assertTrue(isinstance(df.get(TransactionHistory, "Robert"),
                                   TransactionHistory))

    def test_bd_7(self):
        """
        Tests that overlapping before delete triggers execute in the right order
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.delete, 0)
        def before_delete_0(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(0)

        @trigger(Transaction, TriggerTime.before, TriggerAction.delete, 1)
        def before_delete_1(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(1)

        @trigger(Transaction, TriggerTime.before, TriggerAction.delete, 2)
        def before_delete_2(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(2)

        @trigger(Transaction, TriggerTime.before, TriggerAction.delete, 3)
        def before_delete_3(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(3)

        @trigger(Transaction, TriggerTime.before, TriggerAction.delete, 4)
        def before_delete_4(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(4)

        df = dataframe()
        df.add_triggers([before_delete_1,
                         before_delete_0,
                         before_delete_3,
                         before_delete_2,
                         before_delete_4])

        df.add_types([Transaction, TransactionList])

        df.append(TransactionList, TransactionList("Robert"))
        df.append(Transaction, Transaction("Robert", 0))

        df.delete(Transaction, Transaction("Robert", 10))

        self.assertEqual(df.get(TransactionList, "Robert").history, [0, 1, 2, 3, 4])

    #######################
    #### after delete #####

    def test_ad_1(self):
        """
        Tests that after delete trigger arguments are the correct values
        dataframe == dataframe object
        new == None
        old == object trigger is dealing with
        current == None
        """
        result = list()
        @trigger(Transaction, TriggerTime.after, TriggerAction.delete)
        def after_delete(dataframe, new, old, current):
            result.append(("after_delete", dataframe, new, old, current))

        df = dataframe()
        df.add_trigger(after_delete)
        df.add_type(Transaction)
        obj = Transaction("Robert", 10)
        df.append(Transaction, obj)
        df.delete(Transaction, obj)
        self.assertListEqual([("after_delete", df, None, obj, None)], result)

    def test_ad_2(self):
        """
        Tests that raising BlockAction in after delete triggers does nothing
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.delete)
        def after_create(datatframe, new, old, current):
            raise BlockAction

        df = dataframe()
        df.add_trigger(after_create)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Robert", 10))
        df.delete(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Robert"), None)

    def test_ad_3(self):
        """
        After delete triggers cannot alter demensions of an object that is being CRUD
        Because the object is not in dataframe
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.delete)
        def after_delete(datatframe, new, old, current):
            old.amount = 1000

        df = dataframe()
        df.add_trigger(after_delete)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Robert", 10))
        df.delete(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Robert"), None)

    def test_ad_4(self):
        """
        Tests that after delete triggers can [access|change]
        objects in dataframe of the [same] type
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.delete)
        def after_delete(dataframe, new, old, current):
            if old.name == "Robert":
                jake = dataframe.get(Transaction, "Jake")
                jake.amount = 1000

        df = dataframe()
        df.add_trigger(after_delete)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Jake", 10))
        df.append(Transaction, Transaction("Robert", 10))
        df.delete(Transaction, Transaction("Robert", 10))
        self.assertEqual(df.get(Transaction, "Jake").amount, 1000)

    def test_ad_5(self):
        """
        Tests that after delete triggers can [access|change]
        objects in dataframe of a [different] type
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.delete)
        def after_delete(dataframe, new, old, current):
            log = dataframe.get(TransactionHistory, "Robert")
            log.count += 1

        df = dataframe()
        df.add_trigger(after_delete)
        df.add_types([Transaction, TransactionHistory])
        
        df.append(TransactionHistory, TransactionHistory("Robert", 0))

        df.append(Transaction, Transaction("Robert", 10))

        df.delete(Transaction, Transaction("Robert", 10))

        self.assertEqual(df.get(TransactionHistory, "Robert").count, 1)

    def test_ad_6(self):
        """
        Tests that after delete triggers can insert objects into the dataframe
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.delete)
        def after_delete(dataframe, new, old, current):
            dataframe.append(TransactionHistory, TransactionHistory(old.name, 0))

        df = dataframe()
        df.add_trigger(after_delete)
        df.add_types([Transaction, TransactionHistory])

        df.append(Transaction, Transaction("Robert", 10))

        df.delete(Transaction, Transaction("Robert", 10))

        self.assertTrue(isinstance(df.get(TransactionHistory, "Robert"),
                                   TransactionHistory))

    def test_ad_7(self):
        """
        Tests that overlapping after delete triggers execute in the right order
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 0)
        def ad_0(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(0)

        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 1)
        def ad_1(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(1)

        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 2)
        def ad_2(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(2)

        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 3)
        def ad_3(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(3)

        @trigger(Transaction, TriggerTime.after, TriggerAction.create, 4)
        def ad_4(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(4)

        df = dataframe()
        df.add_triggers([ad_1,
                         ad_0,
                         ad_3,
                         ad_2,
                         ad_4])

        df.add_types([Transaction, TransactionList])

        df.append(TransactionList, TransactionList("Robert"))
        df.append(Transaction, Transaction("Robert", 0))

        df.delete(Transaction, Transaction("Robert", 10))

        self.assertEqual(df.get(TransactionList, "Robert").history, [0, 1, 2, 3, 4])
    
    #######################
    #### before update ####

    def test_bu_1(self):
        """
        Tests that before create trigger arguments are the correct values
        dataframe == dataframe object
        new == None
        old == obj
        current == obj
        """
        result = list()
        @trigger(Transaction, TriggerTime.before, TriggerAction.update)
        def before_update(dataframe, new, old, current):
            result.append(("before_update", dataframe, new, old, current))

        df = dataframe()
        df.add_trigger(before_update)
        df.add_type(Transaction)
        obj = Transaction("Robert", 10)
        df.append(Transaction, obj)
        robert = df.get(Transaction, "Robert")
        robert.amount = 0
        self.assertListEqual([("before_update", df, None, obj, obj)], result)

    def test_bu_2(self):
        """
        Tests that before create triggers can block creates using BlockAction
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.update)
        def before_update(datatframe, new, old, current):
            if old.name == "Robert":
                raise BlockAction

        df = dataframe()
        df.add_trigger(before_update)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Robert", 10))
        robert = df.get(Transaction, "Robert")

        robert.amount = 1000 # update the value
        self.assertEqual(robert.amount, 10) # the value stays the same because of trigger
        
    def test_bu_3(self):
        """
        Tests that before update triggers can alter an object before it is inserted,
        but not the same dimensions that are being changed
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.update)
        def before_update(datatframe, new, old, current):
            old.name = "Jake"
            old.amount = 1000

        df = dataframe()
        df.add_trigger(before_update)
        df.add_type(Transaction)

        df.append(Transaction, Transaction("Robert", 10))
        robert = df.get(Transaction, "Robert")

        robert.amount = 0

        self.assertEqual(df.get(Transaction, "Robert").amount, 0)
        self.assertEqual(df.get(Transaction, "Robert").name, "Jake")

    def test_bu_4(self):
        """
        Tests that before update triggers can [access|change]
        objects in dataframe of the [same] type
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.update)
        def before_update(dataframe, new, old, current):
            if old.name == "Robert":
                jake = dataframe.get(Transaction, "Jake")
                jake.amount = 1000

        df = dataframe()
        df.add_trigger(before_update)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Jake", 10))
        df.append(Transaction, Transaction("Robert", 10))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0 # activate the trigger
        
        # check that the trigger worked
        self.assertEqual(df.get(Transaction, "Jake").amount, 1000)

    def test_bu_5(self):
        """
        Tests that before update triggers can [access|change]
        objects in dataframe of a [different] type
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.update)
        def before_update(dataframe, new, old, current):
            log = dataframe.get(TransactionHistory, "Robert")
            log.count += 1

        df = dataframe()
        df.add_trigger(before_update)
        df.add_types([Transaction, TransactionHistory])
        
        df.append(TransactionHistory, TransactionHistory("Robert", 0))
        df.append(Transaction, Transaction("Robert", 10))

        robert = df.get(Transaction, "Robert")
        robert.amount = 1

        self.assertEqual(df.get(TransactionHistory, "Robert").count, 1)

    def test_bu_6(self):
        """
        Tests that before update triggers can insert objects into the dataframe
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.update)
        def before_update(dataframe, new, old, current):
            dataframe.append(TransactionHistory, TransactionHistory(old.name, 0))

        df = dataframe()
        df.add_trigger(before_update)
        df.add_types([Transaction, TransactionHistory])

        df.append(Transaction, Transaction("Robert", 10))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0 

        self.assertTrue(isinstance(df.get(TransactionHistory, "Robert"),
                                   TransactionHistory))

    def test_bu_7(self):
        """
        Tests that overlapping before update triggers execute in the right order
        """
        @trigger(Transaction, TriggerTime.before, TriggerAction.update, 0)
        def before_update_0(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(0)

        @trigger(Transaction, TriggerTime.before, TriggerAction.update, 1)
        def before_update_1(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(1)

        @trigger(Transaction, TriggerTime.before, TriggerAction.update, 2)
        def before_update_2(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(2)

        @trigger(Transaction, TriggerTime.before, TriggerAction.update, 3)
        def before_update_3(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(3)

        @trigger(Transaction, TriggerTime.before, TriggerAction.update, 4)
        def before_update_4(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(4)

        df = dataframe()
        df.add_triggers([before_update_1,
                         before_update_0,
                         before_update_3,
                         before_update_2,
                         before_update_4])

        df.add_types([Transaction, TransactionList])

        df.append(TransactionList, TransactionList("Robert"))
        df.append(Transaction, Transaction("Robert", 0))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0

        self.assertEqual(df.get(TransactionList, "Robert").history, [0, 1, 2, 3, 4])

    #######################
    #### after update #####

    def test_au_1(self):
        """
        Tests that after update trigger arguments are the correct values
        dataframe == dataframe object
        new == object trigger is dealing with
        old == None
        current == object trigger is dealing with
        """
        result = list()
        @trigger(Transaction, TriggerTime.after, TriggerAction.update)
        def after_update(dataframe, new, old, current):
            result.append(("after_update", dataframe, new, old, current))


        df = dataframe()
        df.add_trigger(after_update)
        df.add_type(Transaction)
        obj = Transaction("Robert", 10)
        df.append(Transaction, obj)
        robert = df.get(Transaction, "Robert")
        robert.amount = 0
        self.assertListEqual([("after_update", df, obj, None, obj)], result)

    def test_au_2(self):
        """
        Tests that raising BlockAction in a after update triggers does nothing concern
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.update)
        def after_update(datatframe, new, old, current):
            raise BlockAction

        df = dataframe()
        df.add_trigger(after_update)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Robert", 10))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0

        self.assertEqual(df.get(Transaction, "Robert").amount, 0)

    def test_au_3(self):
        """
        Tests that after update triggers can alter an object before it is inserted
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.update)
        def before_update(datatframe, new, old, current):
            new.amount = 1000

        df = dataframe()
        df.add_trigger(before_update)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Robert", 10))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0

        self.assertEqual(df.get(Transaction, "Robert").amount, 1000)

    def test_au_4(self):
        """
        Tests that after update triggers can [access|change]
        objects in dataframe of the [same] type
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.update)
        def after_update(dataframe, new, old, current):
            if new.name == "Robert":
                jake = dataframe.get(Transaction, "Jake")
                jake.amount = 1000

        df = dataframe()
        df.add_trigger(after_update)
        df.add_type(Transaction)
        df.append(Transaction, Transaction("Jake", 10))
        df.append(Transaction, Transaction("Robert", 10))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0

        self.assertEqual(df.get(Transaction, "Jake").amount, 1000)

    def test_au_5(self):
        """
        Tests that after update triggers can [access|change]
        objects in dataframe of a [different] type
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.update)
        def after_update(dataframe, new, old, current):
            log = dataframe.get(TransactionHistory, "Robert")
            log.count += 1

        df = dataframe()
        df.add_trigger(after_update)
        df.add_types([Transaction, TransactionHistory])
        df.append(TransactionHistory, TransactionHistory("Robert", 0))
        df.append(Transaction, Transaction("Robert", 10))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0

        self.assertEqual(df.get(TransactionHistory, "Robert").count, 1)

    def test_au_6(self):
        """
        Tests that after update triggers can insert objects into the dataframe
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.update)
        def after_update(dataframe, new, old, current):
            dataframe.append(TransactionHistory, TransactionHistory(new.name, 0))

        df = dataframe()
        df.add_trigger(after_update)
        df.add_types([Transaction, TransactionHistory])
        df.append(Transaction, Transaction("Robert", 10))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0

        self.assertTrue(isinstance(df.get(TransactionHistory, "Robert"),
                                   TransactionHistory))

    def test_au_7(self):
        """
        Tests that overlapping after update triggers execute in the right order
        """
        @trigger(Transaction, TriggerTime.after, TriggerAction.update, 0)
        def after_update_0(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(0)

        @trigger(Transaction, TriggerTime.after, TriggerAction.update, 1)
        def after_update_1(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(1)

        @trigger(Transaction, TriggerTime.after, TriggerAction.update, 2)
        def after_update_2(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(2)

        @trigger(Transaction, TriggerTime.after, TriggerAction.update, 3)
        def after_update_3(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(3)

        @trigger(Transaction, TriggerTime.after, TriggerAction.update, 4)
        def after_update_4(dataframe, new, old, current):
            log = dataframe.get(TransactionList, "Robert")
            log.history.append(4)

        df = dataframe()
        df.add_triggers([after_update_1,
                         after_update_0,
                         after_update_3,
                         after_update_2,
                         after_update_4])

        df.add_types([Transaction, TransactionList])
        df.append(TransactionList, TransactionList("Robert"))
        df.append(Transaction, Transaction("Robert", 0))

        robert = df.get(Transaction, "Robert")
        robert.amount = 0

        self.assertEqual(df.get(TransactionList, "Robert").history, [0, 1, 2, 3, 4])
