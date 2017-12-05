from __future__ import absolute_import

from rtypes.pcc.triggers import trigger, TriggerProcedure, TriggerTime, TriggerAction
from tests.test_classes import *

from rtypes.dataframe.trigger_manager import TriggerManager

import unittest


####################################################
########## Classes Used in Triggers Here ###########
####################################################


class Customer(object):
    pass


####################################################
################### Triggers Here ##################
####################################################


def create_triggers():

    @trigger(Customer, TriggerTime.before, TriggerAction.create, 1)
    def before_create(dataframe, new, old, current):
        print("[BEFORE create] - Procedure Executed")

    @trigger(Customer, TriggerTime.after, TriggerAction.create, 1)
    def after_create(dataframe, new, old, current):
        print("[AFTER create] -- Procedure Executed")

    @trigger(Customer, TriggerTime.before, TriggerAction.read, 1)
    def before_read(dataframe, new, old, current):
        print("[BEFORE READ] --- Procedure Executed")

    @trigger(Customer, TriggerTime.after, TriggerAction.read, 1)
    def after_read(dataframe, new, old, current):
        print("[AFTER READ] ---- Procedure Executed")

    @trigger(Customer, TriggerTime.before, TriggerAction.update, 1)
    def before_update(dataframe, new, old, current):
        print("[BEFORE UPDATE] - Procedure Executed")

    @trigger(Customer, TriggerTime.after, TriggerAction.update, 1)
    def after_update(dataframe, new, old, current):
        print("[AFTER UPDATE] -- Procedure Executed")

    @trigger(Customer, TriggerTime.before, TriggerAction.delete, 1)
    def before_delete(dataframe, new, old, current):
        print("[BEFORE DELETE] - Procedure Executed")

    @trigger(Customer, TriggerTime.after, TriggerAction.delete, 1)
    def after_delete(dataframe, new, old, current):
        print("[AFTER DELETE] -- Procedure Executed")

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
                TM.execute_trigger(Customer,time, action, "dataframe",
                                   "new", "old", "current"))
    
    def test_trigger_manager_remove(self):
        TM = TriggerManager()

        TM.add_triggers(create_triggers())

        for time, action in trigger_kinds_seprate():
            self.assertTrue(TM.trigger_map[Customer][time + action] != [])

        for trigger in create_triggers():
            TM.remove_trigger(trigger)

        for time, action in trigger_kinds_seprate():
            self.assertTrue(TM.trigger_map[Customer][time + action] == [])
