'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from __future__ import absolute_import
from uuid import uuid4

from rtypes.dataframe.object_manager import ObjectManager
from rtypes.dataframe.type_manager import TypeManager
from rtypes.dataframe.change_manager import ChangeManager
from rtypes.dataframe.application_queue import ApplicationQueue
from rtypes.dataframe.trigger_manager import TriggerManager
from rtypes.pcc.triggers import TriggerAction, TriggerTime, BlockAction

BASE_TYPES = set([])


class dataframe(object):
    # Control Flows for dataframe
    #.1 Type Management
    ##.1a Adding a type
    ##.1b Reloading a type
    ##.1c Deleting a type

    #.2 Object Management
    ##.2a Adding objects of type
    ##.2b Calculating dependent pure types from any change. (Master type only)
    ##.2c Calculating pure dependent types.
    ##.2d Get objects of type
    ##.2e Track changes to objects
    ###.2ea Uses 3a
    ##.f Delete objects

    #.3 Record Management
    ##.3a Record changes to objects.
    ##.3b Clear changes to objects.
    ##.3c Allow object serialization to records.
    ##.3d Record buffers (new, mod, and deleted).
    ##.3e clear buffers

    #.4 Trigger Management
    
    def __init__(self, name=str(uuid4()), external_db=None):
        # PCCs to be calculated only if it is in Master Mode.
        self.calculate_pcc = True

        # Unique ID for this dataframe.
        self.name = name

        # The object that deals with type management
        self.type_manager = TypeManager()

        # The object that deals with object management
        self.object_manager = ObjectManager(self.type_manager)

        # The object that deals with record management
        self.change_manager = ChangeManager()

        self.external_db = external_db

        # The object that deals with trigger management
        self.trigger_manager = TriggerManager()

        # Flag to see if the dataframe should keep a record of all changes.
        # Can be used to synchnronize between dataframes.
        self.start_recording = external_db is not None

        self.external_db_queue = ApplicationQueue(
            "external_db", list(), self, all=True)
        
    ####### TYPE MANAGEMENT METHODS #############
    def add_type(self, tp, tracking=False):
        pairs_added = self.type_manager.add_type(
            tp, 
            tracking, 
            self.object_manager.adjust_pcc,  
            self.change_manager.report_dim_modification,
            self.object_manager.create_records_for_dim_modification)
        records = self.object_manager.create_tables(pairs_added)
        self.external_db_queue.add_types(pairs_added)
        self.change_manager.add_records(records)

    def add_types(self, types, tracking=False):
        pairs_added = self.type_manager.add_types(
            types, 
            tracking, 
            self.object_manager.adjust_pcc, 
            self.change_manager.report_dim_modification,
            self.object_manager.create_records_for_dim_modification)
        records = self.object_manager.create_tables(pairs_added)
        self.external_db_queue.add_types(pairs_added)
        self.change_manager.add_records(records)

    def has_type(self, tp):
        self.type_manager.has_type(tp)

    def reload_types(self, types):
        self.type_manager.reload_types(types)

    def remove_type(self, tp):
        self.type_manager.remove_type(tp)

    def remove_types(self, types):
        self.type_manager.remove_types(types)

    #############################################

    ####### OBJECT MANAGEMENT METHODS ###########
    def append(self, tp, obj):
        try:
            if (self.type_manager.check_for_new_insert(tp)
                    and self.type_manager.check_obj_type_for_insert(tp, obj)):
                tp_obj = self.type_manager.get_requested_type(tp)
                self.trigger_manager.execute_trigger(
                    tp_obj.type, TriggerTime.before, TriggerAction.create,
                    self, obj, None, None)
                records = self.object_manager.append(tp_obj, obj)
                self.change_manager.add_records(records)
                self.trigger_manager.execute_trigger(
                    tp_obj.type, TriggerTime.after, TriggerAction.create,
                    self, obj, None, obj)
        except BlockAction:
            pass

    def extend(self, tp, objs):
        try:
            if self.type_manager.check_for_new_insert(tp):
                tp_obj = self.type_manager.get_requested_type(tp)
                for obj in objs:
                    # One pass through objects to see if the types match.
                    self.type_manager.check_obj_type_for_insert(tp, obj)
                    self.trigger_manager.execute_trigger(
                        tp_obj.type, TriggerTime.before, TriggerAction.create,
                        self, obj, None, None)

                    self.change_manager.add_records(self.object_manager.append(tp_obj, obj))
                    self.trigger_manager.execute_trigger(
                        tp_obj.type, TriggerTime.after, TriggerAction.create,
                        self, obj, None, obj)
        except BlockAction:
            pass

    def get(self, tp, oid=None, parameters=None):
        try:
            name = tp.__rtypes_metadata__.name
            if name not in self.type_manager.observing_types:
                raise TypeError(
                    ("%s Type is not registered for observing."
                     % name))
            tp_obj = self.type_manager.get_requested_type(tp)
            self.trigger_manager.execute_trigger(
                tp_obj.type, TriggerTime.before, TriggerAction.read,
                self, None, None, None)
            objs = (self.object_manager.get(tp_obj, parameters)
                    if oid is None else
                    [self.object_manager.get_one(tp_obj, oid, parameters)])
            # Try/Except section prevents after read trigers from interrupting the read
            try:
                if self.trigger_manager.trigger_exists(
                    tp_obj.type, TriggerTime.after, TriggerAction.read):
                    for obj in objs:
                        self.trigger_manager.execute_trigger(
                            tp_obj.type, TriggerTime.after, TriggerAction.read,
                            self, None, obj, obj)
            except BlockAction:
                pass
            return objs[0] if oid is not None else objs
        except BlockAction:
            pass

    def delete(self, tp, obj):
        try:
            # TODO: Add checks for tp
            tp_obj = self.type_manager.get_requested_type(tp)
            self.trigger_manager.execute_trigger(
                tp_obj.type, TriggerTime.before, TriggerAction.delete,
                self, None, obj, obj)
            records = self.object_manager.delete(tp_obj, obj)
            try:
                self.trigger_manager.execute_trigger(
                    tp_obj.type, TriggerTime.after, TriggerAction.delete,
                    self, None, obj, None)
                self.change_manager.add_records(records)
            except BlockAction:
                pass
        except BlockAction:
            pass

    def delete_all(self, tp):
        # TODO: Add checks for tp
        tp_obj = self.type_manager.get_requested_type(tp)
        if tp_obj.name in self.object_map:
            for obj in self.object_map[tp_obj.name].values():
                self.trigger_manager.execute_trigger(
                    tp_obj.type, TriggerTime.before, TriggerAction.delete,
                    self, None, obj, obj)
                self.change_manager.add_records(self.object_manager.delete(tp_obj, obj))
                self.trigger_manager.execute_trigger(
                    tp_obj.type, TriggerTime.after, TriggerAction.delete,
                    self, None, obj, None)

    def clear_joins(self):
        for tp_obj in self.type_manager.get_join_types():
            _ = self.object_manager.delete_all(tp_obj)

##########################################################################################
    def update(self, tp, obj, new_value):
        tp_obj = self.type_manager.get_requested_type(tp)

        """
        This method will be used to call update on dataframe
        """
        pass
##########################################################################################

    #############################################

    ####### CHANGE MANAGEMENT METHODS ###########

    @property
    def start_recording(self):
        return self.change_manager.startrecording

    @start_recording.setter
    def start_recording(self, v):
        self.change_manager.startrecording = v

    def apply_changes(self, changes, except_app=None, track=True):
        if "gc" not in changes:
            return

        applied_records, pcc_change_records, deletes = (
            self.object_manager.apply_changes(changes))
        self.object_manager.add_buffer_changes(changes, deletes)
        if track:
            self.change_manager.add_records(
                applied_records, pcc_change_records, except_app)

    def get_record(self):
        return self.change_manager.get_record()

    def clear_record(self):
        return self.change_manager.clear_record()

    def connect_app_queue(self, app_queue):
        return (
            self.type_manager.get_impures_in_types(
                app_queue.types, all=app_queue.all),
            self.change_manager.add_app_queue(app_queue))

    def convert_to_record(self, results, deleted_oids):
        return self.object_manager.convert_to_records(results, deleted_oids)

    def serialize_all(self):
        return self.change_manager.convert_to_serializable_dict(
            self.object_manager.convert_whole_object_map())

    def get_new(self, tp):
        return self.object_manager.get_new(tp)

    def get_mod(self, tp):
        return self.object_manager.get_mod(tp)

    def get_deleted(self, tp):
        return self.object_manager.get_deleted(tp)

    def clear_all(self):
        return self.object_manager.clear_all()

    def clear_buffer(self):
        return self.object_manager.clear_buffer()

    #############################################
    ####### EXTERNAL DB MANAGEMENT METHODS ######

    def pull(self):
        changes, clear_all = self.external_db.__rtypes_query__(
            [df_tp.type for df_tp in self.type_manager.observing_types])
        if clear_all:
            self.clear_all()
        self.apply_changes(changes, except_app=self.external_db_queue.app_name)

    def push(self):
        changes = self.external_db_queue.get_record()
        self.external_db_queue.clear_record()
        pcc_type_map = {
            name: tp_obj.type
            for name, tp_obj in self.type_manager.name2class.iteritems()}
        self.external_db.__rtypes_write__(changes, pcc_type_map)

    #############################################
    ######## Trigger MANAGEMENT METHODS #########

    def add_trigger(self, trigger_obj):
        """ Method can add a trigger into the trigger_manager into the dataframe """
        self.trigger_manager.add_trigger(trigger_obj)

    def add_triggers(self, trigger_objs):
        """ Method can add multiple triggers into the trigger_manager into the dataframe """
        self.trigger_manager.add_triggers(trigger_objs)

    def remove_trigger(self, trigger_obj):
         """ can remove a trigger from the trigger_manager into the dataframe """
         self.trigger_manager.remove_trigger(trigger_obj)
