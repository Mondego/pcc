'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from pcc.attributes import spacetime_property
from pcc.recursive_dictionary import RecursiveDictionary
from pcc.create import create, change_type
from pcc.parameter import ParameterMode
from uuid import uuid4
from threading import Thread
from Queue import Queue
from Queue import Empty

import pcc.dataframe_changes.IDataframeChanges as df_repr
from pcc.dataframe_changes.IDataframeChanges import Event, Record
from object_manager import ObjectManager
from type_manager import TypeManager
from change_manager import ChangeManager

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
    
    def __init__(self, name = str(uuid4())):
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

        # Flag to see if the dataframe should keep a record of all changes.
        # Can be used to synchnronize between dataframes.
        self.start_recording = False
        
    ####### TYPE MANAGEMENT METHODS #############
    def add_type(self, tp, tracking=False):
        pairs_added = self.type_manager.add_type(
            tp, 
            tracking, 
            self.object_manager.adjust_pcc,  
            self.change_manager.report_dim_modification,
            self.object_manager.create_records_for_dim_modification)
        self.object_manager.create_tables(pairs_added)

    def add_types(self, types, tracking=False):
        pairs_added = self.type_manager.add_types(
            types, 
            tracking, 
            self.object_manager.adjust_pcc, 
            self.change_manager.report_dim_modification,
            self.object_manager.create_records_for_dim_modification)
        self.object_manager.create_tables(pairs_added)

    def has_type(self, tp):
        self.type_manager.has_types(tp)

    def reload_types(self, types):
        # TODO
        self.type_manager.reload_types(types)

    def remove_type(self, tp):
        # TODO
        self.type_manager.remove_type(tp)

    def remove_types(self, types):
        # TODO
        self.type_manager.remove_types(types)
    #############################################

    ####### OBJECT MANAGEMENT METHODS ###########
    def append(self, tp, obj):
        if (self.type_manager.check_for_new_insert(tp)
            and self.type_manager.check_obj_type_for_insert(tp, obj)):
            tp_obj = self.type_manager.get_requested_type(tp)
            records = self.object_manager.append(tp_obj, obj)
            self.change_manager.add_records(records)

    def extend(self, tp, objs):
        if (self.type_manager.check_for_new_insert(tp)):
            tp_obj = self.type_manager.get_requested_type(tp)
            for obj in objs:
                # One pass through objects to see if the types match.
               self.type_manager.check_obj_type_for_insert(tp, obj)
            records = self.object_manager.extend(tp_obj, objs)
            self.change_manager.add_records(records)

    def get(self, tp, oid = None, parameters = None):
        # TODO: Add checks for tp
        if tp.__realname__ not in self.type_manager.observing_types:
            raise TypeError("%s Type is not registered for observing." % tp.__realname__)
        tp_obj = self.type_manager.get_requested_type(tp)
        return self.object_manager.get(tp_obj, parameters) if oid == None else self.object_manager.get_one(tp_obj, oid, parameters)

    def delete(self, tp, obj):
        # TODO: Add checks for tp
        tp_obj = self.type_manager.get_requested_type(tp)
        records = self.object_manager.delete(tp_obj, obj)
        self.change_manager.add_records(records)

    def delete_all(self, tp):
        # TODO: Add checks for tp
        tp_obj = self.type_manager.get_requested_type(tp)
        records = self.object_manager.delete_all(tp_obj)
        self.change_manager.add_records(records)

    def clear_joins(self):
        for tp_obj in self.type_manager.get_join_types():
            _ = self.object_manager.delete_all(tp_obj)
    #############################################

    ####### CHANGE MANAGEMENT METHODS ###########

    @property
    def start_recording(self): return self.change_manager.startrecording

    @start_recording.setter
    def start_recording(self, v): self.change_manager.startrecording = v

    def apply_changes(self, changes, except_app = None, track = True):
        if "gc" not in changes:
            return

        applied_records, pcc_change_records, deletes = self.object_manager.apply_changes(changes)
        self.object_manager.add_buffer_changes(changes, deletes)
        if track:
            self.change_manager.add_records(applied_records, pcc_change_records, except_app)

    def get_record(self):
        return self.change_manager.get_record()

    def clear_record(self):
        return self.change_manager.clear_record()

    def connect_app_queue(self, app_queue):
        return self.type_manager.get_impures_in_types(app_queue.types), self.change_manager.add_app_queue(app_queue)

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