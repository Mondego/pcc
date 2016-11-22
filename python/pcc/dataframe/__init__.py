'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from attributes import spacetime_property
from recursive_dictionary import RecursiveDictionary
from create import create, change_type
from parameter import ParameterMode
from uuid import uuid4
from multiprocessing import RLock, Queue

import dataframe_changes.IDataframeChanges as df_repr
from dataframe_changes.IDataframeChanges import Event, Record
from object_manager import ObjectManager
from type_manager import TypeManager
from record_manager import RecordManager
from cache_manager import CacheManager

BASE_TYPES = set([])

class dataframe_new(object):
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

    def __init__(self, name = str(uuid4()), lock = RLock()):
        # PCCs to be calculated only if it is in Master Mode.
        self.calculate_pcc = True

        # Flag to see if the dataframe should keep a record of all changes.
        # Can be used to synchnronize between dataframes.
        self.start_recording = False

        # Unique ID for this dataframe.
        self.name = name

        # The lock for looking at typemaps, current_state, and object_map.
        # NOT for records.
        self.lock = lock
        
        # The object that deals with type management
        self.type_manager = TypeManager()

        # The object that deals with object management
        self.object_manager = ObjectManager()

        # The object that deals with record management
        self.record_manager = RecordManager()

        # The object that deals with external "viewers" of records
        self.cache_manager = CacheManager()

    ####### TYPE MANAGEMENT METHODS #############
    def add_type(self, tp, tracking=False):
        pairs_added = self.type_manager.add_type(
            tp, 
            tracking, 
            self.object_manager.adjust_pcc, 
            self.record_manager.report_dim_modification)
        self.object_manager.create_tables(pairs_added)

    def add_types(self, types, tracking=False):
        pairs_added = self.type_manager.add_types(
            types, 
            tracking, 
            self.object_manager.adjust_pcc, 
            self.record_manager.report_dim_modification)
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
            if self.start_recording:
                self.record_manager.add_records(records)
            self.cache_manager.add_records(records)

    def extend(self, tp, objs):
        if (self.type_manager.check_for_new_insert(tp)):
            tp_obj = self.type_manager.get_requested_type(tp)
            for obj in objs:
                # One pass through objects to see if the types match.
               self.type_manager.check_obj_type_for_insert(tp, obj)
            records = self.object_manager.extend(tp_obj, objs)
            if self.start_recording:
                self.record_manager.add_records(records)
            self.cache_manager.add_records(records)

    def get(self, tp, oid = None, parameters = None):
        # TODO: Add checks for tp
        tp_obj = self.type_manager.get_requested_type(tp)
        if oid != None:
            return self.object_manager.get_one(tp_obj, oid, parameters)
        return self.object_manager.get(tp_obj, parameters)

    def delete(self, tp, obj):
        # TODO: Add checks for tp
        tp_obj = self.type_manager.get_requested_type(tp)
        records = self.object_manager.delete(tp_obj, obj)
        if self.start_recording:
            self.record_manager.add_records(records)
        self.cache_manager.add_records(records)

    def delete_all(self, tp):
        tp_obj = self.type_manager.get_requested_type(tp)
        records = self.object_manager.delete_all(tp_obj)
        if self.start_recording:
            self.record_manager.add_records(records)
        self.cache_manager.add_records(records)

    #############################################
