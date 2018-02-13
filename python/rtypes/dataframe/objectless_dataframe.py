'''
Create on Feb 27, 2016

@author: Rohan Achar
'''
from __future__ import absolute_import
from uuid import uuid4

from rtypes.dataframe.state_manager import StateManager

BASE_TYPES = set([])


class ObjectlessDataframe(object):
    def __init__(self, name=str(uuid4()), external_db=None):
        # Unique ID for this dataframe.
        self.name = name

        # The object that deals with object management
        self.state_manager = StateManager()

        self.external_db = external_db
        
    def add_types(self, types):
        self.state_manager.add_types(types)

    def add_type(self, tp):
        self.state_manager.add_type(tp)
        
    def apply_changes(self, changes):
        if self.external_db:
            self.external_db.__rtypes_write__(changes)
        return self.state_manager.apply_changes(changes)

    def get_records(self, types_to_code):
        return self.state_manager.get_records(types_to_code)

    def push(self):
        changes = self.external_db_queue.get_record()
        self.external_db_queue.clear_record()
        pcc_type_map = {
            name: tp_obj.type
            for name, tp_obj in self.type_manager.name2class.iteritems()}
        self.external_db.__rtypes_write__(changes, pcc_type_map)

