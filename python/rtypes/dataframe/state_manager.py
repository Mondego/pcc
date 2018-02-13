
import datetime
import dill
from uuid import uuid4
from dateutil import parser
from rtypes.pcc.utils.recursive_dictionary import RecursiveDictionary
from rtypes.dataframe.type_manager import TypeManager
from rtypes.pcc.types.parameter import ParameterMode

from rtypes.dataframe.dataframe_changes import IDataframeChanges as df_repr
from rtypes.dataframe.dataframe_type import object_lock
from rtypes.pcc.create import create, change_type
from rtypes.pcc.utils._utils import ValueParser
from rtypes.pcc.utils.enums import Event, Record
from rtypes.dataframe.type_state import TypeState


#################################################
#### Object Management Stuff (Atomic Needed) ####
#################################################


class StateManager(object):
    def __init__(self):
        # <group key> -> id -> object state.
        # (Might have to make this even better)
        # object state is {"base": base state, "type 1": extra fields etc., ...}
        self.type_to_objstate = dict()

        self.type_to_transformation = dict()
        self.type_manager = TypeManager()
        self.typename_state_map = dict()


    #################################################
    ### Static Methods ##############################
    #################################################


    #################################################
    ### API Methods #################################
    #################################################

    def add_types(self, types):
        pairs = self.type_manager.add_types(types)
        self.create_tables(pairs)
    
    def add_type(self, tp):
        pairs = self.type_manager.add_type(tp)
        self.create_table(tp)
    
    def create_table(self, tpname):
        with object_lock:
            return self.__create_table(tpname)

    def create_tables(self, tpnames_basetype_pairs):
        with object_lock:
            for tpname, _ in tpnames_basetype_pairs:
                self.__create_table(tpname)


    def apply_changes(self, df_changes):
        existing_type_updates = dict()
        if "types" in df_changes:
            types_added, types_deleted = self.__parse_type_changes(
                df_changes["types"])
            self.type_manager.delete_types(types_deleted)
            self.remove_tables(types_deleted)
            pairs = self.type_manager.add_types(types_added)
            self.create_tables(pairs)
        if "gc" in df_changes:
            existing_type_updates = self.__parse_changes(df_changes["gc"])
        return existing_type_updates

    def clear_all(self):
        self.type_to_objstate.clear()

    def remove_tables(self, types):
        for tp in types:
            if tp.name in self.type_to_objstate:
                del self.type_to_objstate[tp.name]

    #################################################
    ### Private Methods #############################
    #################################################

    def __parse_type_changes(self, type_changes):
        types_added = set()
        types_deleted = set()
        for tpname, changes in type_changes.iteritems():
            if changes["status"] == Event.New:
                types_added.add(dill.loads(changes["serial"]))
            elif (changes["status"] == Event.Delete
                  and tpname in self.type_manager.name2type):
                types_deleted.add(self.type_manager.name2type[tpname])
        return types_added, types_deleted

    def __parse_changes(self, df_changes):
        '''
        all_changes is a dictionary in this format
        
        { <- gc stands for group changes
            "group_key1": { <- Group key for the type EG: Car
                "object_key1": { <- Primary key of object
                    "dims": { <- optional
                        "dim_name": { <- EG "velocity"
                            "type": <Record type, EG Record.INT.
                                        Enum values can be found in
                                        Record class>
                            "value": <Corresponding value,
                                        either a literal, or a collection,
                                        or a foreign key format.
                                        Can be optional if type is Null
                        },
                        <more dim records> ...
                    },
                    "types": {
                        "type_name": <status of type. Enum values can be
                                        found in Event class>,
                        <More type to status mappings>...
                    }
                },
                <More object change logs> ...
            },
            <More group change logs> ...
        }
        '''

        tm = self.type_manager
        final_applied_changes = dict()
        for groupname, group_changes in df_changes.iteritems():
            try:
                group_type = tm.get_requested_type_from_str(groupname)
            except TypeError:
                continue
            final_applied_changes[groupname] = dict()
            for oid, obj_changes in group_changes.items():
                if "version" not in obj_changes:
                    continue
                obj_changelist = self.type_to_objstate.setdefault(
                    group_type, RecursiveDictionary()).setdefault(
                        oid, RecursiveDictionary())

                inc_version = obj_changes["version"]
                last_known_version = obj_changes["last_known_version"]
                current_version = self.type_to_objstate[group_type][oid].lastkey()
                if current_version is None:
                    applied_changes = self.__parse_as_new(
                        group_type, oid, obj_changes, obj_changelist)
                else:
                    applied_changes = self.__process_as_updated(
                        group_type, oid, obj_changes, obj_changelist, last_known_version)
                final_applied_changes[groupname][oid] = applied_changes
        self.resolve_rtypes_changes(final_applied_changes)
            
    def __parse_as_new(self, group_type, oid, obj_changes, obj_changelist):
        self.type_to_objstate[group_type][oid][obj_changelist] = obj_changes
        self.type_to_transformation[group_type][oid] = dict()
        return obj_changes

    def __parse_as_updated(self, group_type, oid, obj_changes, obj_changelist, last_known_version):
        all_changes = self.type_to_objstate[group_type][oid][last_known_version:][1:]
        current_master_changelist = self.type_to_objstate[group_type][oid].lastkey()
        conflated_changes = self.conflate_changes(group_type, all_changes)
        self.resolve_changes(
            group_type, conflate_changes, obj_changes,
            obj_changelist, self.type_to_transformation[group_type][oid],
            current_master_changelist)
        master_changes = self.conflate_changes(
            group_type,
            self.type_to_objstate[group_type][oid][current_master_changelist:][1:])
        return master_changes

    def conflate_changes(self, group_name, changes):
        final_change = dict()
        for change in changes:
            final_change = self.merge_change(group_name, final_change, change)
        return final_change

    def merge_change(self, group_name, dest, src):
        if not dest:
            dest.update(src)
            return dest
        dest_event = dest["types"][group_name]
        src_event = src["types"][group_name]
        if dest_event == Event.Delete:
            if src_event != Event.New:
                # If the previous record is Delete,
                # and the next record is not new,
                # delete the next record.
                # This case should not happen
                # unless src_event is Event.Modification
                return dest
            # This case could easily happen.
            # Do not return src directly, always a copy.
            return dict(src)
        if src_event == Event.Delete:
            return dict(src)
        if dest_event == Event.Modification and src_event == Event.New:
            # Error. This case is a merge conflict.
            # Should already have been taken care off.
            # How is it there?
            raise RuntimeError("Bad condition reached. Investigate")
        if src_event == Event.Modification:
            # Should be the only condition left to merge on.
            if "dims" not in src:
                # Nothing to merge actually.
                return dest
            dest.setdefault("dims", RecursiveDictionary()).update(src["dims"])
            # The type map should remain whatever dest type map is.
            # New should remain new, modification should remain modification.
            return dest
        raise RuntimeError("Why did it reach this line. It should not be here.")

    def resolve_changes(
            self, group_type, old_change, new_change,
            changelist, transformation_group, master_changelist):
        old_event = old_change["types"][group_type]
        new_event = new_change["types"][group_type]
        if old_event == Event.Delete:
            if new_event == Event.Delete:
                # The changes conflict, however, there is
                # nothing to resolve, both events do the same thing
                # Add a NoOp transformation from new_change to end of master.
                
                transformation_group[changelist] = {
                    "resolved_changelist": master_changelist,
                    "NOOP": True}
                return
            # Change is a conflict, add a delete transformation
            # from new change to end of master.
            transformation_group[changelist] = {
                "resolved_changelist": master_changelist,
                "NOOP": False,
                "transformation": old_change
            }
            return
        if old_event == Event.New:
            if new_event == Event.New:
                # The changes conflict.
                # Add Modifiy transformation from new change to end of
                # master. The new object is essentially deleted.
                # Mode of resolution is accept master.
                transformation_group[changelist] = {
                    "resolved_changelist": master_changelist,
                    "NOOP": False,
                    "transformation": old_change
                }
            # The changes conflict. Likely scenario is that
            # the obj was deleted and recreated in the master.
            # This means that the inc changes were on another
            # obj which does not exist now. This inc obj should
            # be deleted and the new obj should take its place.
            # Think this is the safest option.
            transformation_group[changelist] = {
                "resolved_changelist": master_changelist,
                "NOOP": False,
                "transformation": old_change
            }
        if old_event == Event.Modification:
            if new_event == Event.New:
                # If this happens, it means that the 
                # dataframe deleted the obj,
                # and created a new copy.
                # Mode is accept incoming? <--Think about it.
                self.type_to_objstate[group_type][oid][str(uuid.uuid4())] = {
                    "types": {
                        group_type: Event.Delete
                    }
                }
                self.type_to_objstate[group_type][oid][changelist] = new_change
            if new_event == Event.Delete:
                # Merge, no conflict.
                # Just add the delete event to the master.
                self.type_to_objstate[group_type][oid][changelist] = new_change
            if new_event == Event.Modification:
                # Merge dimension by dimension,
                # There might be a transformation
                # between inc and new node.
                # Added to master: (old - inc) + NoOp
                # Added to fork: (inc - old) pointing to NoOp node.
                
                # This is the transformation for master -> new state
                new_changelist = str(uuid.uuid4())
                if "dims" in new_change and len(new_change["dims"]) > 0:
                    self.type_to_objstate[group_type][oid][new_changelist] = new_change
                else:
                    self.type_to_objstate[group_type][oid][new_changelist] = {
                        "types": {
                            group_type: Event.Modification
                        }
                    }
                if "dims" in old_change and len(old_change["dims"]) > 0:
                    new_changes_diff = {
                        "dims": {
                            dim: old_change["dims"][dim]
                            for dim in old_change["dims"]
                            if dim not in new_change.setdefault("dims", RecursiveDictionary())
                        },
                        "types": {
                            group_type: Event.Modification
                        }
                    }
                    if len(new_changes_diff["dims"]) > 0:
                        transformation_group[changelist] = {
                            "resolved_changelist": new_changelist,
                            "NOOP": True
                        }
                    else:
                        transformation_group[changelist] = {
                            "resolved_changelist": new_changelist,
                            "NOOP": False,
                            "transformation": new_changes_diff
                        }
        return

    def resolve_rtypes_changes(self, applied_changes):
        for group_name in applied_changes:
            group_tp_obj = self.type_manager.get_requested_type_from_str(group_name)
            dim_to_type = group_tp_obj.dim_to_groupmember_trigger
            for oid, obj_changes in applied_changes[group_name]:
                if obj_changes["types"][group_name] == Event.Delete:
                    for gm in group_tp_obj.group_members:
                        if gm.name in self.type_to_objstate and oid in self.type_to_objstate[gm.name]:
                            self.type_to_transformation.setdefault(
                                

                            )
                else:
                    pass

    def __create_table(self, tpname):
        tp_obj = self.type_manager.get_requested_type_from_str(tpname)
        self.type_to_objstate.setdefault(tpname, TypeState(tp_obj))


    def get_records(self, type_to_stamp):
        final_record = RecursiveDictionary()
        for tp in type_to_stamp:
            final_record.rec_update(
                TypeState.merge_state(
                    self.type_to_objstate[tp][type_to_stamp[tp]:]))
        return final_record
