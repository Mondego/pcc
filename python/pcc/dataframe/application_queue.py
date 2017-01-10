from pcc.recursive_dictionary import RecursiveDictionary
from pcc.dataframe_changes.IDataframeChanges import Event
from threading import Thread, RLock
from Queue import Empty
import pcc.dataframe_changes.IDataframeChanges as df_repr


class ApplicationQueue(object):
    def __init__(self, name, types, dataframe):
        self.app_name = name
        self.known_objects = RecursiveDictionary()
        self.current_record = RecursiveDictionary()
        self.types = types
        self.dataframe = dataframe
        self.registered_impures, self.queue = self.dataframe.connect_app_queue(self)
        self.lock = RLock()
        
    def merge_records(self, records):
        with self.lock:
            for rec in records:
                event, tpname, groupname, oid, dim_change, full_obj = (
                    rec.event, rec.tpname, rec.groupname, rec.oid, rec.dim_change, rec.full_obj)
                obj_changes = self.current_record.setdefault(groupname, RecursiveDictionary()).setdefault(oid, RecursiveDictionary())
                type_changes = obj_changes.setdefault("types", RecursiveDictionary())
                if tpname in type_changes and type_changes[tpname] == Event.Delete:
                    continue
                is_known = tpname in self.known_objects and oid in self.known_objects[tpname]
                if event == Event.New:
                    type_changes[tpname] = event
                    obj_changes.setdefault("dims", RecursiveDictionary()).rec_update(full_obj)
                elif event == Event.Modification:
                    type_changes[tpname] = event if is_known else Event.New
                    obj_changes.setdefault("dims", RecursiveDictionary()).rec_update(dim_change if is_known else full_obj)
                elif event == Event.Delete:
                    type_changes[tpname] = event
            
    def get_record(self):
        while True:
            try:
                records = self.queue.get_nowait()
                self.merge_records(records)
            except Empty:
                break
        objmap = self.fetch_impure_types()

        return ApplicationQueue.__convert_to_serializable_dict(
            self.set_known_objects(
                self.merge_impure_record(self.current_record, objmap)))

    def clear_record(self):
        self.current_record = RecursiveDictionary()

    def fetch_impure_types(self):
        objmap = RecursiveDictionary()
        for tp in self.registered_impures:
            objmap[tp] = self.dataframe.get(tp)
        return objmap

    def merge_impure_record(self, current_record, objmap):
        results = RecursiveDictionary()
        for tp in self.registered_impures:
            results[tp] = self.dataframe.get(tp)
        deleted = RecursiveDictionary()

        for tp in self.registered_impures:
            tpname = tp.__realname__
            if tpname in self.known_objects:
                obj_oids = self.known_objects[tpname]
                next_oids = set([obj.__primarykey__ for obj in results[tp]])
                deleted_oids = obj_oids.difference(next_oids)
                deleted[tpname] = deleted_oids

        impure_results = self.dataframe.convert_to_record(results, deleted)
        for group_name, grpchanges in impure_results.items():
            if group_name not in current_record:
                current_record[group_name] = grpchanges
                continue
            for oid, obj_changes in grpchanges.items():
                if oid not in current_record[group_name]:
                    current_record[group_name][oid] = obj_changes
                    continue

                for tpname, event in obj_changes["types"].items():
                    if tpname in current_record[group_name][oid]["types"]:
                        existing_event = current_record[group_name][oid]["types"][tpnames]
                    else:
                        existing_event = event
                    if existing_event == Event.Delete or existing_event == Event.Modification:
                        continue
                    current_record[group_name][oid].setdefault("dims", RecursiveDictionary()).rec_update(obj_changes["dims"])
                    current_record[group_name][oid]["types"][tpname] = existing_event
        return current_record

    def set_known_objects(self, current_record):
        for groupname, grp_changes in current_record.items():
             for oid, obj_changes in grp_changes.items():
                 for tpname, status in obj_changes["types"].items():
                     if status == Event.New:
                         self.known_objects.setdefault(tpname, set()).add(oid)
                     elif status == Event.Delete:
                         self.known_objects[tpname].remove(oid)

        return current_record

    @staticmethod
    def __convert_to_serializable_dict(current_record):
        df_changes = df_repr.DataframeChanges_Base()
        df_changes.ParseFromDict({"gc": current_record})
        return df_changes
