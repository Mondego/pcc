from pcc.recursive_dictionary import RecursiveDictionary

class RepeatedProperty(property):
    def extender(self, f):
        self.extend = f
        return self

class DataframeChanges_Base(RecursiveDictionary):
    @RepeatedProperty
    def group_changes(self):
        return self.setdefault("gc", RecursiveDictionary()).values()

    @group_changes.setter
    def group_changes(self, values):
        self.setdefault("gc", RecursiveDictionary()).clear()
        for gc in values:
            self["gc"][gc.group_key] = gc

    @group_changes.extender
    def group_changes(self, values):
        for gc in values:
            self["gc"][gc.group_key] = gc

    @RepeatedProperty
    def types(self):
        return self.setdefault("types", [])

    @types.setter
    def types(self, values):
        self["types"] = values
        
    @types.extender
    def types(self, values):
        self["types"].extend(values)

    def __init__(self):
        pass

    def SerializeToDict(self):
        return self

    def ParseFromDict(self, parsed_dict):
        gcs = []
        for gc in parsed_dict["gc"]:
            gc_obj = GroupChanges()
            gc_obj.ParseFromDict(parsed_dict[gc])
            gcs.append(gc_obj)
        self.group_changes.extend(gcs) 
        if "types" in parsed_dict:
            types = []
            for tc in parsed_dict["gc"]:
                tc_obj = DFType()
                tc_obj.ParseFromDict(parsed_dict[tc])
                types.append(tc_obj)
            self.types.extend(types) 


class GroupChanges(RecursiveDictionary):
    @RepeatedProperty
    def object_changes(self):
        return self.values()

    @object_changes.setter
    def object_changes(self, values):
        self.clear()
        for oc in values:
            self[oc.object_key] = oc

    @object_changes.extender
    def object_changes(self, values):
        for oc in values:
            self[oc.object_key] = oc

    def __init__(self):
        pass

    def ParseFromDict(self, parsed_dict):
        ocs = []
        for oc_key in parsed_dict:
            oc_obj = ObjectChanges()
            oc_obj.ParseFromDict(parsed_dict[oc_key])
            ocs.append(oc_obj)
        self.object_changes.extend(ocs)

class ObjectChanges(RecursiveDictionary):
    @RepeatedProperty
    def dimension_changes(self):
        return self["dims"]

    @dimension_changes.setter
    def dimension_changes(self, values):
        self["dims"] = values

    @dimension_changes.extender
    def dimension_changes(self, values):
        self["dims"].extend(values)

    @RepeatedProperty
    def type_changes(self):
        return self["types"].values()

    @type_changes.setter
    def type_changes(self, values):
        self.setdefault("types", RecursiveDictionary()).clear()
        for tc in values:
            self.setdefault("types", RecursiveDictionary())[tc.type.name] = tc

    @type_changes.extender
    def type_changes(self, values):
        for tc in values:
            self.setdefault("types", RecursiveDictionary()).rec_update(tc)

    def __init__(self):
        self["dims"] = RecursiveDictionary()
        self["types"] = RecursiveDictionary()

    def ParseFromDict(self, parsed_dict):
        if "dims" in parsed_dict:
            dims = []
            for dim_key in parsed_dict["dims"]:
                dc_obj = DimensionChanges()
                dc_obj.ParseFromDict(dim_key, parsed_dict["dims"][dim_key])
                dims.append(dc_obj)
            self.dimension_changes = dims
        if "types" in parsed_dict:
            tps = []
            for tc_key in parsed_dict["types"]:
                tc_obj = TypeChanges()
                tc_obj.ParseFromDict(tc_key, parsed_dict["types"][tc_key])
                tps.append(tc_obj)
            self.type_changes = tps

class DimensionChanges(RecursiveDictionary):
    @property
    def value(self):
        return self.setdefault(self.dimension_name, Record())

    @value.setter
    def value(self, rec):
        self[self.dimension_name] = rec

    def __init__(self):
        self.dimension_name = None

    def ParseFromDict(self, name, record_dict):
        self.dimension_name = name
        rec = Record()
        rec.ParseFromDict(record_dict)
        self.value = rec


class Event(object):
    Delete = 0
    New = 1
    Modification = 2

class DFType(RecursiveDictionary):
    @property
    def name(self): return self["name"] if "name" in self else None
    @name.setter
    def name(self, n): self["name"] = n

    @property
    def type_pickled(self): return self["type_pickled"] if "type_pickled" in self else None
    @type_pickled.setter
    def type_pickled(self, n): self["type_pickled"] = n

    def __init__(self):
        self.name = None
        self.type_pickled = None

    def ParseFromDict(self, d):
        self.rec_update(d)

    def IsInitialized(self):
        if self.name:
            return True
        return False

class TypeChanges(RecursiveDictionary):
    @property
    def event(self):
        return self[self.type.name] if self.type.name in self else None

    @event.setter
    def event(self, status_enum):
        self[self.type.name] = status_enum   

    def __init__(self):
        self.type = DFType()

    def ParseFromDict(self, tp, status):
        self.type.name = tp
        self.event = status

class Record(RecursiveDictionary):
    INT = 1
    FLOAT = 2
    STRING = 3
    BOOL = 4
    NULL = 5

    COLLECTION = 10
    DICTIONARY = 11

    OBJECT = 12
    FOREIGN_KEY = 13

    @property
    def record_type(self):
        return self["type"] if "value" in self else Record.NULL

    @record_type.setter
    def record_type(self, r_type):
        self["type"] = r_type

    @property
    def value(self):
        return self.setdefault("value", Value())

    @value.setter
    def value(self, v):
        self["value"] = v

    def ParseFromDict(self, rec_dict):
        self.record_type = rec_dict["type"]
        val = Value()
        val.ParseFromDict(self.record_type, rec_dict["value"])
        self.value = val

class Value(RecursiveDictionary):
    class Pair(RecursiveDictionary):
        @property
        def key(self):
            return self.setdefault("key", Record())

        @key.setter
        def key(self, key_record):
            self["key"] = key_record

        @property
        def value(self):
            return self.setdefault("value", Record())

        @value.setter
        def value(self, value_record):
            self["value"] = value_record

        def ParseFromDict(self, pair_dict):
            self.key.ParseFromDict(pair_dict["key"])
            self.value.ParseFromDict(pair_dict["value"])

    class Object(RecursiveDictionary):
        @property
        def type(self):
            return self.setdefault("type", DFType())

        @type.setter
        def type(self, tp):
            self["type"] = tp

        @RepeatedProperty
        def object_map(self):
            return self.setdefault("object_map", [])

        @object_map.setter
        def object_map(self, values):
            self["object_map"] = values
        
        @object_map.extender
        def object_map(self, values):
            self["object_map"].extend(values)

        def ParseFromDict(self, obj_dict):
            self.type.ParseFromDict(obj_dict["type"])
            pairs = []
            for pc in obj_dict["object_map"]:
                pc_obj = Value.Pair()
                pc_obj.ParseFromDict(pc)
                pairs.append(pc_obj)
            self.object_map = pairs

        
    class ForeignKey(RecursiveDictionary):
        @property
        def group_key(self): return self["group_key"]
        
        @group_key.setter
        def group_key(self, key): self["group_key"] = key

        @property
        def actual_type(self): return self.setdefault("actual_type", DFType())
        
        @actual_type.setter
        def actual_type(self, key): self["actual_type"] = key

        @property
        def object_key(self): return self["object_key"]
        
        @object_key.setter
        def object_key(self, key): self["object_key"] = key

        def ParseFromDict(self, parsed_dict):
            self.group_key = parsed_dict["group_key"]
            self.object_key = parsed_dict["object_key"]
            self.actual_type.ParseFromDict(parsed_dict["actual_type"])

    @property
    def int_value(self): return self["value"] if "value" in self else None

    @int_value.setter
    def int_value(self, v): self["value"] = v

    @property
    def bool_value(self): return self["value"] if "value" in self else None

    @bool_value.setter
    def bool_value(self, v): self["value"] = v

    @property
    def str_value(self): return self["value"] if "value" in self else None

    @str_value.setter
    def str_value(self, v): self["value"] = v

    @property
    def float_value(self): return self["value"] if "value" in self else None

    @float_value.setter
    def float_value(self, v): self["value"] = v

    @property
    def object(self): return self.setdefault("value", Value.Object())

    @object.setter
    def object(self, v): self["value"] = v

    @property
    def foreign_key(self): return self.setdefault("value", Value.ForeignKey())

    @foreign_key.setter
    def foreign_key(self, v): self["value"] = v

    @RepeatedProperty
    def collection(self):
        return self.setdefault("value", [])

    @collection.setter
    def collection(self, values):
        self["value"] = values
        
    @collection.extender
    def collection(self, values):
        self.setdefault("value", []).extend(values)

    @RepeatedProperty
    def map(self):
        return self.setdefault("value", [])

    @map.setter
    def map(self, values):
        self["value"] = values
        
    @map.extender
    def map(self, values):
        self.setdefault("value", []).extend(values)

    def ParseFromDict(self, rec_type, value_dict):
        if rec_type == Record.INT:
            self.int_value = long(value_dict)
        if rec_type == Record.FLOAT:
            self.float_value = float(value_dict)
        if rec_type == Record.STRING:
            self.str_value = value_dict
        if rec_type == Record.BOOL:
            self.bool_value = value_dict
        if rec_type == Record.COLLECTION:
            recs = []
            for rec_dict in value_dict:
                rec = Record()
                rec.ParseFromDict(rec_dict)
                recs.append(rec)
            self.collection = recs
        if rec_type == Record.DICTIONARY:
            pairs = []
            for pair_dict in value_dict:
                pair = Value.Pair()
                pair.ParseFromDict(pair_dict)
                pairs.append(pair)
            self.map = pairs
        if rec_type == Record.OBJECT:
            self.object.ParseFromDict(value_dict)
        if rec_type == Record.FOREIGN_KEY:
            self.foreign_key.ParseFromDict(value_dict)
