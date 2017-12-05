import datetime

from rtypes.pcc.utils.metadata import Metadata
from rtypes.pcc.utils.enums import Record


def build_required_attrs(cooked_cls, category, parents, projection_dims=None):
    if (hasattr(cooked_cls, "__rtypes_metadata__")
            and cooked_cls.__rtypes_metadata__.name == (
                cooked_cls.__module__ + "." + cooked_cls.__name__)):
        cooked_cls.__rtypes_metadata__ = (
            cooked_cls.__rtypes_metadata__.add_data(
                cooked_cls, category, parents, projection_dims))
    else:
        if (len(set(cooked_cls.mro()).difference(
                set([cooked_cls, object]))) >= 1 and hasattr(
                    cooked_cls.mro()[1], "__rtypes_metadata__")):
            inherited_cls_metadata = cooked_cls.mro()[1].__rtypes_metadata__
            if inherited_cls_metadata.final_category == category:
                cooked_cls.__rtypes_metadata__.aliases.add(
                    cooked_cls.__module__ + "." + cooked_cls.__name__)
                return
        cooked_cls.__rtypes_metadata__ = Metadata(
            cooked_cls, category, parents, projection_dims)


class ValueParser(object):
    @staticmethod
    def get_obj_type(obj):
        # both iteratable/dictionary + object type is messed up. Won't work.
        try:
            if hasattr(obj, "__rtypes_metadata__"):
                return Record.FOREIGN_KEY
            if isinstance(obj, dict):
                return Record.DICTIONARY
            if hasattr(obj, "__iter__"):
                return Record.COLLECTION
            if isinstance(obj, int) or isinstance(obj, long):
                return Record.INT
            if isinstance(obj, float):
                return Record.FLOAT
            if isinstance(obj, str) or isinstance(obj, unicode):
                return Record.STRING
            if isinstance(obj, bool):
                return Record.BOOL
            if obj == None:
                return Record.NULL
            if (isinstance(obj, datetime.date)
                   or isinstance(obj, datetime.datetime)):
                return Record.DATETIME
            if hasattr(obj, "__dict__"):
                return Record.OBJECT
        except TypeError, e:
            return -1
        return -1