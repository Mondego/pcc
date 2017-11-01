from rtypes.pcc.utils.metadata import Metadata


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
