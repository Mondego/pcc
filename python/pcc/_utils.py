from attributes import spacetime_property, aggregate_property

def build_required_attrs(cooked_cls):
    cooked_cls.__pcc_type__ = True
    cooked_cls.__start_tracking__ = __start_tracking_prop__
    cooked_cls.__realname__ = __realname__ = cooked_cls.__name__
    if hasattr(cooked_cls, "__group_by__"):
        aggrs = set()
        for attr in dir(cooked_cls):
            try:
                if isinstance(getattr(cooked_cls, attr), aggregate_property):
                    aggrs.add(getattr(cooked_cls, attr))
            except AttributeError:
                continue
        if len(aggrs) == 0:
            raise TypeError("Groupby clause is useless without aggregate functions in class %s".format(cooked_cls.__name__))
        cooked_cls.__dimensions__ = aggrs
        cooked_cls.__dimensions_name__ = set([agg._name for agg in aggrs])
        cooked_cls.__primarykey__ = cooked_cls.__group_by__
        cooked_cls.__grouped_pcc__ = True
        return
    cooked_cls.__grouped_pcc__ = False     
    cooked_cls.__dimensions__ = set() if not hasattr(cooked_cls, "__dimensions__") else set(cooked_cls.__dimensions__)
    cooked_cls.__dimensions_name__ = set() if not hasattr(cooked_cls, "__dimensions_name__") else set(cooked_cls.__dimensions_name__)
    cooked_cls.__primarykey__ = None if not hasattr(cooked_cls, "__primarykey__") else cooked_cls.__primarykey__
    values = set()
    for attr in dir(cooked_cls):
        try:
            if isinstance(getattr(cooked_cls, attr), spacetime_property):
                values.add(getattr(cooked_cls, attr))
        except AttributeError:
            continue
            
    for base in cooked_cls.mro():
        values.update(set([getattr(base, attr) 
                            for attr in dir(base) 
                            if isinstance(getattr(base, attr), spacetime_property)]))
    for value in values:
        if hasattr(value, "_dimension"):
            cooked_cls.__dimensions__.add(value)
            cooked_cls.__dimensions_name__.add(value._name)
        if hasattr(value, "_primarykey") and getattr(value, "_primarykey") != None:
            cooked_cls.__primarykey__ = value
            setattr(cooked_cls, value._name, value)
    for dimension in cooked_cls.__dimensions__:
        setattr(cooked_cls, dimension._name, dimension)

@property
def __start_tracking_prop__(self): 
    if not hasattr(self, "__pcc_tracking__"):
        self.__pcc_tracking__ = False
    return self.__pcc_tracking__

@__start_tracking_prop__.setter
def __start_tracking_prop__(self, v): self.__pcc_tracking__ = v

