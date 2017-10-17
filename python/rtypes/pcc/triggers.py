

class TriggerTime(object):
    before = 1
    after = 2


class TriggerAction(object):
    insert = 1
    read = 2
    update = 3
    delete = 4


class TriggerProcedure(object):
    def __init__(self, procedure):
        self.procedure = procedure

    def __call__(self, *args, **kwargs):
        return self.procedure(*args, **kwargs)


class Trigger(object):

    def __init__(self, pcc_type, time, action, priority=None):
        self.pcc_type = pcc_type
        self.time = time
        self.action = action
        self.priority = priority

    def __call__(self, procedure):
        return TriggerProcedure(procedure)


if __name__ == "__main__":
    pass

