

class Trigger(object):

    def __init__(self, pcc_type, time, action, priority=None):
        self.pcc_type = pcc_type
        self.time = time
        self.action = action
        self.priority = priority

    def __call__(self, procedure):
        class TriggerProcedure(object):

            def __init__(self, procedure):
                self.procedure = procedure

            def __call__(self, *args, **kwargs):
                return self.procedure(*args, **kwargs)

        return TriggerProcedure(procedure)


class TriggerTime(object):
    before = 1
    after = 2


class TriggerAction(object):
    insert = 1
    read = 2
    update = 3
    delete = 4


if __name__ == "__main__":

    class Customer():
        pass

    @Trigger(Customer, "before", "insert")
    def insert_trigger(dataframe, new, old, current):
        print("now we can have some fun :)")


    insert_trigger('test_dataframe', 'test_new', 'test_old', 'test_current')
