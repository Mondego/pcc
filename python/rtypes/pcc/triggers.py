

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

            def __call__(self):
                return self.procedure

        self.TP = TriggerProcedure(procedure)

        return self.TP.__call__()

    
class Customer():
    pass


@Trigger(Customer, 'before', 'insert')
def insert_trigger(dataframe, new, old, current):
    print("now we can have some fun :)")


insert_trigger('test_dataframe', 'test_new', 'test_old', 'test_current')
