from __future__ import absolute_import

from rtypes.pcc.triggers import Trigger
from rtypes.pcc.triggers import TriggerAction
from rtypes.pcc.triggers import TriggerTime


if __name__ == "__main__":
    print("Starting: Trigger Unit Test ------\n")

    class Customer():
        pass

    @Trigger(Customer, TriggerTime.before, TriggerAction.insert)
    def insert_trigger(dataframe, new, old, current):
        print("now we can have some fun :)")


    test = insert_trigger('test_dataframe', 'test_new', 'test_old', 'test_current')

    print(type(insert_trigger))