from dataframe import dataframe
from uuid import uuid4

class dataframe_client(dataframe):
    def __init__(self, name = str(uuid4())):
        super(dataframe_client, self).__init__(name)
        self.calculate_pcc = False
        self.object_manager.calculate_pcc = False