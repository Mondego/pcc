from pcc.recursive_dictionary import RecursiveDictionary


class DataframeChanges_Base(RecursiveDictionary):
    # Add all checks to DataframeChanges structure here.
    def ParseFromDict(self, parsed_dict):
        self.rec_update(parsed_dict)

class Event(object):
    Delete = 0
    New = 1
    Modification = 2

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