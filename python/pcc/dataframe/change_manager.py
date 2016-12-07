#################################################
#### Record keeping (Atomic Needed) #############
#################################################
from recursive_dictionary import RecursiveDictionary
from multiprocessing import Queue

class ChangeManager(object):
    def __init__(self):
        # Stores the object references for new, mod, and deleted.
        self.current_buffer = RecursiveDictionary()

        # groupname -> {oid -> proto object representing changes.}
        self.current_record = RecursiveDictionary()

        self.known_objects = RecursiveDictionary()

        self.deleted_objs = RecursiveDictionary()

        self.temp_record = Queue()
        
    #################################################
    ### Static Methods ##############################
    #################################################
    
    def add_records(self, records):
        print "record:", records

    def add_changelog(self, changes):
        pass

    def add_buffer_changes(self, buffer_changes):
        pass
    
    #################################################
    ### API Methods #################################
    #################################################
    def report_dim_modification(self, tp_obj, oid, value_change):
        print "dim mod", tp_obj.name, oid, value_change

    #################################################
    ### Private Methods #############################
    #################################################
    
    