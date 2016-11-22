#################################################
#### Record keeping (Atomic Needed) #############
#################################################
from recursive_dictionary import RecursiveDictionary
from multiprocessing import Queue

class RecordManager(object):
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
    
    
    
    #################################################
    ### API Methods #################################
    #################################################
    def report_dim_modification(self):
        pass

    #################################################
    ### Private Methods #############################
    #################################################
    
    