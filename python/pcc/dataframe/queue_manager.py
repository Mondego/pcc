from multiprocessing import Manager, RLock
from multiprocessing.queues import Empty

#################################################
#### Attached Dataframe Stuff ###################
#################################################

class QueueManager(object):
    def __init__(self):
        
        self.df_to_tp = dict()

        self.attached_dataframes = set()

        self.tp_to_attached_df = dict()

        self.manager = Manager()
        
        self.new_queue = self.manager.Queue()
        self.queues = dict()
        self.type_map = dict()
        self.add_lock = RLock()

    #################################################
    ### Static Methods ##############################
    #################################################
    
    
    
    #################################################
    ### API Methods #################################
    #################################################
        
    def add_records(self, records):
        application_to_record = dict()
        with self.add_lock:
            for rec in records:
                event, tpname, groupname, oid, dim_change, full_obj, fk_for_tp = (
                    rec.event, rec.tpname, rec.groupname, rec.oid, rec.dim_change, rec.full_obj, rec.fk_type)
                if tpname in self.type_map:
                    for app in self.type_map[tpname]:
                        application_to_record.setdefault(app, list()).append(rec)
                elif fk_for_tp and fk_for_tp in self.type_map:
                    for app in self.type_map[fk_for_tp]:
                        application_to_record.setdefault(app, list()).append(rec)
        for app in application_to_record:
            self.queues[app].put(application_to_record[app])

    def add_app_queue(self, app_queue):
        q = self.manager.Queue()
        with self.add_lock:
            for t in app_queue.types:
                self.type_map.setdefault(t.__realname__, list()).append(app_queue.app_name)
            self.queues[app_queue.app_name] = q
        return q

    #################################################
    ### Private Methods #############################
    #################################################
    
    