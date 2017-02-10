from multiprocessing import RLock
from Queue import Empty, Queue

#################################################
#### Attached Dataframe Stuff ###################
#################################################

class QueueManager(object):
    def __init__(self):
        
        self.df_to_tp = dict()

        self.attached_dataframes = set()

        self.tp_to_attached_df = dict()

        self.queues = dict()
        
        self.type_map = dict()
        
        self.add_lock = RLock()

    #################################################
    ### Static Methods ##############################
    #################################################
    
    
    
    #################################################
    ### API Methods #################################
    #################################################
    def add_records(self, applied_records, pcc_change_records, except_app = None):
        application_to_record = dict()

        with self.add_lock:
            for rec in applied_records:
                self._add_tp_app_record(rec, application_to_record, except_app)
            if pcc_change_records:
                for rec in pcc_change_records:
                    self._add_tp_app_record(rec, application_to_record)

        for app in application_to_record:
            self.queues[app].put(application_to_record[app])

    def add_app_queue(self, app_queue):
        q = Queue()
        with self.add_lock:
            for t in app_queue.types:
                self.type_map.setdefault(t.__realname__, list()).append(app_queue.app_name)
            self.queues[app_queue.app_name] = q
        return q

    #################################################
    ### Private Methods #############################
    #################################################
    
    def _add_tp_app_record(self, rec, application_to_record, except_app = None):
        event, tpname, groupname, oid, dim_change, full_obj, fk_for_tp = (
            rec.event, rec.tpname, rec.groupname, rec.oid, rec.dim_change, rec.full_obj, rec.fk_type)
        if tpname in self.type_map:
            for app in self.type_map[tpname]:
                if app != except_app:
                    application_to_record.setdefault(app, list()).append(rec)
        elif fk_for_tp and fk_for_tp in self.type_map:
            for app in self.type_map[fk_for_tp]:
                if app != except_app:
                    application_to_record.setdefault(app, list()).append(rec)
