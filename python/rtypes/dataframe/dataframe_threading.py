from threading import Thread
from uuid import uuid4

from multiprocess import Queue
from multiprocess.queues import Empty

from rtypes.dataframe import dataframe

from dataframe_request import GetDFRequest, \
    AppendDFRequest, \
    ExtendDFRequest, \
    DeleteDFRequest, \
    DeleteAllDFRequest, \
    ApplyChangesDFRequest, \
    PutDFRequest, \
    ShutdownDFRequest, \
    GetRecordDFRequest

class dataframe_wrapper(Thread):
    def __init__(self, name=str(uuid4()), dataframe=None):
        Thread.__init__(self, name="Thread_dataframe_wrapper_{0}".format(name))
        self.name = name
        self.dataframe = dataframe if dataframe is not None else dataframe(
            name=name)

        # Insert/Get changes Queue
        self.queue = Queue()

        # Results for get requests
        self.get_token_dict = dict()
        self.daemon = True
        self.stop = False

    def run(self):
        while not self.stop:
            req = self.queue.get()
            if (isinstance(req, GetDFRequest)
                    or isinstance(req, GetRecordDFRequest)):
                self.process_get_req(req, self.get_token_dict)
            else:
                self.process_put_req(req, self.get_token_dict)

    def shutdown(self):
        self.queue.put(ShutdownDFRequest())

    def process_get_req(self, get_req, token_dict):
        result = None
        if isinstance(get_req, GetDFRequest):
            result = self.dataframe.get(
                get_req.type_object, get_req.oid, get_req.param)
        elif isinstance(get_req, GetRecordDFRequest):
            result = self.dataframe.get_record(get_req.changelist)
        token_dict[get_req.token].put(result)

    def process_put_req(self, put_req, token_dict):
        if isinstance(put_req, ApplyChangesDFRequest):
            self.process_apply_req(put_req, token_dict)
        elif isinstance(put_req, AppendDFRequest):
            self.process_append_req(put_req)
        elif isinstance(put_req, ExtendDFRequest):
            self.process_extend_req(put_req)
        elif isinstance(put_req, DeleteDFRequest):
            self.process_delete_req(put_req)
        elif isinstance(put_req, DeleteAllDFRequest):
            self.process_deleteall_req(put_req)
        elif isinstance(put_req, ShutdownDFRequest):
            self.stop = True
        return

    def process_append_req(self, append_req):
        self.dataframe.append(
            append_req.type_object, append_req.obj)

    def process_extend_req(self, extend_req):
        self.dataframe.extend(
            extend_req.type_object, extend_req.objs)

    def process_delete_req(self, delete_req):
        self.dataframe.delete(
            delete_req.type_object, delete_req.obj)

    def process_deleteall_req(self, deleteall_req):
        self.dataframe.delete_all(
            deleteall_req.type_object)

    def process_apply_req(self, apply_req, token_dict):
        self.dataframe.apply_changes(
            apply_req.df_changes, except_app=apply_req.except_app)
        if apply_req.wait_for_server:
            token_dict[apply_req.token].put(True)

    ####### TYPE MANAGEMENT METHODS #############
    def add_type(self, tp, tracking=False):
        self.dataframe.add_type(tp, tracking)

    def add_types(self, types, tracking=False):
        self.dataframe.add_types(types, tracking)

    def has_type(self, tp):
        self.dataframe.has_type(tp)

    def reload_types(self, types):
        self.dataframe.reload_types(types)

    def remove_type(self, tp):
        self.dataframe.remove_type(tp)

    def remove_types(self, types):
        self.dataframe.remove_types(types)
    #############################################

    ####### OBJECT MANAGEMENT METHODS ###########
    def append(self, tp, obj):
        req = AppendDFRequest()
        req.obj = obj
        req.type_object = tp
        self.queue.put(req)

    def extend(self, tp, objs):
        req = ExtendDFRequest()
        req.objs = objs
        req.type_object = tp
        self.queue.put(req)

    def get(self, tp, oid = None, parameters=None):
        req = GetDFRequest()
        req.type_object = tp
        req.oid = oid
        req.param = parameters
        req.token = uuid4()
        self.get_token_dict[req.token] = Queue()
        self.queue.put(req)
        try:
            return self.get_token_dict[req.token].get(timeout=5)
        except Empty:
            return list()

    def delete(self, tp, obj):
        req = DeleteDFRequest()
        req.obj = obj
        req.type_object = tp
        self.queue.put(req)

    def delete_all(self, tp):
        req = DeleteAllDFRequest()
        req.type_object = tp
        self.queue.put(req)

    #############################################

    ####### CHANGE MANAGEMENT METHODS ###########

    @property
    def start_recording(self):
        return self.dataframe.startrecording

    @start_recording.setter
    def start_recording(self, v):
        self.dataframe.startrecording = v

    @property
    def object_manager(self):
        return self.dataframe.object_manager

    def apply_changes(self, changes, except_app=None, wait_for_server=False):
        req = ApplyChangesDFRequest()
        req.df_changes = changes
        req.except_app = except_app
        req.wait_for_server = wait_for_server
        if wait_for_server:
            req.token = uuid4()
            self.get_token_dict[req.token] = Queue()
        self.queue.put(req)
        if wait_for_server:
            try:
                return self.get_token_dict[req.token].get(timeout=5)
            except Empty:
                return False

    def get_record(self, changelist=None):
        req = GetRecordDFRequest()
        req.changelist = changelist
        req.token = uuid4()
        self.get_token_dict[req.token] = Queue()
        self.queue.put(req)
        try:
            return self.get_token_dict[req.token].get(timeout=5)
        except Empty:
            return dict()

    def clear_record(self):
        return self.dataframe.clear_record()

    def connect_app_queue(self, app_queue):
        return self.dataframe.connect_app_queue(app_queue)

    def convert_to_record(self, results, deleted_oids):
        return self.dataframe.convert_to_record(results, deleted_oids)

    def serialize_all(self):
        # have to put it through the queue
        return self.dataframe.serialize_all()

    #############################################
