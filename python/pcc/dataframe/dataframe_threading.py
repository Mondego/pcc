from threading import Thread
from Queue import Queue, Empty
from dataframe import dataframe
from uuid import uuid4

from dataframe_request import GetDFRequest, \
    AppendDFRequest, \
    ExtendDFRequest, \
    DeleteDFRequest, \
    DeleteAllDFRequest, \
    ApplyChangesDFRequest, \
    PutDFRequest

class dataframe_wrapper(Thread):
    def __init__(self, name = str(uuid4()), df = None):
        Thread.__init__(self)
        self.name = name
        self.dataframe = dataframe(name = name)
        
        # Insert changes Queue
        self.put_queue = Queue()

        # Extract changes Queue
        self.get_queue = Queue()

        # Results for get requests
        self.get_token_dict = dict()
        self.isdaemon = True
        self.shutdown = False
        self.start()


    def run(self):
        while not self.shutdown:
            try:
                get_req = self.get_queue.get_nowait()
                self.process_get_req(get_req, self.get_token_dict)
            except Empty:
                pass
            try:
                put_req = self.put_queue.get_nowait()
                self.process_put_req(put_req)
            except Empty:
                pass

    def process_get_req(self, get_req, token_dict):
        if not isinstance(get_req, GetDFRequest):
            return
        result = self.dataframe.get(get_req.type_object, 
                                         get_req.oid,
                                         get_req.param)
        token_dict[get_req.token].put(result)

    def process_put_req(self, put_req):
        if isinstance(put_req, ApplyChangesDFRequest):
            self.process_apply_req(put_req)
        elif isinstance(put_req, AppendDFRequest):
            self.process_append_req(put_req)
        elif isinstance(put_req, ExtendDFRequest):
            self.process_extend_req(put_req)
        elif isinstance(put_req, DeleteDFRequest):
            self.process_delete_req(put_req)
        elif isinstance(put_req, DeleteAllDFRequest):
            self.process_deleteall_req(put_req)
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

    def process_apply_req(self, apply_req):
        pass

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
        self.put_queue.put(req)

    def extend(self, tp, objs):
        req = ExtendDFRequest()
        req.objs = objs
        req.type_object = tp
        self.put_queue.put(req)

    def get(self, tp, oid = None, parameters = None):
        req = GetDFRequest()
        req.type_object = tp
        req.oid = oid
        req.param = parameters
        req.token = uuid4()
        self.get_token_dict[req.token] = Queue()
        self.put_queue.put(req)
        return self.get_token_dict[req.token].get()

    def delete(self, tp, obj):
        req = DeleteDFRequest()
        req.obj = obj
        req.type_object = tp
        self.put_queue.put(req)

    def delete_all(self, tp):
        req = DeleteAllDFRequest()
        req.type_object = tp
        self.put_queue.put(req)

    #############################################
    