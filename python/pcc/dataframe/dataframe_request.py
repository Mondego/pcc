class DFRequest(object):
    @property
    def type_object(self): return self._tp_obj
    @type_object.setter
    def type_object(self, v): self._tp_obj = v

class PutDFRequest(DFRequest):
    pass

class GetDFRequest(DFRequest):
    @property
    def token(self): return self._token
    @token.setter
    def token(self, v): self._token = v
    
    @property
    def oid(self):
        try:
            return self._oid
        except AttributeError:
            self._oid = None
            return self._oid
        
    @oid.setter
    def oid(self, v): self._oid = v

    @property
    def param(self):
        try:
            return self._param
        except AttributeError:
            self._param = None
            return self._param
        
    @param.setter
    def param(self, v): self._param = v

class AppendDFRequest(PutDFRequest):
    @property
    def obj(self):
        try:
            return self._obj
        except AttributeError:
            self._obj = None
            return self._obj
        
    @obj.setter
    def obj(self, v): self._obj = v
        

class ExtendDFRequest(PutDFRequest):
    @property
    def objs(self):
        try:
            return self._objs
        except AttributeError:
            self._objs = list()
            return self._objs
        
    @objs.setter
    def objs(self, v): self._objs = v
        
class DeleteDFRequest(PutDFRequest):
    @property
    def obj(self):
        try:
            return self._obj
        except AttributeError:
            self._obj = None
            return self._obj
        
    @obj.setter
    def obj(self, v): self._obj = v

class DeleteAllDFRequest(PutDFRequest):
    pass

class ApplyChangesDFRequest(object):
    @property
    def df_changes(self):
        try:
            return self._df_changes
        except AttributeError:
            self._df_changes = None
            return self._df_changes
        
    @df_changes.setter
    def df_changes(self, v): self._df_changes = v

    @property
    def except_app(self):
        try:
            return self._except_app
        except AttributeError:
            self._except_app = None
            return self._except_app
        
    @except_app.setter
    def except_app(self, v): self._except_app = v

    