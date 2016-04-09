from abc import *

class IApplication(object):
    __metaclass__ = ABCMeta
    _producer = set()
    _getter = set()
    _gettersetter = set()
    _tracker = set()

    def start_shutdown(self):
        return False

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def shutdown(self):
        pass