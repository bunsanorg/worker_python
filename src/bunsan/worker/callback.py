import xmlrpc.client

class Callback(object):

    def _method(self, *args):
        """ Send's implementation. """
        pass

    def send(self, msg_type, *args):
        args_ = [msg_type] + list(args)
        return self._method(*args_)


class XMLRPCCallback(Callback):

    def __init__(self, uri, method, *args):
        proxy = xmlrpc.client.ServerProxy(uri)
        self.__method = getattr(proxy, method)
        self._args = args

    def _method(self, *args):
        args = list(self._args) + list(args)
        return self.__method(*args)
