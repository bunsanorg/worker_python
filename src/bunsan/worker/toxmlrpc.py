import http.client
import xmlrpc.client
import socket


class TimeoutTransport(xmlrpc.client.Transport):

    def __init__(self, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, *args, **kwargs):
        super(TimeoutTransport, self).__init__(*args, **kwargs)
        self.timeout = timeout

    def make_connection(self, host):
        #return an existing connection if possible.  This allows
        #HTTP/1.1 keep-alive.
        if self._connection and host == self._connection[0]:
            return self._connection[1]
        # create a HTTP connection object from a host descriptor
        chost, self._extra_headers, x509 = self.get_host_info(host)
        self._connection = host, http.client.HTTPConnection(chost, timeout=self.timeout)
        return self._connection[1]


class ServerProxy(xmlrpc.client.ServerProxy):

    def __init__(self, *args, timeout=None, **kwargs):
        if timeout is None:
            super(ServerProxy, self).__init__(*args, **kwargs)
        else:
            super(ServerProxy, self).__init__(*args, transport=TimeoutTransport(timeout), **kwargs)
