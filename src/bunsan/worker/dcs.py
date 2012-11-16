from bunsan.worker.toxmlrpc import ServerProxy

class Hub(object):

    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def proxy(self):
        return HubProxy(*self._args, **self._kwargs)


class HubProxy(object):

    def __init__(self, hub_uri, machine, *args, **kwargs):
        self._hub = ServerProxy(hub_uri, *args, **kwargs)
        self._machine = str(machine)

    def ping(self):
        return self._hub.ping(self._machine)

    def add_machine(self, capacity):
        return self._hub.add_machine(self._machine, float(capacity))

    def remove_machine(self):
        return self._hub.remove_machine(self._machine)

    def set_capacity(self, capacity):
        return self._hub.set_capacity(self._machine, float(capacity))

    def set_resource_uri(self, resource, uri):
        return self._hub.set_resource_uri(self._machine, resource, uri)

    def increase_capacity(self, delta=1):
        return self._hub.increase_capacity(self._machine, float(delta))

    def decrease_capacity(self, delta=1):
        return self._hub.decrease_capacity(self._machine, float(delta))

    def add_resource(self, resource, uri):
        return self._hub.add_resource(self._machine, resource, uri)

    def remove_resource(self, resource):
        return self._hub.remove_resource(self._machine, resource)

    def select_resource(self, resource):
        return self._hub.select_resource(self._machine)

    def context(self, *args, **kwargs):
        hub = self
        class Context(object):
            def __enter__(self):
                hub.add_machine(*args, **kwargs)
            def __exit__(self, exc_type, exc_value, traceback):
                hub.remove_machine()
        return Context()
