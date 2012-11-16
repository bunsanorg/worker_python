from bunsan.worker.toxmlrpc import ServerProxy

import threading


class Hub(object):

    _lock = threading.Lock()

    @staticmethod
    def _to_dict(pairs):
        if isinstance(pairs, dict):
            pairs = pairs.items()
        return {i: j for i, j in pairs}

    def __init__(self, hub_uri, description, resources=None, *args, **kwargs):
        self._hub = ServerProxy(hub_uri, *args, **kwargs)
        self._id = None
        self._description = str(description)
        self._resources = Hub._to_dict(resources) if resources is not None else None

    def ping(self):
        with self._lock:
            return self._hub.ping(self._id)

    def register_machine(self, capacity):
        with self._lock:
            args = [self._description, float(capacity)]
            if self._resources is not None:
                args.append(self._resources)
            self._id = self._hub.register_machine(*args)
            assert self._id is not None
            return self._id

    def unregister_machine(self):
        with self._lock:
            return self._hub.unregister_machine(self._id)

    def set_capacity(self, capacity):
        with self._lock:
            return self._hub.set_capacity(self._id, float(capacity))

    def set_resource_uri(self, resource, uri):
        with self._lock:
            return self._hub.set_resource_uri(self._id, resource, uri)

    def increase_capacity(self, delta=1):
        with self._lock:
            return self._hub.increase_capacity(self._id, float(delta))

    def decrease_capacity(self, delta=1):
        with self._lock:
            return self._hub.decrease_capacity(self._id, float(delta))

    def add_resources(self, resources):
        with self._lock:
            return self._hub.add_resources(self._id, Hub._to_dict(resources))

    def add_resource(self, resource, uri):
        with self._lock:
            return self._hub.add_resource(self._id, resource, uri)

    def remove_resource(self, resource):
        with self._lock:
            return self._hub.remove_resource(self._id, resource)

    def select_resource(self, resource):
        return self._hub.select_resource(resource)

    def context(self, *args, **kwargs):
        hub = self
        class Context(object):
            def __enter__(self):
                hub.register_machine(*args, **kwargs)
            def __exit__(self, exc_type, exc_value, traceback):
                hub.unregister_machine()
        return Context()
