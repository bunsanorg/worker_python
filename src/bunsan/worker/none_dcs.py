class NoneHub(object):

    def __init__(self, hub_uri, description, resources=None, *args, **kwargs):
        pass

    def ping(self):
        return True

    def register_machine(self, capacity):
        pass

    def unregister_machine(self):
        pass

    def set_capacity(self, capacity):
        pass

    def set_resource_uri(self, resource, uri):
        pass

    def increase_capacity(self, delta=1):
        pass

    def decrease_capacity(self, delta=1):
        pass

    def add_resources(self, resources):
        pass

    def add_resource(self, resource, uri):
        pass

    def remove_resource(self, resource):
        pass

    def select_resource(self, resource):
        pass

    def context(self, *args, **kwargs):
        hub = self
        class Context(object):
            def __enter__(self):
                hub.register_machine(*args, **kwargs)
            def __exit__(self, exc_type, exc_value, traceback):
                hub.unregister_machine()
        return Context()
