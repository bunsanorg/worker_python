import threading


class Counter(object):

    _lock = threading.Lock()

    def __init__(self, callback, init=1):
        self._callback = callback
        self._value = init

    def __call__(self):
        with self._lock:
            return self._value

    def use(self, delta=1):
        capacity = self
        class Context(object):
            def __enter__(self):
                with capacity._lock:
                    capacity._value -= delta
                try:
                    capacity._callback()
                except BaseException as e:
                    with capacity._lock:
                        capacity._value += delta
                    raise e
            def __exit__(self, exc_type, exc_value, traceback):
                with capacity._lock:
                    capacity._value += delta
                capacity._callback()
        return Context()
