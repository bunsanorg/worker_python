#!/usr/bin/python3

import sys
import queue
import xmlrpc.server

from bunsan.pm import Repository


class _Callback(object):

    def __init__(self, *args):
        pass

    def send(self, *args, **kwargs):
        pass


class _ProcessSettings(object):

    def __init__(self, arguments, stdin_data=None):
        self.arguments = arguments
        self.stdin_data = stdin_data


class _Task(object):

    def __init__(self, callback, package, process):
        self.callback = callback
        self.package = package
        self.process = process


_queue = queue.Queue()
def _worker():
    while True:
        task = _queue.get()
        callback = None
        try:
            callback = task.callback
            callback.send('STARTED')
            # TODO
            callback.send('DONE')
        except Exception as e:
            try:
                callback.send('FAIL', e)
            except:
                pass
        _queue.task_done()


def _add_task(callback, package, process):
    assert callback['type'] == 'xmlrpc'
    callback_ = _Callback(*callback['arguments'])
    process_ = _ProcessSettings(**process)
    callback_.send('RECEIVED')
    _queue.put(_Task(callback=callback_, package=package, process=process_))


def _interface(addr, hub_uri, worker_count):
    server = xmlrpc.server.SimpleXMLRPCServer(addr=addr, allow_none=True)
    server.register_function(_add_task, name='add_task')
    # TODO start workers
    server.serve_forever()


if __name__ == '__main__':
    pass
    #_interface()
