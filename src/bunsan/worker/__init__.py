#!/usr/bin/python3

import sys
import signal
import queue
import xmlrpc.client
import xmlrpc.server
import threading
import tempfile
import subprocess

import bunsan.pm

from bunsan.worker.dcs import *
from bunsan.worker.callback import *

# for logging
import math
import traceback
import time


def _log(*args, **kwargs):
    kwargs["file"] = sys.stderr
    ftime, itime = math.modf(time.time())
    _1 = time.strftime("%Y-%b-%d %T", time.localtime(itime))
    _2 = str(ftime)[2:8]
    tb = traceback.extract_stack()[-2]
    line = " -> ".join(map(str.strip, traceback.format_list([tb])[0].strip().split('\n')))
    print("[{}.{}] INFO [{}] -".format(_1, _2, line), *args, **kwargs)


class _InterruptedError(BaseException):
    pass


def _interrupt_raiser(signum, frame):
    raise(_InterruptedError(signum))


def _auto_restart(func):
    def __log(*args, **kwargs):
        args = ["Function {}:".format(func)] + list(args)
        _log(*args, **kwargs)
    def func_(*args, **kwargs):
        completed = False
        while not completed:
            try:
                func(*args, **kwargs)
                __log('completed.')
                completed = True
            except Exception as e:
                __log('exception occurred:', e)
        __log('exiting.')
    return func_



class _ProcessSettings(object):

    def __init__(self, arguments, stdin_data=None):
        self.arguments = arguments
        if stdin_data is None:
            self.stdin_data = None
        else:
            self.stdin_data = stdin_data.data


class _Task(object):

    def __init__(self, callback, package, process):
        self.callback = callback
        self.package = package
        self.process = process


# global variables
_quit = threading.Event()
_queue = queue.Queue()
_repository = None
_hub = None
_tmpdir = None


@_auto_restart
def _worker(num):
    assert _repository is not None
    assert _hub is not None
    assert _tmpdir is not None
    def __log(*args, **kwargs):
        args = ["Worker {}:".format(num)] + list(args)
        _log(*args, **kwargs)
    hub = _hub.proxy()
    __log("started")
    while not _quit.is_set():
        __log("waiting for task")
        task = None
        while not _quit.is_set() and task is None:
            try:
                task = _queue.get(timeout=0.1)
            except queue.Empty:
                pass
        if _quit.is_set():
            return
        assert task is not None
        hub.decrease_capacity()
        __log("task received")
        callback = None
        try:
            callback = task.callback
            callback.send('STARTED')
            with tempfile.TemporaryDirectory(dir=_tmpdir) as tmpdir:
                callback.send('EXTRACTING')
                __log("extracting to ", tmpdir)
                _repository.extract(task.package, tmpdir)
                callback.send('EXTRACTED')
                __log("running ", repr(task.process.arguments))
                with subprocess.Popen(task.process.arguments, cwd=tmpdir, stdin=subprocess.PIPE) as proc:
                    if task.process.stdin_data is not None:
                        proc.stdin.write(task.process.stdin_data)
                    proc.stdin.close()
                    ret = proc.wait()
                    if ret != 0:
                        raise subprocess.CalledProcessError(ret, task.process.arguments)
            callback.send('DONE')
        except Exception as e:
            try:
                __log('failed due to:', traceback.format_exc())
                callback.send('FAIL', traceback.format_exc())
            except:
                pass
        _queue.task_done()
        hub.increase_capacity()


def _add_task(callback, package, process):
    _log("Received new task, parsing...")
    assert callback['type'] == 'xmlrpc'
    callback_ = XMLRPCCallback(*callback['arguments'])
    process_ = _ProcessSettings(**process)
    callback_.send('RECEIVED')
    _log("Registered new task.")
    _queue.put(_Task(callback=callback_, package=package, process=process_))


@_auto_restart
def _ping_it():
    hub = _hub.proxy()
    while not _quit.wait(timeout=10):
        hub.ping()
        _log("Pinged.")


def _interface(addr, hub_uri, worker_count, resources):
    signal.signal(signal.SIGTERM, _interrupt_raiser)
    _log("Starting worker at {} hub={} threads={}".format(addr, hub_uri, worker_count))
    hub = _hub.proxy()
    with hub.context(capacity=worker_count):
        pinger = threading.Thread(target=_ping_it)
        pinger.start()
        for resource, uri in resources:
            hub.add_resource(resource, uri)
        server = xmlrpc.server.SimpleXMLRPCServer(addr=addr, allow_none=True)
        server.register_function(_add_task, name='add_task')
        _log("Starting threads...")
        workers = []
        for i in range(worker_count):
            thread = threading.Thread(target=_worker, args=(i,))
            thread.start()
            workers.append(thread)
        _log("{} workers were started.".format(worker_count))
        _log("Starting xmlrpc server...")
        try:
            server.serve_forever()
        except (KeyboardInterrupt, _InterruptedError) as e:
            _log("Exiting.")
            _quit.set()
            _log("Joining pinger...")
            pinger.join()
            _log("Joining workers...")
            for worker in workers:
                worker.join()
            _log("Completed.")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser("bunsan::worker")
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.0.1', help="version information")
    parser.add_argument('-l', '--listen', action='store', dest='addr', help='Listen on addr:port', required=True)
    parser.add_argument('-d', '--hub', action='store', dest='hub_uri', help='hub xmlrpc interface', required=True)
    parser.add_argument('-m', '--machine', action='store', dest='machine', help='machine name', required=True)
    parser.add_argument('-c', '--worker-count', action='store', dest='worker_count', type=int, help='worker count', default=1)
    parser.add_argument('-r', '--repository-config', action='store', dest='repository_config', help='path to repository config', required=True)
    parser.add_argument('-s', '--resource', action='append', dest='resources', help='resources in format resource_id=resource_uri')
    parser.add_argument('-t', '--tmpdir', action='store', dest='tmpdir', help='temporary directory path', default='/tmp')
    args = parser.parse_args()
    host, port = tuple(args.addr.split(':'))
    port = int(port)
    _repository = bunsan.pm.Repository(args.repository_config)
    _hub = Hub(hub_uri=args.hub_uri, machine=args.machine)
    _tmpdir = args.tmpdir
    def split_resource(s):
        pos = s.index('=')
        return (s[:pos], s[pos + 1:])
    resources = list(map(split_resource, args.resources or []))
    _interface(addr=(host, port), hub_uri=args.hub_uri, worker_count=args.worker_count, resources=resources)
