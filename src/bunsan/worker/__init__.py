#!/usr/bin/python3

import sys
import signal
import queue
import xmlrpc.client
import xmlrpc.server
import threading
import tempfile
import subprocess

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
                traceback.print_exc()
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

class _Counter(object):

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


class Worker(object):

    def __init__(self, repository, hub, tmpdir, addr, worker_count):
        self._quit = threading.Event()
        self._queue = queue.Queue()
        self._repository = repository
        self._hub = hub
        self._tmpdir = tmpdir
        self._addr = addr
        self._worker_count = worker_count
        self._counter = _Counter(self._set_capacity, self._worker_count)
        self._need_registration = threading.Event()

    def _capacity(self):
        return self._counter() - self._queue.qsize()

    def _set_capacity(self):
        try:
            _log("Updating capacity...")
            self._hub.set_capacity(self._capacity())
        except Exception as e:
            _log("Unable to set capacity, due to {}, need registration".format(e))
            self._need_registration.set()

    @_auto_restart
    def _worker(self, num):
        def __log(*args, **kwargs):
            args = ["Worker {}:".format(num)] + list(args)
            _log(*args, **kwargs)
        __log("started")
        while not self._quit.is_set():
            __log("waiting for task")
            task = None
            while not self._quit.is_set() and task is None:
                try:
                    task = self._queue.get(timeout=0.1)
                except queue.Empty:
                    pass
            if self._quit.is_set():
                return
            assert task is not None
            with self._counter.use():
                __log("task received")
                callback = None
                try:
                    callback = task.callback
                    callback.send('STARTED')
                    with tempfile.TemporaryDirectory(dir=self._tmpdir) as tmpdir:
                        callback.send('EXTRACTING')
                        __log("extracting to ", tmpdir)
                        self._repository.extract(task.package, tmpdir)
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
                finally:
                    self._queue.task_done()

    @_auto_restart
    def _ping_it(self):
        while not self._quit.wait(timeout=10):
            try:
                if not self._hub.ping():
                    self._need_registration.set()
            except Exception as e:
                self._need_registration.set()
                raise e
            _log("Pinged.")

    def _add_task(self, callback, package, process):
        """
            Note: error in this method will cause xmlrpc fault.
            No other information is needed for caller.
        """
        _log("Received new task, parsing...")
        assert callback['type'] == 'xmlrpc'
        callback_ = XMLRPCCallback(*callback['arguments'])
        process_ = _ProcessSettings(**process)
        callback_.send('RECEIVED')
        _log("Registering new task...")
        task = _Task(callback=callback_, package=package, process=process_)
        self._queue.put(task)
        _log("Registered.")
        self._set_capacity()

    def serve_forever(self, signals=None):
        """
            \param signals which signals will cause normal exit
        """
        if signals is not None:
            for s in signals:
                _log("Registering", _interrupt_raiser, "for", s, "signal...")
                signal.signal(s, _interrupt_raiser)
        _log("Starting {worker_count} workers listening on {addr}".format(addr=self._addr, worker_count=self._worker_count))
        with self._hub.context(capacity=self._capacity()):
            pinger = threading.Thread(target=self._ping_it)
            pinger.start()
            class XMLRPCServer(xmlrpc.server.SimpleXMLRPCServer):
                def __init__(self, *args, **kwargs):
                    super(XMLRPCServer, self).__init__(*args, **kwargs)
                    self.request_timeout = False
                def handle_request(self):
                    self.request_timeout = False
                    super(XMLRPCServer, self).handle_request()
                def handle_timeout(self):
                    self.request_timeout = True
            server = XMLRPCServer(addr=self._addr, allow_none=True)
            server.timeout = 1
            server.register_function(self._add_task, name='add_task')
            _log("Starting threads...")
            workers = []
            for i in range(self._worker_count):
                thread = threading.Thread(target=self._worker, args=(i,))
                thread.start()
                workers.append(thread)
            _log("{} workers were started.".format(self._worker_count))
            _log("Starting xmlrpc server...")
            try:
                _log("Entering task handling loop...")
                while True:
                    if not server.request_timeout:
                        _log("Waiting for request...")
                    server.handle_request()
                    if not server.request_timeout:
                        _log("Request was handled.")
                    if self._need_registration.is_set():
                        _log("Registration is needed...")
                        try:
                            _log("Trying to register machine...")
                            self._hub.register_machine(capacity=self._capacity())
                            _log("Machine was successfully registered.")
                            self._need_registration.clear()
                        except Exception as e:
                            _log("Unable to register due to", e)
            except (KeyboardInterrupt, _InterruptedError) as e:
                _log("Exiting.")
                self._quit.set()
                _log("Joining pinger...")
                pinger.join()
                _log("Joining workers...")
                for worker in workers:
                    worker.join()
                _log("Completed.")
