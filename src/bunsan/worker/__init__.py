#!/usr/bin/python3

import sys
import signal
import queue
import xmlrpc.client
import xmlrpc.server
import threading
import tempfile
import subprocess
import logging
import traceback

from bunsan.worker.callback import *
from bunsan.worker.counter import *


class _InterruptedError(BaseException):
    pass


def _interrupt_raiser(signum, frame):
    raise(_InterruptedError(signum))


def _auto_restart(func):
    funcname = "Function " + repr(func)
    def func_(*args, **kwargs):
        completed = False
        while not completed:
            try:
                func(*args, **kwargs)
                logging.debug("%s has completed.", funcname)
                completed = True
            except Exception as e:
                logging.exception("%s: unable to complete due to %s.", funcname, e)
        logging.debug("Exiting %s loop.", func)
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


class Worker(object):

    def __init__(self, repository, query_interval, hub, tmpdir, addr, worker_count):
        self._quit = threading.Event()
        self._queue = queue.Queue()
        self._query_interval = query_interval
        self._repository = repository
        self._hub = hub
        self._tmpdir = tmpdir
        self._addr = addr
        self._worker_count = worker_count
        self._counter = Counter(self._set_capacity, self._worker_count)
        self._need_registration = threading.Event()

    def _capacity(self):
        return self._counter() - self._queue.qsize()

    def _set_capacity(self):
        try:
            logging.debug("Updating capacity...")
            self._hub.set_capacity(self._capacity())
        except Exception as e:
            logging.exception("Unable to set capacity, due to %s, need registration.", e)
            self._need_registration.set()

    @_auto_restart
    def _worker(self, num):
        workername = "Worker {}".format(num)
        logging.debug(workername + " has started.")
        while not self._quit.is_set():
            logging.debug("%s: waiting for task", workername)
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
                logging.debug("%s: task received.", workername)
                callback = None
                try:
                    callback = task.callback
                    callback.send('STARTED')
                    with tempfile.TemporaryDirectory(dir=self._tmpdir) as tmpdir:
                        callback.send('EXTRACTING')
                        logging.debug("%s: extracting to %s...", workername, tmpdir)
                        self._repository.extract(task.package, tmpdir)
                        callback.send('EXTRACTED')
                        logging.debug("%s: running %s.", workername, repr(task.process.arguments))
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
                        logging.exception("%s: failed due to %s.", workername, e)
                        callback.send('FAIL', traceback.format_exc())
                    except Exception as e:
                        logging.exception("%s: unable to use callback due to %s.", workername, e)
                        pass
                finally:
                    self._queue.task_done()

    def _add_task(self, callback, package, process):
        """
            Note: error in this method will cause xmlrpc fault.
            No other information is needed for caller.
        """
        logging.debug("Received new task, parsing...")
        assert callback['type'] == 'xmlrpc'
        callback_ = XMLRPCCallback(*callback['arguments'])
        process_ = _ProcessSettings(**process)
        callback_.send('RECEIVED')
        logging.debug("Registering new task...")
        task = _Task(callback=callback_, package=package, process=process_)
        self._queue.put(task)
        logging.debug("Registered.")
        self._set_capacity()

    def serve_forever(self, signals=None):
        """
            \param signals which signals will cause normal exit
        """
        if signals is not None:
            for s in signals:
                logging.debug("Registering %s for %s signal...", _interrupt_raiser, s)
                signal.signal(s, _interrupt_raiser)
        logging.info("Starting %d worker(s) listening on \"%s:%d\"...", self._worker_count, *self._addr)
        with self._hub.context(capacity=self._capacity()):
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
            server.timeout = self._query_interval
            server.register_function(self._add_task, name='add_task')
            logging.debug("Starting threads...")
            workers = []
            for i in range(self._worker_count):
                thread = threading.Thread(target=self._worker, args=(i,))
                thread.start()
                workers.append(thread)
            logging.debug("%d workers were started.", self._worker_count)
            logging.debug("Starting xmlrpc server...")
            try:
                logging.debug("Entering task handling loop...")
                while True:
                    if not server.request_timeout:
                        logging.debug("Waiting for request...")
                    server.handle_request()
                    if not server.request_timeout:
                        logging.debug("Request was handled.")
                    if self._need_registration.is_set():
                        logging.debug("Registration is needed...")
                        try:
                            logging.debug("Trying to register machine...")
                            self._hub.register_machine(capacity=self._capacity())
                            logging.debug("Machine was successfully registered.")
                            self._need_registration.clear()
                        except Exception as e:
                            logging.exception("Unable to register due to %s.", e)
                    else:
                        logging.debug("Trying to ping...")
                        try:
                            if not self._hub.ping():
                                self._need_registration.set()
                            logging.debug("Pinged.")
                        except Exception as e:
                            logging.exception("Unable to ping due to %s.", e)
                            self._need_registration.set()
            except (KeyboardInterrupt, _InterruptedError) as e:
                logging.debug("Exiting.")
                self._quit.set()
                logging.debug("Joining workers...")
                for worker in workers:
                    worker.join()
                logging.info("Completed.")
