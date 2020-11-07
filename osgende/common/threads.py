# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2015-2020 Sarah Hoffmann
"""
Helper classes for multi-threaded execution.
"""

import logging
import threading
import queue

LOG = logging.getLogger(__name__)

class WorkerError(Exception):
    """Raised when a worker thread unexpectedly dies."""

class _WorkerQueueSimple:
    """ An implementation of the worker queue without threading.
    """
    def __init__(self, process_func, initfunc, shutdownfunc):
        self.process_func = process_func
        self.shutdownfunc = shutdownfunc

        if initfunc is not None:
            initfunc()

    def add_task(self, data):
        self.process_func(data)

    def finish(self, flush):
        if self.shutdownfunc is not None:
            self.shutdownfunc()

class _WorkerQueueThreaded:
    """ An implementation of the worker queue with threading.
    """
    def __init__(self, process_func, numthreads=None, initfunc=None, shutdownfunc=None):
        self.numthreads = numthreads
        self.queue = queue.Queue(10*self.numthreads)

        LOG.info("Using %d parallel threads.", self.numthreads)

        def worker_loop():
            if initfunc is not None:
                initfunc()

            while True:
                req = self.queue.get()
                if req is None:
                    break

                process_func(req)
                self.queue.task_done()

            if shutdownfunc is not None:
                shutdownfunc()

        self.workers = []
        for _ in range(self.numthreads):
            worker_thread = threading.Thread(target=worker_loop)
            worker_thread.daemon = True
            worker_thread.start()
            self.workers.append(worker_thread)

    def add_task(self, data):
        """Add an item to be processed to the queue.
        """
        while True:
            try:
                self.queue.put(data, True, 2)
                break
            except queue.Full:
                self.check_worker_state()


    def check_worker_state(self):
        """ Check that all workers are still alive and haven't died from
            an exception.
        """
        for worker in self.workers:
            if not worker.is_alive():
                LOG.critical("Internal error. Thread died. Killing other threads.")
                self.finish(True)
                raise WorkerError("Internal error. Thread died.")

    def finish(self, flush=False):
        """Wait for the threads to finish and then let them die.
           Note that you must *always* call this function even if there
           is an error or your Python app will hang.

           If 'flush' is true, then all remaining elements in the queue
           will be deleted before the threads are killed. You should set
           this to true in case of a fatal error where your threads may
           not consume any data anymore.
        """
        if flush:
            while not self.queue.empty():
                try:
                    self.queue.get(False)
                except queue.Empty:
                    pass # don't care

        for _ in range(self.numthreads):
            self.queue.put(None)
        LOG.debug("Waiting for threads to finish")
        for w in self.workers:
            w.join()


class WorkerQueue:
    """ Provides a queue and a pool of threads that process the tasks in the
        queue. Note that this class works for consumer threads only.

        'numthreads' states the number of threads to use for execution. If
        it is 0, no threads will be used and the items instead immediately
        processed when they are entered in the queue. If it is None the
        systemwide default will be used.

        'process_func' must be a function that takes exactly one argument: the
        next item to be processed. The optional 'initfunc' is called from the
        worker thread, once it is set up and 'shutdownfunc' when the thread is
        ended properly (not when an exception occurs). If the queue is run in
        single-threaded mode, 'initfunc' is called immediately and 'shutdownfunc'
        within finish().

    """

    numthreads = 0

    def __init__(self, process_func, numthreads=None, initfunc=None, shutdownfunc=None):
        numthreads = numthreads or self.numthreads
        if numthreads == 0:
            self.worker = _WorkerQueueSimple(process_func, initfunc, shutdownfunc)
        else:
            self.worker = _WorkerQueueThreaded(process_func, numthreads,
                                               initfunc, shutdownfunc)


    def add_task(self, data):
        """Add an item to be processed to the queue.
        """
        self.worker.add_task(data)


    def finish(self, flush=False):
        """Wait for all work to finish.
           Note that you must *always* call this function even if there
           is an error or your Python app will hang.

           If 'flush' is true, then all remaining elements in the queue
           will be deleted before the threads are killed. You should set
           this to true in case of a fatal error where your threads may
           not consume any data anymore.
        """
        self.worker.finish(flush)


class ThreadableDBObject:
    """ Mixin for tables that can process data in parallel.
    """

    numthreads = None

    def set_num_threads(self, num):
        """Set the number of worker threads to use when processing the
           table. Note that this is the number of additional threads
           created when processing, so the total number of threads in
           the system is num+1. Setting num to None (the default) disables
           parallel processing.

        """
        self.numthreads = num


    def create_worker_queue(self, engine, processfunc):
        self.thread = threading.local()
        self.worker_engine = engine
        return WorkerQueue(processfunc, self.numthreads,
                           self._init_worker_thread,
                           self._shutdown_worker_thread)

    def _init_worker_thread(self):
        LOG.debug("Initialising worker...")
        self.thread.conn = self.worker_engine.connect()
        self.thread.trans = self.thread.conn.begin()

    def _shutdown_worker_thread(self):
        LOG.debug("Shutting down worker...")
        self.thread.trans.commit()
        self.thread.conn.close()
