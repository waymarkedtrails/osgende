# This file is part of Osgende
# Copyright (C) 2010-11 Sarah Hoffmann
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
"""
Helper classes for multi-threaded execution.
"""

import threading
import Queue

class WorkerError(Exception):
    """Raised when a worker thread unexpectedly dies."""
    pass

class WorkerQueue:
    """ Provides a queue and a pool of threads that process the tasks in the
        queue. Note that this class works for consumer threads only.

        'numthreads' states the number of threads to use for execution. If
        it is None, no threads will be used and the items instead immediately
        processed when they are entered in the queue.

        'process_func' must be a function that takes exactly one argument: the
        next item to be processed. The optional 'initfunc' is called from the
        worker thread, once it is set up and 'shutdownfunc' when the thread is
        ended properly (not when an exception occurs). If the queue is run in
        single-threaded mode, 'initfunc' is called immediately and 'shutdownfunc'
        within finish().

    """

    def __init__(self, process_func, numthreads=None, initfunc=None, shutdownfunc=None):
        self.numthreads = numthreads
        if numthreads is None:
            # If we are in monothreading mode, simply execute
            # the processing function, when a new task is added
            self.add_task = process_func
            if initfunc is not None:
                initfunc()
            self.shutdownfunc = shutdownfunc
        else:
            self._setup_threads(process_func, initfunc, shutdownfunc)


    def add_task(self, data):
        """Add an item to be processed to the queue.
        """
        try:
            while True:
                try:
                    self.queue.put(data, True, 2)
                    break
                except Queue.Full:
                    # check that all our threads are still alive
                    for w in self.workers:
                        if not w.is_alive():
                            print "Internal error. Thread died. Killing other threads."
                            self.finish(True)
                            raise Exception("Internal error. Thread died.")
        except KeyboardInterrupt:
            print "User requested abort. Cleaning up..."
            self.finish(True)              
            raise SystemExit("Ctrl-C detected, exiting...")
        


    def finish(self, flush=False):
        """Wait for the threads to finish and then let them die.
           Note that you must *always* call this function even if there
           is an error or your Python app will hang.

           If 'flush' is true, then all remaining elements in the queue
           will be deleted before the threads are killed. You should set
           this to true in case of a fatal error where your threads may
           not consume any data anymore.
        """
        if self.numthreads is None:
            if self.shutdownfunc is not None:
                self.shutdownfunc()
        else:
            if flush:
                while not self.queue.empty():
                    try:
                        self.queue.get(False)
                    except Queue.Empty:
                        pass # don't care

            for i in range(self.numthreads):
                self.queue.put(None)
            print "Waiting for threads to finish"
            for w in self.workers:
                w.join()



    def _setup_threads(self, process_func, initfunc, shutdownfunc):
        print "Using", self.numthreads, "parallel threads."
        self.queue = Queue.Queue(10*self.numthreads)
        
        self.workers = []
        for i in range(self.numthreads):
            worker = _WorkerThread(self.queue, process_func, initfunc, shutdownfunc)
            worker_thread = threading.Thread(target=worker.loop)
            worker_thread.start()
            self.workers.append(worker_thread)


class _WorkerThread:

    def __init__(self, queue, process_func, initfunc, shutdownfunc):
        self.queue = queue
        self.process_func = process_func
        self.initfunc = initfunc
        self.shutdownfunc = shutdownfunc

    def loop(self):
        self.initfunc()

        while True:
            req = self.queue.get()
            if req is None:
                break

            self.process_func(req)
            self.queue.task_done()

        self.shutdownfunc()

