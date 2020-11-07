# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2020 Sarah Hoffmann
""" Test threading helpers.
"""

import unittest
import time
from itertools import count

from osgende.common.threads import WorkerQueue, WorkerError

class TestWorkerQueue:

    def test_init_func(self):
        init_called = count()

        queue = WorkerQueue(lambda x: x, numthreads=self.numthreads,
                            initfunc=init_called.__next__)

        queue.finish()

        self.assertEqual(max(1, self.numthreads), next(init_called))

    def test_add_task(self):
        done = set()

        queue = WorkerQueue(done.add, numthreads=self.numthreads)

        for i in range(100):
            queue.add_task(i)

        queue.finish()

        self.assertSetEqual(set(range(100)), done)

    def test_shutdown_func(self):
        shutdown_called = count()

        queue = WorkerQueue(lambda x: x, numthreads=self.numthreads,
                            shutdownfunc=shutdown_called.__next__)

        queue.finish()

        self.assertEqual(max(1, self.numthreads), next(shutdown_called))




class TestSimpleWorkerQueue(TestWorkerQueue, unittest.TestCase):
    numthreads = 0

class TestThreadsWorkerQueue(TestWorkerQueue, unittest.TestCase):
    numthreads = 3

    def test_queue_deadlock(self):
        queue = WorkerQueue(time.sleep, numthreads=1)

        queue.add_task('x')
        with self.assertRaises(WorkerError):
            for i in range(1000):
                queue.add_task(0.1)

