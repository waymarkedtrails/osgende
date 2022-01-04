# SPDX-License-Identifier: GPL-3.0-only
#
# This file is part of Osgende
# Copyright (C) 2022 Sarah Hoffmann
""" Test threading helpers.
"""
import time
from itertools import count

import pytest

from osgende.common.threads import WorkerQueue, WorkerError

@pytest.mark.parametrize('threads', (0, 3))
def test_init_func(threads):
    init_called = count()

    queue = WorkerQueue(lambda x: x, numthreads=threads,
                        initfunc=init_called.__next__)

    queue.finish()

    assert max(1, threads) == next(init_called)


@pytest.mark.parametrize('threads', (0, 3))
def test_add_task(threads):
    done = set()

    queue = WorkerQueue(done.add, numthreads=threads)

    for i in range(100):
        queue.add_task(i)

    queue.finish()

    assert set(range(100)) == done


@pytest.mark.parametrize('threads', (0, 3))
def test_shutdown_func(threads):
    shutdown_called = count()

    queue = WorkerQueue(lambda x: x, numthreads=threads,
                        shutdownfunc=shutdown_called.__next__)

    queue.finish()

    assert max(1, threads) == next(shutdown_called)


def test_queue_deadlock():
    queue = WorkerQueue(time.sleep, numthreads=1)

    queue.add_task('x')
    with pytest.raises(WorkerError):
        for i in range(1000):
            queue.add_task(0.1)
