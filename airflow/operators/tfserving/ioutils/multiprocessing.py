#!/usr/bin/env python
# -*- coding: UTF-8 -*-

# We must import this explicitly, it is not imported by the top-level
# multiprocessing module.
import multiprocessing.pool
import time


class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


def manual_gc(pool):
    time.sleep(10)
    pool.close()
    pool.join()
    time.sleep(5)
