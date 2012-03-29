# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import pika.channel
import threading
import zope.interface


class Channel(pika.channel.Channel):

    zope.interface.implements(gocept.amqprun.interfaces.IChannelManager)

    def __init__(self, handler):
        pika.channel.Channel.__init__(self, handler)
        self._gocept_amqprun_refcount = ThreadSafeCounter()

    def acquire(self):
        self._gocept_amqprun_refcount.inc()

    def release(self):
        self._gocept_amqprun_refcount.dec()

    def close_if_possible(self):
        if self._gocept_amqprun_refcount:
            return False
        self.close()
        return True


class ThreadSafeCounter(object):

    def __init__(self):
        self.lock = threading.Lock()
        self.value = 0

    def __nonzero__(self):
        return bool(self.value)

    def inc(self):
        with self.lock:
            self.value += 1

    def dec(self):
        with self.lock:
            self.value -= 1
