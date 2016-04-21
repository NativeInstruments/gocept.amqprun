import gocept.amqprun.interfaces
import logging
import pika.channel
import sys
import threading
import zope.interface


log = logging.getLogger(__name__)


class Channel(pika.channel.Channel):

    zope.interface.implements(gocept.amqprun.interfaces.IChannelManager)

    def __init__(self, handler):
        pika.channel.Channel.__init__(self, handler)
        self._gocept_amqprun_refcount = ThreadSafeCounter()

    def acquire(self):
        self._gocept_amqprun_refcount.inc()
        log.debug(
            'Channel[%s] acquired by %s, refcount %s',
            self.handler.channel_number, caller_name(),
            self._gocept_amqprun_refcount)

    def release(self):
        self._gocept_amqprun_refcount.dec()
        log.debug(
            'Channel[%s] released by %s, refcount %s',
            self.handler.channel_number, caller_name(),
            self._gocept_amqprun_refcount)

    def close_if_possible(self):
        if self._gocept_amqprun_refcount:
            return False
        log.info('Closing channel %s', self.handler.channel_number)
        self.close()
        return True

    @property
    def connection_lock(self):
        return self.handler.connection.lock


class ThreadSafeCounter(object):
    """Counter which is thread-safe and cannot fall below zero."""

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
            if self.value < 0:
                raise gocept.amqprun.interfaces.CounterBelowZero()

    def __str__(self):
        return str(self.value)


def caller_name():
    frame = sys._getframe(2)
    function = frame.f_code.co_name
    obj = frame.f_locals.get('self')
    if obj is None:
        return function
    else:
        return '%r:%s' % (obj, function)
