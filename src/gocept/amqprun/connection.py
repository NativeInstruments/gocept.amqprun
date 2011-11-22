# Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import asyncore
import logging
import os
import pika
import threading
import time


log = logging.getLogger(__name__)


class Parameters(object):
    """Connection parameters with sensible defaults."""

    heartbeat_interval = 0
    hostname = NotImplemented
    password = None
    port = None
    username = None
    virtual_host = "/"

    def __init__(self, **kw):
        self.__dict__.update(kw)


class WriteDispatcher(asyncore.file_dispatcher):

    def handle_read(self):
        # Read and discard byte.
        os.read(self.fileno(), 1)


class Connection(pika.AsyncoreConnection):

    _close_now = False

    def __init__(self, parameters, reconnection_strategy=None):
        self.lock = threading.Lock()
        self._main_thread_lock = threading.RLock()
        self._main_thread_lock.acquire()
        self.notifier_dispatcher = None
        credentials = None
        if parameters.username and parameters.password:
            credentials = pika.PlainCredentials(
                parameters.username, parameters.password)
        self._pika_parameters = pika.ConnectionParameters(
            host=parameters.hostname,
            port=parameters.port,
            virtual_host=parameters.virtual_host,
            credentials=credentials,
            heartbeat=parameters.heartbeat_interval)
        self._reconnection_strategy = reconnection_strategy

    def finish_init(self):
        pika.AsyncoreConnection.__init__(
            self, self._pika_parameters, wait_for_open=True,
            reconnection_strategy=self._reconnection_strategy)

    def connect(self, host, port):
        if not self.notifier_dispatcher:
            self.notifier_r, self.notifier_w = os.pipe()
            self.notifier_dispatcher = WriteDispatcher(self.notifier_r)
        pika.AsyncoreConnection.connect(self, host, port)

    def reconnect(self):
        pika.AsyncoreConnection.reconnect(self)
        self.notify()

    def notify(self):
        os.write(self.notifier_w, 'R')

    def drain_events(self, timeout=None):
        # The actual communication takes *only* place in the main thread. If
        # another thread detects that there is data to be written, it notifies
        # the main thread about it using the notifier pipe.
        if self.is_main_thread:
            pika.asyncore_loop(count=1, timeout=timeout)
            if self._close_now:
                self.close()
        else:
            # Another thread may notify the main thread about changes. Write
            # exactly 1 byte. This corresponds to handle_read() reading exactly
            # one byte.
            if self.outbound_buffer:
                self.notify()
                time.sleep(0.05)

    @property
    def is_main_thread(self):
        return self._main_thread_lock.acquire(False)

    def close(self):
        if not self.connection_open:
            return
        if self.is_main_thread:
            pika.AsyncoreConnection.close(self)
            self.notifier_dispatcher.close()
            self._main_thread_lock.release()
        else:
            self._close_now = True
            self.notify()
