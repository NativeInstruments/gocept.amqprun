import asyncore
import gocept.amqprun.channel
import logging
import os
import pika
import pika.asyncore_adapter
import pika.channel
import socket
import threading
import time


log = logging.getLogger(__name__)


class Parameters(object):
    """Connection parameters with sensible defaults."""

    def __init__(self, heartbeat_interval=0, hostname=NotImplemented,
                 password=None, port=pika.spec.PORT, username=None,
                 virtual_host="/"):
        self.heartbeat_interval = heartbeat_interval
        self.hostname = hostname
        self.password = password
        self.port = int(port)
        self.username = username
        self.virtual_host = virtual_host


class WriteDispatcher(asyncore.file_dispatcher):

    def handle_read(self):
        # Read and discard byte.
        os.read(self.fileno(), 1)


class RabbitDispatcher(pika.asyncore_adapter.RabbitDispatcher):

    def __init__(self, connection):
        asyncore.dispatcher.__init__(self, map=connection.socket_map)
        self.connection = connection


class Connection(pika.AsyncoreConnection):

    _close_now = False

    def __init__(self, parameters, reconnection_strategy=None):
        self.lock = threading.Lock()
        self._main_thread_lock = threading.RLock()
        self._main_thread_lock.acquire()
        self.socket_map = {}
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
        if not self.is_alive():
            raise RuntimeError(
                'Connection not alive after connect, maybe the credentials '
                'are wrong.')

    def connect(self, host, port):
        if not self.notifier_dispatcher:
            self.notifier_r, self.notifier_w = os.pipe()
            self.notifier_dispatcher = WriteDispatcher(
                self.notifier_r, map=self.socket_map)

        # not calling super since we need to use our subclassed
        # RabbitDispatcher
        self.dispatcher = RabbitDispatcher(self)
        self.dispatcher.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dispatcher.connect((host, port))

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
            pika.asyncore_loop(self.socket_map, count=1, timeout=timeout)
            if self._close_now:
                self.close()
        else:
            # Another thread may notify the main thread about changes. Write
            # exactly 1 byte. This corresponds to handle_read() reading exactly
            # one byte.
            if self.outbound_buffer:
                self.notify()
                time.sleep(0.05)

    def channel(self):
        return gocept.amqprun.channel.Channel(
            pika.channel.ChannelHandler(self))

    @property
    def is_main_thread(self):
        return self._main_thread_lock.acquire(False)

    def close(self, *args, **kw):
        if not self.connection_open:
            return
        if self.is_main_thread:
            pika.AsyncoreConnection.close(self, *args, **kw)
            self.notifier_dispatcher.close()
            self._main_thread_lock.release()
        else:
            self._close_now = True
            self.notify()
