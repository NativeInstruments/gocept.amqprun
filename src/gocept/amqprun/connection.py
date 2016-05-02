import asyncore
import gocept.amqprun.channel
import logging
import os
import pika
import pika.adapters.asyncore_connection
# import socket
import threading
import time


log = logging.getLogger(__name__)


class Parameters(object):
    """Connection parameters with sensible defaults."""

    def __init__(self, heartbeat_interval=0, hostname=NotImplemented,
                 password=None, port=None, username=None, virtual_host="/"):
        self.heartbeat_interval = heartbeat_interval
        self.hostname = hostname
        self.password = password
        self.port = port
        self.username = username
        self.virtual_host = virtual_host


class WriteDispatcher(asyncore.file_dispatcher):

    def handle_read(self):
        # Read and discard byte.
        os.read(self.fileno(), 1)


class RabbitDispatcher(pika.adapters.asyncore_connection.PikaDispatcher):
    """Support more than one Server in a single process

    by using a separate socket_map per Connection.
    """

    def __init__(self, connection):
        asyncore.dispatcher.__init__(self, map=connection.socket_map)
        self.connection = connection


class Connection(pika.SelectConnection):

    _close_now = False

    def __init__(self, parameters, on_open_callback=None,
                 on_close_callback=None):
        self.lock = threading.Lock()
        self._main_thread_lock = threading.RLock()
        self._main_thread_lock.acquire()
        self.socket_map = {}
        self.notifier_dispatcher = None
        self.on_open_callback = on_open_callback
        self.on_close_callback = on_close_callback

        if isinstance(parameters, pika.connection.Parameters):
            self._pika_parameters = parameters
        else:
            credentials = None
            if parameters.username and parameters.password:
                credentials = pika.PlainCredentials(
                    parameters.username, parameters.password)
            self._pika_parameters = pika.ConnectionParameters(
                        host=parameters.hostname,
                        port=int(parameters.port),
                        virtual_host=parameters.virtual_host,
                        credentials=credentials,
                        heartbeat_interval=int(
                            parameters.heartbeat_interval))

    def finish_init(self):
        pika.SelectConnection.__init__(
            self,
            self._pika_parameters,
            on_open_callback=self.on_open_callback,
            on_close_callback=self.on_close_callback,
        )
        if self.connection_state != self.CONNECTION_PROTOCOL:
            raise RuntimeError(
                'Connection not alive after connect, maybe the credentials '
                'are wrong.')

    def connect(self):
        if not self.notifier_dispatcher:
            self.notifier_r, self.notifier_w = os.pipe()
            self.notifier_dispatcher = WriteDispatcher(
                self.notifier_r, map=self.socket_map)

        # host = self._pika_parameters.host
        # port = self._pika_parameters.port
        # not calling super since we need to use our subclassed
        # RabbitDispatcher
        # self.dispatcher = RabbitDispatcher(self)
        # self.dispatcher.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.dispatcher.connect((host, port or pika.spec.PORT))
        super(Connection, self).connect()

    def reconnect(self):
        self.connect()
        self.notify()

    def notify(self):
        os.write(self.notifier_w, 'R')

    def drain_events(self, timeout=None):
        # The actual communication takes *only* place in the main thread. If
        # another thread detects that there is data to be written, it notifies
        # the main thread about it using the notifier pipe.
        if self.is_main_thread:
            self.ioloop.start()
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

    def close(self, *args, **kw):
        if not self.is_open:
            return
        super(Connection, self).close(*args, **kw)
        if self.is_main_thread:
            self.notifier_dispatcher.close()
            self._main_thread_lock.release()
        else:
            self._close_now = True
            self.notify()

    #  taken from pika 0.9.14 core
    def _create_channel(self, channel_number, on_open_callback):
        """Create a new channel using the specified channel number and calling
        back the method specified by on_open_callback

        :param int channel_number: The channel number to use
        :param method on_open_callback: The callback when the channel is opened

        """
        return gocept.amqprun.channel.Channel(
            self, channel_number, on_open_callback)
