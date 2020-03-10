import kombu
import logging


log = logging.getLogger(__name__)


class Parameters(object):
    """Connection parameters with sensible defaults."""

    def __init__(self, heartbeat=0, hostname=NotImplemented,
                 password=None, port=5672,  # pika.spec.PORT
                 username=None,
                 virtual_host="/"):
        self.heartbeat = heartbeat
        self.hostname = hostname
        self.password = password
        self.port = int(port)
        self.userid = username
        self.virtual_host = virtual_host

    def getSectionAttributes(self):
        return self.__dict__.keys()


# class WriteDispatcher(asyncore.file_dispatcher):

#     def handle_read(self):
#         # Read and discard byte.
#         os.read(self.socket.fileno(), 1)


# class RabbitDispatcher(object):  # pika.asyncore_adapter.RabbitDispatcher

#     def __init__(self, connection):
#         asyncore.dispatcher.__init__(self, map=connection.socket_map)
#         self.connection = connection


# from kombu.asynchronous import Hub
# hub = Hub()


# class Connection(object):  # pika.AsyncoreConnection
class Connection(kombu.Connection):
    """Conn."""

    # _close_now = False

    def __init__(self, *args, **kw):
        # self.lock = threading.Lock()
        # self._main_thread_lock = threading.RLock()
        # self._main_thread_lock.acquire()
        # self.socket_map = {}
        # self.notifier_dispatcher = None
        super(Connection, self).__init__(*args, **kw)
        # credentials = None
        # if parameters.username and parameters.password:
        #     credentials = object(  # pika.PlainCredentials(
        #         parameters.username, parameters.password)
        # self._pika_parameters = object(  # pika.ConnectionParameters(
        #     host=parameters.hostname,
        #     port=parameters.port,
        #     virtual_host=parameters.virtual_host,
        #     credentials=credentials,
        #     heartbeat=parameters.heartbeat_interval)
        # self._reconnection_strategy = reconnection_strategy

    def finish_init(self):
        # pika.AsyncoreConnection.__init__(
        #     self, self._pika_parameters, wait_for_open=True,
        #     reconnection_strategy=self._reconnection_strategy)

        # self.register_with_event_loop(hub)
        if not self.ensure_connection(max_retries=1):
            raise RuntimeError(
                'Connection not alive after connect, maybe the credentials '
                'are wrong.')

    # def connect(self, host, port):
        # if not self.notifier_dispatcher:
        #     self.notifier_r, self.notifier_w = os.pipe()
        #     self.notifier_dispatcher = WriteDispatcher(
        #         self.notifier_r, map=self.socket_map)

#         # not calling super since we need to use our subclassed
#         # RabbitDispatcher
#         self.dispatcher = RabbitDispatcher(self)
#         self.dispatcher.create_socket(socket.AF_INET, socket.SOCK_STREAM)
#         self.dispatcher.connect((host, port))

#     def reconnect(self):
#         # pika.AsyncoreConnection.reconnect(self)
#         self.notify()

    # def notify(self):
    #     os.write(self.notifier_w, 'R')

#     def channel(self):
#         return gocept.amqprun.channel.Channel(
#             object(self)  # pika.channel.ChannelHandler(self)
#         )

    # @property
    # def is_main_thread(self):
    #     return self._main_thread_lock.acquire(False)

    # def close(self, *args, **kw):
    #     # if not self.connection_open:
    #     #     return
    #     if self.is_main_thread:
    #         # pika.AsyncoreConnection.close(self, *args, **kw)
    #         self.notifier_dispatcher.close()
    #         self._main_thread_lock.release()
    #     else:
    #         self._close_now = True
    #         self.notify()
