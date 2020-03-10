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
