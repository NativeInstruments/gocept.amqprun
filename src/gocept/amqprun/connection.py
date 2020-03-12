class Parameters(object):
    """Connection parameters with sensible defaults."""

    def __init__(self, heartbeat=0, hostname=NotImplemented,
                 password=None, port=5672,
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
