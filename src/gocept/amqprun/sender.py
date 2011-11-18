# Copyright (c) 2011 gocept gmbh & co. kg
# See also LICENSE.txt

from gocept.amqprun.server import Connection, AMQPDataManager, Session
import gocept.amqprun.interfaces
import logging
import pika.connection
import select
import threading
import transaction
import zope.interface


log = logging.getLogger(__name__)


class MessageSender(object, pika.connection.NullReconnectionStrategy):

    zope.interface.implements(gocept.amqprun.interfaces.ILoop)

    def __init__(self, connection_parameters):
        self.connection_parameters = connection_parameters
        self.running = False
        self.connection = None
        self.channel = None
        self.local = threading.local()
        self.local.session = None

    def start(self):
        log.info('Starting message sender.')
        self.connection = Connection(self.connection_parameters, self)
        self.connection.finish_init()
        self.running = True
        while self.running:
            self.run_once()
        # closing
        self.connection.ensure_closed()
        self.connection = None

    def run_once(self):
        try:
            self.connection.drain_events()
        except select.error:
            log.error("Error while draining events", exc_info=True)
            self.connection._disconnect_transport(
                "Select error")
            self.channel = None
            return

    def stop(self):
        log.info('Stopping message sender.')
        self.running = False
        self.connection.close()

    def send(self, message):
        session = self._get_session()
        session.send(message)

    def _get_session(self):
        if self.local.session is not None:
            return self.local.session
        self.local.session = Session(self)
        return self.local.session

    def open_channel(self):
        assert self.channel is None
        log.debug('Opening new channel.')
        self.channel = self.connection.channel()

    # Connection Strategy Interface

    def on_connection_open(self, connection):
        assert connection == self.connection
        assert connection.is_alive()
        log.info('AMQP connection opened.')
        with self.connection.lock:
            self.open_channel()
        log.info('Finished connection initialization.')

    def on_connection_closed(self, connection):
        assert connection == self.connection
        if self.connection.connection_close:
            message = self.connection.connection_close.reply_text
        else:
            message = 'no detail available'
        log.info('AMQP connection closed (%s).', message)
        self.channel = None
        if self.running:
            log.info('Reconnecting in 1 s.')
            self.connection.delayed_call(1, self.connection.reconnect)
