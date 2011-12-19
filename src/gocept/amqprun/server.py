## Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import gocept.amqprun.connection
import gocept.amqprun.interfaces
import gocept.amqprun.message
import gocept.amqprun.session
import logging
import pika
import select
import threading
import time
import zope.component
import zope.interface


log = logging.getLogger(__name__)


class Consumer(object):

    def __init__(self, handler, tasks):
        self.handler = handler
        self.tasks = tasks

    def __call__(self, channel, method, header, body):
        message = gocept.amqprun.message.Message(
            header, body, method.delivery_tag, method.routing_key)
        log.debug("Received message %s via routing key '%s'",
                  message.delivery_tag, method.routing_key)
        self.tasks.put(self.handler(message))


class Server(object, pika.connection.NullReconnectionStrategy):

    zope.interface.implements(gocept.amqprun.interfaces.ILoop)

    CHANNEL_LIFE_TIME = 360

    def __init__(self, connection_parameters):
        self.connection_parameters = connection_parameters
        self.tasks = Queue.Queue()
        self.running = False
        self.connection = None
        self.channel = None
        self._old_channel = None
        self._switching_channels = False

    def start(self):
        log.info('Starting message reader.')
        self.local = threading.local()
        self.connection = gocept.amqprun.connection.Connection(
            self.connection_parameters, self)
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
            self.channel = self._old_channel = None
            return
        if self.connection.is_alive():
            if time.time() - self._channel_opened > self.CHANNEL_LIFE_TIME:
                self.switch_channel()
            self.close_old_channel()

    def stop(self):
        log.info('Stopping message reader.')
        self.running = False
        self.connection.close()

    def send(self, message):
        session = self.get_session()
        session.send(message)

    def get_session(self, message=None):
        if not hasattr(self.local, 'session'):
            self.local.session = gocept.amqprun.session.Session(self, message)
        return self.local.session

    def open_channel(self):
        assert self.channel is None
        log.debug('Opening new channel')
        self.channel = self.connection.channel()
        self._declare_and_bind_queues()
        self._channel_opened = time.time()

    def switch_channel(self):
        if not zope.component.getUtilitiesFor(
            gocept.amqprun.interfaces.IHandlerDeclaration):
            return False
        if self._switching_channels:
            return False
        log.info('Switching to a new channel')
        locked = self.connection.lock.acquire(False)
        if not locked:
            return False
        try:
            self._switching_channels = True
            for consumer_tag in self.channel.callbacks.keys():
                self.channel.basic_cancel(consumer_tag)
            while True:
                try:
                    self.tasks.get(block=False)
                except Queue.Empty:
                    break
            self._old_channel = self.channel
            self.channel = None
            self.open_channel()
            return True  # Switch successful
        finally:
            self.connection.lock.release()

    def close_old_channel(self):
        if self._old_channel is None:
            return
        locked = self.connection.lock.acquire(False)
        if not locked:
            return False
        try:
            closed = gocept.amqprun.interfaces.IChannelManager(
                self._old_channel).close_if_possible()
            if closed:
                self._old_channel = None
                self._switching_channels = False
        finally:
            self.connection.lock.release()

    def _declare_and_bind_queues(self):
        assert self.channel is not None
        for name, declaration in zope.component.getUtilitiesFor(
            gocept.amqprun.interfaces.IHandlerDeclaration):
            queue_name = unicode(declaration.queue_name).encode('UTF-8')
            decl_args = declaration.arguments or {}
            arguments = dict(
                (unicode(key).encode('UTF-8'),
                 unicode(value).encode('UTF-8')) for key, value
                                                 in decl_args.iteritems())
            log.info(
                "Channel[%s]: Handling routing key(s) '%s' on queue '%s'"
                " via '%s'",
                self.channel.handler.channel_number,
                declaration.routing_key, queue_name, name)
            self.channel.queue_declare(
                queue=queue_name, durable=True,
                exclusive=False, auto_delete=False,
                arguments=arguments)
            routing_keys = declaration.routing_key
            if not isinstance(routing_keys, list):
                routing_keys = [routing_keys]
            for routing_key in routing_keys:
                routing_key = unicode(routing_key).encode('UTF-8')
                self.channel.queue_bind(
                    queue=queue_name, exchange='amq.topic',
                    routing_key=routing_key)
            self.channel.basic_consume(
                Consumer(declaration, self.tasks),
                queue=queue_name)

    # Connection Strategy Interface

    def on_connection_open(self, connection):
        assert connection == self.connection
        assert connection.is_alive()
        log.info('AMQP connection opened.')
        with self.connection.lock:
            self.open_channel()
        log.info('Finished connection initialization')

    def on_connection_closed(self, connection):
        assert connection == self.connection
        if self.connection.connection_close:
            message = self.connection.connection_close.reply_text
        else:
            message = 'no detail available'
        log.info('AMQP connection closed (%s)', message)
        self._old_channel = self.channel = None
        if self.running:
            log.info('Reconnecting in 1s')
            self.connection.delayed_call(1, self.connection.reconnect)
