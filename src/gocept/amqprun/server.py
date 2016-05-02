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
            header, body, method.delivery_tag, method.routing_key, channel)
        log.debug("Received message %s via routing key '%s'",
                  message.delivery_tag, method.routing_key)
        session = gocept.amqprun.session.Session(channel, message)
        self.tasks.put((session, self.handler))
        gocept.amqprun.interfaces.IChannelManager(channel).acquire()


class Server(object):

    zope.interface.implements(
        gocept.amqprun.interfaces.ILoop,
        gocept.amqprun.interfaces.ISender)

    CHANNEL_LIFE_TIME = 360
    exit_status = 0

    keep_running = False

    def __init__(self, connection_parameters, setup_handlers=True):
        self.connection_parameters = connection_parameters
        self.tasks = Queue.Queue()
        self.local = threading.local()
        self._send_channel_initialized = threading.Event()
        self._ready_to_consume = threading.Event()
        self.connection = None
        self.channel = None
        self._old_channel = None
        self._switching_channels = False
        self.setup_handlers = setup_handlers

    def wait_until_running(self, timeout=None):
        return (self._send_channel_initialized.wait(timeout) and
                (not self.setup_handlers or
                 self._ready_to_consume.wait(timeout)))

    def start(self):
        log.info('Starting message reader.')
        self.connection = gocept.amqprun.connection.Connection(
            self.connection_parameters,
            on_open_callback=self.on_connection_open,
            on_close_callback=self.on_connection_closed)
        self.connection.finish_init()
        self.keep_running = True
        while self.keep_running:
            self.run_once()
        # closing
        self.connection._ensure_closed()
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
        if self.connection.is_open:
            if time.time() - self._channel_opened > self.CHANNEL_LIFE_TIME:
                self.switch_channel()
            self.close_old_channel()

    def stop(self):
        log.info('Stopping message reader.')
        self.keep_running = False
        self.connection.close()

    def send(self, message):
        self._send_session.send(message)

    @property
    def _send_session(self):
        if not hasattr(self.local, 'session'):
            self.local.session = gocept.amqprun.session.Session(
                self.send_channel)
        return self.local.session

    def open_channel(self):
        log.debug('Opening new channel')
        try:
            channel = self.connection.channel(self._declare_and_bind_queues)
        except pika.exceptions.ChannelClosed:
            log.debug('Opening new channel aborted due to closed connection,'
                      ' since a reconnect should happen soon anyway.')
            channel = None
        else:
            self._channel_opened = time.time()
        return channel

    def switch_channel(self):
        if not zope.component.getUtilitiesFor(
                gocept.amqprun.interfaces.IHandler):
            return
        if self._switching_channels:
            return
        locked = self.connection.lock.acquire(False)
        if not locked:
            log.debug(
                'Want to switch channel, but connection lock not available;'
                ' will try again later.')
            return
        try:
            self._switching_channels = True
            log.info('Switching to a new channel')
            for consumer_tag in self.channel.consumer_tags:
                self.channel.basic_cancel(None, consumer_tag)
            while True:
                try:
                    self.tasks.get(block=False)
                except Queue.Empty:
                    break
                else:
                    gocept.amqprun.interfaces.IChannelManager(
                        self.channel).release()
            self._old_channel = self.channel
            self.channel = self.open_channel()
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

    def _declare_and_bind_queues(self, channel):
        if not self.setup_handlers:
            return
        assert channel is not None
        bound_queues = {}
        for name, handler in zope.component.getUtilitiesFor(
                gocept.amqprun.interfaces.IHandler):
            queue_name = unicode(handler.queue_name).encode('UTF-8')
            if queue_name in bound_queues:
                raise ValueError('Queue %r already bound to handler for %r' % (
                    queue_name, bound_queues[queue_name]))
            else:
                bound_queues[queue_name] = handler.routing_key
            handler_args = handler.arguments or {}
            arguments = dict(
                (unicode(key).encode('UTF-8'), unicode(value).encode('UTF-8'))
                for key, value in handler_args.iteritems())
            log.info(
                "Channel[%s]: Handling routing key(s) '%s' on queue '%s'"
                " via '%s'",
                channel.channel_number,
                handler.routing_key, queue_name, name)
            channel.queue_declare(
                None,
                queue=queue_name, durable=True,
                exclusive=False, auto_delete=False,
                arguments=arguments)
            routing_keys = handler.routing_key
            if not isinstance(routing_keys, list):
                routing_keys = [routing_keys]
            for routing_key in routing_keys:
                routing_key = unicode(routing_key).encode('UTF-8')
                channel.queue_bind(
                    None,
                    queue=queue_name, exchange='amq.topic',
                    routing_key=routing_key)
            channel.basic_consume(
                Consumer(handler, self.tasks), queue=queue_name)
        self._ready_to_consume.set()

    # Connection Strategy Interface

    def on_connection_open(self, connection):
        assert connection == self.connection
        assert connection.is_open
        log.info('AMQP connection opened.')
        with self.connection.lock:
            self.channel = self.open_channel()
        self.send_channel = self.connection.channel(
            lambda *args: self._send_channel_initialized.set())
        log.info('Finished connection initialization')

    def on_connection_closed(self, connection, reply_code, reply_text):
        assert connection == self.connection
        if self.connection.is_closed:
            message = reply_text
        else:
            message = 'no detail available'
        log.info('AMQP connection closed (%s)', message)
        self._old_channel = self.channel = None
        if self.keep_running:
            log.info('Reconnecting in 1s')
            self.connection.add_timeout(1, self.connection.reconnect)
