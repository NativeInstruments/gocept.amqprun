import gocept.amqprun.connection
import gocept.amqprun.interfaces
import gocept.amqprun.message
import gocept.amqprun.session
import gocept.amqprun.worker
import kombu
import logging
import select
import time
import zope.component
import zope.event
import zope.interface


log = logging.getLogger(__name__)


class Consumer(object):

    def __init__(self, handler):
        self.handler = handler

    def __call__(self, message):
        channel = message.channel
        delivery_tag = message.delivery_info['delivery_tag']
        routing_key = message.delivery_info['routing_key']
        amqprun_message = gocept.amqprun.message.Message(
            message.properties, message.body,
            delivery_tag, routing_key, channel)
        log.debug("Channel[%s] received message %s via routing key '%s'",
                  channel.channel_id, delivery_tag, routing_key)
        session = gocept.amqprun.session.Session(channel, amqprun_message)
        worker = gocept.amqprun.worker.Worker(session, self.handler)
        return worker()


class Server(object):  # pika.connection.NullReconnectionStrategy

    zope.interface.implements(
        gocept.amqprun.interfaces.ISender)

    CHANNEL_LIFE_TIME = 360
    exit_status = 0

    def __init__(self, connection_parameters, setup_handlers=True):
        self.connection_parameters = connection_parameters
        # self.local = threading.local()
        # self._running = threading.Event()
        self.connection = None
        self.channel = None
        self._old_channel = None
        self.setup_handlers = setup_handlers
        self.bound_consumers = {}

    # @property
    # def running(self):
    #     return self._running.is_set()

    # @running.setter
    # def running(self, value):
    #     if value:
    #         self._running.set()
    #     else:
    #         self._running.clear()

    # def wait_until_running(self, timeout=None):
    #     self._running.wait(timeout)
    #     return self.running

    def connect(self):
        log.info('Starting message reader.')
        params = {
            key: getattr(self.connection_parameters, key)
            for key in self.connection_parameters.getSectionAttributes()
        }
        self.connection = kombu.Connection(**params)
        self.connection.ensure_connection(max_retries=1)
        self.on_connection_open(self.connection)

    def start(self):
        self.connect()
        zope.event.notify(gocept.amqprun.interfaces.ProcessStarted())
        try:
            while True:
                self.run_once()
                time.sleep(1)  # This might get a polling algorithm.
        except:  # noqa
            # closing
            self.connection.close()
            # XXX self.connection.ensure_closed()
            self.connection = None

    def run_once(self, timeout=None):
        try:
            for queue, consumer in self.bound_consumers.items():
                message = self.channel.basic_get(queue)
                if message:
                    consumer(message)
        except select.error:
            log.error("Error while draining events", exc_info=True)
            self.connection._disconnect_transport(
                "Select error")
            self.channel = self._old_channel = None
            return
        if time.time() - self._channel_opened > self.CHANNEL_LIFE_TIME:
            self.switch_channel()

    # def stop(self):
    #     log.info('Stopping message reader.')
    #     self.running = False
    #     self.connection.close()

    def send(self, message):
        self._send_session.send(message)

    @property
    def _send_session(self):
        if not hasattr(self, 'session'):
            self.session = gocept.amqprun.session.Session(self.send_channel)
        return self.session

    def open_channel(self):
        assert self.channel is None
        log.debug('Opening new channel')
        try:
            self.channel = self.connection.channel()
        except Exception:  # pika.exceptions.ChannelClosed
            log.debug('Opening new channel aborted due to closed connection,'
                      ' since a reconnect should happen soon anyway.')
            return
        self.bound_consumers = self._declare_and_bind_queues()
        self._channel_opened = time.time()

    def switch_channel(self):
        if not zope.component.getUtilitiesFor(
                gocept.amqprun.interfaces.IHandler):
            return
        log.info('Switching to a new channel')
        for consumer_tag in self.channel.callbacks.keys():
            self.channel.basic_cancel(consumer_tag)
        self.channel.close()
        self.channel = None
        self.open_channel()

    def _declare_and_bind_queues(self):
        if not self.setup_handlers:
            return
        assert self.channel is not None
        bound_queues = {}
        bound_consumers = {}
        for name, handler in zope.component.getUtilitiesFor(
                gocept.amqprun.interfaces.IHandler):
            queue_name = unicode(handler.queue_name).encode('UTF-8')
            if queue_name in bound_queues:
                raise ValueError(
                    'Queue %r already bound to handler %r for %r' % (
                        queue_name, handler, bound_queues[queue_name]))
            else:
                bound_queues[queue_name] = handler.routing_key
            handler_args = handler.arguments or {}
            arguments = dict(
                (unicode(key).encode('UTF-8'), unicode(value).encode('UTF-8'))
                for key, value in handler_args.iteritems())
            log.info(
                "Channel[%s]: Handling routing key(s) '%s' on queue '%s'"
                " via '%s'",
                self.channel.channel_id, handler.routing_key, queue_name, name)
            self.channel.queue_declare(
                queue=queue_name, durable=True,
                exclusive=False, auto_delete=False,
                arguments=arguments)
            routing_keys = handler.routing_key
            if not isinstance(routing_keys, list):
                routing_keys = [routing_keys]
            for routing_key in routing_keys:
                routing_key = unicode(routing_key).encode('UTF-8')
                self.channel.queue_bind(
                    queue=queue_name, exchange='amq.topic',
                    routing_key=routing_key)
            bound_consumers[queue_name] = Consumer(handler)

        return bound_consumers

    # Connection Strategy Interface

    def on_connection_open(self, connection):
        assert connection == self.connection
        assert connection.ensure_connection(max_retries=1)
        log.info('AMQP connection opened.')
        # with self.connection.lock:
        self.open_channel()
        self.send_channel = self.connection.channel()
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
