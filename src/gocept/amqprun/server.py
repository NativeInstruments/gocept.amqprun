# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import asyncore
import gocept.amqprun.interfaces
import gocept.amqprun.message
import gocept.amqprun.worker
import logging
import os
import pika
import pika.asyncore_adapter
import pika.spec
import select
import threading
import time
import transaction.interfaces
import zope.component
import zope.interface


log = logging.getLogger(__name__)


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


class MessageReader(object, pika.connection.NullReconnectionStrategy):

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

    def create_session(self, handler):
        session = Session()
        dm = AMQPDataManager(self, handler.message, session)
        transaction.get().join(dm)
        return session

    def open_channel(self):
        assert self.channel is None
        log.debug('Opening new channel')
        self.channel = self.connection.channel()
        self._declare_and_bind_queues()
        self._channel_opened = time.time()

    def switch_channel(self):
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


class Session(object):

    zope.interface.implements(gocept.amqprun.interfaces.ISession)

    def __init__(self):
        self.messages = []

    def send(self, message):
        self.messages.append(message)

    def clear(self):
        self.messages[:] = []


class AMQPDataManager(object):

    zope.interface.implements(transaction.interfaces.IDataManager)

    transaction_manager = None

    def __init__(self, reader, message, session):
        self.connection_lock = reader.connection.lock
        self._channel = reader.channel
        self.message = message
        self.session = session
        self._tpc_begin = False
        gocept.amqprun.interfaces.IChannelManager(self._channel).acquire()

    def abort(self, transaction):
        # Called on transaction.abort() *and* on errors in tpc_vote/tpc_finish
        # of any datamanger *if* self has *not* voted, yet.
        #
        if self._tpc_begin:
            # If a TPC has begun already, do nothing. tpc_abort handles
            # everythin we do as well.
            return
        with self.connection_lock:
            self.session.clear()
            gocept.amqprun.interfaces.IChannelManager(self._channel).release()

    def tpc_begin(self, transaction):
        log.debug("Acquire commit lock for %s", transaction)
        self.connection_lock.acquire()
        self._tpc_begin = True
        self._channel.tx_select()

    def commit(self, transaction):
        log.debug("Ack'ing message %s", self.message.delivery_tag)
        self._channel.basic_ack(self.message.delivery_tag)
        for message in self.session.messages:
            log.debug("Publishing message (%s)", message.routing_key)
            if not message.header.correlation_id:
                message.header.correlation_id = self.message.header.message_id
            self._set_references(message)
            self._channel.basic_publish(
                message.exchange, message.routing_key,
                message.body, message.header)

    def _set_references(self, message):
        if not self.message.header.message_id:
            return
        if message.header.headers is None:
            message.header.headers = {}
        if 'references' not in message.header.headers:
            if (self.message.header.headers and
                self.message.header.headers['references']):
                parent_references = (
                    self.message.header.headers['references'] + '\n')
            else:
                parent_references = ''
            message.header.headers['references'] = (
                parent_references +
                self.message.header.message_id)

    def tpc_abort(self, transaction):
        log.debug('tx_rollback')
        self._channel.tx_rollback()
        # The original idea was to reject the message here. Reject with requeue
        # immediately re-queues the message in the current rabbitmq
        # implementation (2.1.1). We let the message dangle until the channel
        # is closed. At this point the message is re-queued and re-submitted to
        # us.
        self.session.clear()
        gocept.amqprun.interfaces.IChannelManager(self._channel).release()
        self.connection_lock.release()

    def tpc_vote(self, transaction):
        log.debug("tx_commit")
        self._channel.tx_commit()

    def tpc_finish(self, transaction):
        log.debug("releasing commit lock")
        gocept.amqprun.interfaces.IChannelManager(self._channel).release()
        self.connection_lock.release()

    def sortKey(self):
        # Try to sort last, so that we vote last.
        return "~gocept.amqprun:%f" % time.time()
