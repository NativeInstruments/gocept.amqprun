# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import ZConfig
import asyncore
import gocept.amqprun.interfaces
import gocept.amqprun.settings
import gocept.amqprun.worker
import logging
import os
import pika
import pika.asyncore_adapter
import pika.spec
import pkg_resources
import signal
import socket
import tempfile
import threading
import time
import transaction.interfaces
import zope.component
import zope.configuration.xmlconfig
import zope.interface


log = logging.getLogger(__name__)


class WriteDispatcher(asyncore.file_dispatcher):

    def handle_read(self):
        # Read and discard byte.
        os.read(self.fileno(), 1)


class Connection(pika.AsyncoreConnection):

    _close_now = False

    def __init__(self, *args, **kw):
        self.lock = threading.Lock()
        self._main_thread_lock = threading.RLock()
        self._main_thread_lock.acquire()
        pika.AsyncoreConnection.__init__(self, *args, **kw)

    def connect(self, host, port):
        self.notifier_r, self.notifier_w = os.pipe()
        self.notifier_dispatcher = WriteDispatcher(self.notifier_r)
        self.dispatcher = pika.asyncore_adapter.RabbitDispatcher(self)
        self.dispatcher.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.dispatcher.connect((host, port))

    def drain_events(self):
        # The actual communication takes *only* place in the main thread. If
        # another thread detects that there is data to be written, it notifies
        # the main thread about it using the notifier pipe.
        if self.is_main_thread:
            pika.asyncore_loop(count=1)
            if self._close_now:
                self.close()
        else:
            # Another thread may notify the main thread about changes. Write
            # exactly 1 byte. This corresponds to handle_read() reading exactly
            # one byte.
            if self.outbound_buffer:
                os.write(self.notifier_w, 'W')
                time.sleep(0.05)

    @property
    def is_main_thread(self):
        return self._main_thread_lock.acquire(False)

    def close(self):
        if not self.connection_open:
            return
        if self.is_main_thread:
            pika.AsyncoreConnection.close(self)
            self._main_thread_lock.release()
        else:
            self._close_now = True
            os.write(self.notifier_w, 'C')


class Consumer(object):

    def __init__(self, handler, tasks):
        self.handler = handler
        self.tasks = tasks

    def __call__(self, channel, method, header, body):
        message = Message(header, body, method.delivery_tag)
        log.debug("Adding message: %s", message)
        self.tasks.put(self.handler(message))


class MessageReader(object):

    zope.interface.implements(gocept.amqprun.interfaces.ILoop)

    def __init__(self, hostname):
        self.hostname = hostname
        self.tasks = Queue.Queue()
        self.running = False

    def start(self):
        log.info('starting message consumer for %s' % self.hostname)
        self.connection = Connection(pika.ConnectionParameters(self.hostname))
        with self.connection.lock:
            self.channel = self.connection.channel()
            self._declare_and_bind_queues()
            self.running = True
        while self.running:
            try:
                self.connection.drain_events()
            except KeyboardInterrupt:
                self.stop()
        self.connection.close()

    def stop(self):
        self.running = False
        self.connection.close()

    def create_session(self, handler):
        session = Session()
        dm = AMQPDataManager(self, handler.message, session)
        transaction.get().join(dm)
        return session

    def _declare_and_bind_queues(self):
        for name, declaration in zope.component.getUtilitiesFor(
            gocept.amqprun.interfaces.IHandlerDeclaration):
            log.info("Declaring queue: %s", declaration.queue_name)
            self.channel.queue_declare(
                queue=declaration.queue_name, durable=True,
                exclusive=False, auto_delete=False)
            log.info("Binding queue: %s to routing key %s",
                     declaration.queue_name, declaration.routing_key)
            self.channel.queue_bind(
                queue=declaration.queue_name, exchange='amq.topic',
                routing_key=declaration.routing_key)
            self.channel.basic_consume(
                Consumer(declaration, self.tasks),
                queue=declaration.queue_name)


class Message(object):

    zope.interface.implements(gocept.amqprun.interfaces.IMessage)

    exchange = 'amq.topic'

    def __init__(self, header, body, delivery_tag=None, routing_key=None):
        if not isinstance(header, pika.spec.BasicProperties):
            header = self.convert_header(header)
        self.header = header
        self.body = body
        self.delivery_tag = delivery_tag
        self.routing_key = (
            unicode(routing_key).encode('UTF-8') if routing_key else None)
        gocept.amqprun.interfaces.IMessage.validateInvariants(self)

    def convert_header(self, header):
        header = header.copy()
        result = pika.spec.BasicProperties()
        result.timestamp = time.time()
        result.delivery_mode = 2 # persistent
        for key in dir(result):
            value = header.pop(key, self)
            if isinstance(value, unicode):
                value = value.encode('UTF-8')
            if value is not self:
                setattr(result, key, value)
        result.headers = header
        return result


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

    def abort(self, transaction):
        # Called on transaction.abort() *and* on errors in tpc_vote/tpc_finish
        # of any datamanger *if* self has *not* voted, yet.
        #
        if self._tpc_begin:
            # If a TPC has begun already, do nothing. tpc_abort handles
            # everythin we do as well.
            return
        with self.connection_lock:
            # XXX reject is not implemented by RabbitMQ
            #self._channel.basic_reject(self.message.delivery_tag)
            self.session.clear()

    def tpc_begin(self, transaction):
        log.debug("Acquire commit lock for %s", transaction)
        self.connection_lock.acquire()
        self._tpc_begin = True
        self._channel.tx_select()

    def commit(self, transaction):
        log.debug("Acking")
        self._channel.basic_ack(self.message.delivery_tag)
        for message in self.session.messages:
            log.debug("Publishing %s", message)
            self._channel.basic_publish(
                message.exchange, message.routing_key,
                message.body, message.header)

    def tpc_abort(self, transaction):
        self._channel.tx_rollback()
        # XXX reject is not implemented by RabbitMQ
        #self._channel.basic_reject(self.message.delivery_tag)
        self.session.clear()
        self.connection_lock.release()

    def tpc_vote(self, transaction):
        log.debug("tx_commit")
        self._channel.tx_commit()

    def tpc_finish(self, transaction):
        log.debug("releasing commit lock")
        self.connection_lock.release()

    def sortKey(self):
        return '\xff'


# Holds a reference to the reader stared by main(). This is to make testing
# easier where main() is started in a thread.
main_reader = None


def main(config_file):
    global main_reader
    schema = ZConfig.loadSchemaFile(pkg_resources.resource_stream(
        __name__, 'schema.xml'))
    conf, handler = ZConfig.loadConfigFile(schema, open(config_file))
    conf.eventlog.startup()
    # Provide utility before xml config to allow components configured via ZCML
    # to use the utiltiy.
    settings = gocept.amqprun.settings.Settings()
    zope.component.provideUtility(settings)
    if conf.settings:
        settings.update(conf.settings)

    zope.configuration.xmlconfig.file(conf.worker.component_configuration)

    reader = MessageReader(conf.amqp_server.hostname)
    main_reader = reader

    workers = []
    for i in range(conf.worker.amount):
        worker = gocept.amqprun.worker.Worker(
            reader.tasks, reader.create_session)
        worker.start()
        workers.append(worker)

    reader.start()  # this blocks until reader is stopped from outside.

    log.info('waiting for workers to shut down')
    # each worker might block in its queue.get() until the timeout is reached
    for worker in workers:
        worker.stop()
    main_reader = None
    log.info('exit')
