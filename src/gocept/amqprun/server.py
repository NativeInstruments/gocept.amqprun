# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import ZConfig
import asyncore
import gocept.amqprun.interfaces
import gocept.amqprun.worker
import logging
import os
import pika
import pika.asyncore_adapter
import pkg_resources
import signal
import socket
import tempfile
import threading
import time
import transaction.interfaces
import zope.dottedname.resolve
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
        self._loop_lock = threading.RLock()
        self._loop_lock.acquire()
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
        if self._loop_lock.acquire(False):
            # This is true for the main thread, which opened the connection
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

    def close(self):
        if not self.connection_open:
            return
        if self._loop_lock.acquire(False):
            pika.AsyncoreConnection.close(self)
            self._loop_lock.release()
        else:
            self._close_now = True
            os.write(self.notifier_w, 'C')


class Consumer(object):

    def __init__(self, handler, tasks):
        self.handler = handler
        self.tasks = tasks

    def __call__(self, channel, method, header, body):
        log.debug("Adding message %s to queue", body)
        self.tasks.put(self.handler(Message(channel, method, header, body)))


class MessageReader(object):

    def __init__(self, hostname):
        self.hostname = hostname
        self.lock = threading.Lock()
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
            self.connection.drain_events()
        self.connection.close()

    def stop(self):
        self.running = False
        self.connection.close()

    def create_datamanager(self, message):
        return AMQPDataManager(self.connection.lock, message)

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

    def __init__(self, channel, method, header, body):
        self.channel = channel
        self.method = method
        self.header = header
        self.body = body


class AMQPDataManager(object):

    zope.interface.implements(transaction.interfaces.IDataManager)

    transaction_manager = None

    def __init__(self, connection_lock, message):
        self.connection_lock = connection_lock
        self.message = message
        self._channel = message.channel

    def abort(self, transaction):
        with self.connection_lock:
            self._channel.tx_reject(self.message.method.delivery_tag)

    def tpc_begin(self, transaction):
        log.debug("Acquire commit lock for %s", transaction)
        self.connection_lock.acquire()
        self._channel.tx_select()

    def commit(self, transaction):
        log.debug("Acking")
        self._channel.basic_ack(self.message.method.delivery_tag)

    def tpc_abort(self, transaction):
        self._channel.tx_rollback()
        self._channel.tx_reject(self.message.method.delivery_tag)
        self.connection_lock.release()

    def tpc_vote(self, transaction):
        log.debug("tx_commit")
        self._channel.tx_commit()

    def tpc_finish(self, transaction):
        log.debug("releasing commit lock")
        self.connection_lock.release()

    def sortKey(self):
        return '\xff'


def main(config_file):
    schema = ZConfig.loadSchemaFile(pkg_resources.resource_stream(
        __name__, 'schema.xml'))
    conf, handler = ZConfig.loadConfigFile(schema, open(config_file))
    conf.eventlog.startup()
    reader = MessageReader(conf.amqp_server.hostname)

    handler = zope.dottedname.resolve.resolve(conf.worker.handler)
    for i in range(conf.worker.amount):
        worker = gocept.amqprun.worker.Worker(
            reader.tasks, handler, reader.create_datamanager)
        worker.start()

    reader.start()
