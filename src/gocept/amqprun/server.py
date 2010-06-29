# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import ZConfig
import gocept.amqprun.worker
import logging
import pika
import pkg_resources
import threading
import time
import transaction.interfaces
import zope.dottedname.resolve
import zope.interface


log = logging.getLogger(__name__)


class MessageReader(object):

    # XXX make configurable?
    queue_name = 'test'

    def __init__(self, hostname):
        self.hostname = hostname
        self.lock = threading.Lock()
        self.tasks = Queue.Queue()
        self.running = False
        self.connection_lock = threading.Lock()

    def start(self):
        log.info('starting message consumer for %s' % self.hostname)
        self.connection = pika.AsyncoreConnection(
            pika.ConnectionParameters(self.hostname))
        self.channel = self.connection.channel()
        self.channel.queue_declare(
            queue=self.queue_name, durable=True,
            exclusive=False, auto_delete=False)
        self.channel.queue_bind(queue=self.queue_name, exchange='amq.fanout')
        self.channel.basic_consume(self.handle_message, queue=self.queue_name)

        self.running = True
        while self.running:
            locked = self.connection_lock.acquire(False)
            if locked:
                try:
                    # This makes the loop only delay 1s. But I have the feeling
                    # that we actually should do that differently.
                    self.connection.delayed_call(1, lambda: None)
                    pika.asyncore_loop(count=1)
                finally:
                    self.connection_lock.release()
            else:
                time.sleep(1)
        self.connection.close()

    def handle_message(self, channel, method, header, body):
        log.debug("Adding message %s to queue", body)
        self.tasks.put(Message(channel, method, header, body))

    def stop(self):
        self.running = False

    def create_datamanager(self, message):
        return AMQPDataManager(self.connection_lock, message)


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
        # Does nothing for now.
        pass

    def tpc_begin(self, transaction):
        log.debug("Acquire commit lock for %s", transaction)
        self.connection_lock.acquire()
        self._channel.tx_select()

    def commit(self, transaction):
        log.debug("Acking")
        self._channel.basic_ack(self.message.method.delivery_tag)

    def tpc_abort(self, transaction):
        self._channel.tx_rollback()
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
