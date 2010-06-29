# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import ZConfig
import gocept.amqprun.worker
import logging
import pika
import pkg_resources
import threading
import zope.dottedname.resolve


log = logging.getLogger(__name__)


class MessageReader(object):

    # XXX make configurable?
    queue_name = 'test'

    def __init__(self, hostname):
        self.hostname = hostname
        self.lock = threading.Lock()
        self.tasks = Queue.Queue()
        self.running = False

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
            pika.asyncore_loop(count=1)

    def handle_message(self, channel, method, header, body):
        self.tasks.put(Message(channel, method, header, body))

    def stop(self):
        self.running = False
        self.connection.close()

    def create_datamanager(self):
        pass


class Message(object):

    def __init__(self, channel, method, header, body):
        self.channel = channel
        self.method = method
        self.header = header
        self.body = body


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
