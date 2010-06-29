# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import pika


class MessageReader(object):

    # XXX make configurable
    hostname = 'localhost'
    queue_name = 'test'

    def __init__(self):
        self.tasks = Queue.Queue()
        self.running = False

    def start(self):
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
        self.tasks.put(Message(header, body))

    def stop(self):
        self.running = False
        self.connection.close()


class Message(object):

    def __init__(self, header, body):
        self.header = header
        self.body = body
