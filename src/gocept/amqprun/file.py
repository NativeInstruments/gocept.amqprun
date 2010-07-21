# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import amqplib.client_0_8 as amqp
import gocept.filestore
import time


class FileStoreReader(object):

    def __init__(self, path, hostname, routing_key):
        self.running = False
        self.connection = amqp.Connection(host=hostname)
        self.channel = self.connection.channel()
        self.routing_key = routing_key
        self.filestore = gocept.filestore.FileStore(path)
        self.filestore.prepare()

    def start(self):
        while self.running:
            self.scan()
            time.sleep(1)

    def stop(self):
        self.running = False
        self.channel.close()
        self.connection.close()

    def scan(self):
        for filename in self.filestore.list('new'):
            self.send(open(filename).read())
            self.filestore.move(filename, 'new', 'cur')

    def send(self, body):
        # XXX make content-type configurable?
        self.channel.basic_publish(
            amqp.Message(body, content_type='text/xml'),
            'amq.topic', routing_key=self.routing_key)
