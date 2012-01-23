# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import gocept.filestore
import logging
import time
import zope.component
import zope.interface


log = logging.getLogger(__name__)


class FileStoreReader(object):

    zope.interface.implements(gocept.amqprun.interfaces.ILoop)

    def __init__(self, path, routing_key):
        self.running = False
        self.routing_key = routing_key
        self.filestore = gocept.filestore.FileStore(path)
        self.filestore.prepare()

    def start(self):
        self.running = True
        while self.running:
            self.scan()
            time.sleep(1)

    def stop(self):
        self.running = False

    def scan(self):
        for filename in self.filestore.list('new'):
            log.debug('reading %r' % filename)
            self.send(open(filename).read())
            self.filestore.move(filename, 'new', 'cur')

    def send(self, body):
        # XXX make content-type configurable?
        message = gocept.amqprun.message.Message(
            {'content_type': 'text/xml'}, body, routing_key=self.routing_key)
        sender = zope.component.getUtility(gocept.amqprun.interfaces.ISender)
        sender.send(message)
