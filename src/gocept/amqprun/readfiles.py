# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import gocept.filestore
import logging
import threading
import time
import transaction
import zope.component
import zope.configuration.fields
import zope.interface
import zope.schema


log = logging.getLogger(__name__)


class FileStoreReader(threading.Thread):

    def __init__(self, path, routing_key):
        self.running = False
        self.routing_key = routing_key
        self.filestore = gocept.filestore.FileStore(path)
        self.filestore.prepare()
        super(FileStoreReader, self).__init__()
        self.daemon = True

    def run(self):
        log.info('starting')
        self.running = True
        while self.running:
            self.scan()
            time.sleep(1)

    def stop(self):
        log.info('stopping')
        self.running = False
        self.join()

    def scan(self):
        for filename in self.filestore.list('new'):
            log.debug('sending %r to %r' % (filename, self.routing_key))
            self.send(open(filename).read())
            self.filestore.move(filename, 'new', 'cur')
            transaction.commit()

    def send(self, body):
        # XXX make content-type configurable?
        message = gocept.amqprun.message.Message(
            {'content_type': 'text/xml'}, body, routing_key=self.routing_key)
        sender = zope.component.getUtility(gocept.amqprun.interfaces.ISender)
        sender.send(message)


class IReadFilesDirective(zope.interface.Interface):

    directory = zope.configuration.fields.Path(
        title=u"Path to the directory from which to read files")

    routing_key = zope.schema.TextLine(
        title=u"Routing key to send to")


def readfiles_directive(_context, directory, routing_key):
    reader = FileStoreReader(directory, routing_key)
    zope.component.zcml.subscriber(
        _context,
        for_=(gocept.amqprun.interfaces.IProcessStarting,),
        handler=lambda event: reader.start())
    zope.component.zcml.subscriber(
        _context,
        for_=(gocept.amqprun.interfaces.IProcessStopping,),
        handler=lambda event: reader.stop())
