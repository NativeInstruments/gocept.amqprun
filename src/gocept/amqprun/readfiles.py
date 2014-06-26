# Copyright (c) 2010-2014 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import gocept.filestore
import logging
import os
import os.path
import signal
import threading
import time
import transaction
import zope.component
import zope.component.zcml
import zope.configuration.fields
import zope.interface
import zope.schema


log = logging.getLogger(__name__)


class FileStoreReader(threading.Thread):

    zope.interface.Interface(gocept.amqprun.interfaces.ILoop)

    def __init__(self, path, routing_key):
        self.running = False
        self.routing_key = routing_key
        self.filestore = gocept.filestore.FileStore(path)
        self.filestore.prepare()
        self.session = Session(self)
        super(FileStoreReader, self).__init__()
        self.daemon = True

    def wait_until_running(self, timeout=None):
        deadline = time.time() + timeout
        while not self.running or time.time() > deadline:
            time.sleep(0.025)
        return self.running

    def run(self):
        log.info('starting to watch %s', self.filestore.path)
        self.running = True
        while self.running:
            try:
                self.scan()
                time.sleep(1)
            except:
                log.error('Unhandled exception, terminating.', exc_info=True)
                os.kill(os.getpid(), signal.SIGUSR1)

    def stop(self):
        log.info('stopping')
        self.running = False
        self.join()

    def scan(self):
        for filename in self.filestore.list('new'):
            transaction.begin()
            log.debug('sending %r to %r' % (filename, self.routing_key))
            try:
                self.send(filename)
                self.session.mark_done(filename)
            except:
                log.error('Sending message failed', exc_info=True)
                transaction.abort()
            else:
                transaction.commit()

    def send(self, filename):
        body = open(filename).read()
        # XXX make content-type configurable?
        message = gocept.amqprun.message.Message(
            {'content_type': 'text/xml',
             'X-Filename': os.path.basename(filename).encode('utf-8')},
            body, routing_key=self.routing_key)
        sender = zope.component.getUtility(gocept.amqprun.interfaces.ISender)
        sender.send(message)


class Session(object):

    def __init__(self, context):
        self.files = []
        self.context = context
        self._needs_to_join = True

    def mark_done(self, filename):
        self._join_transaction()
        self.files.append(filename)

    def commit(self):
        for f in self.files:
            self.context.filestore.move(f, 'new', 'cur')

    def reset(self):
        self.files[:] = []
        self._needs_to_join = True

    def _join_transaction(self):
        # XXX copy&paste from gocept.amqprun.session. #9988
        if not self._needs_to_join:
            return
        dm = FileStoreDataManager(self)
        transaction.get().join(dm)
        self._needs_to_join = False


class FileStoreDataManager(object):

    zope.interface.implements(transaction.interfaces.IDataManager)

    def __init__(self, session):
        self.session = session
        self._tpc_begin = False

    def abort(self, transaction):
        self.tpc_abort(transaction)

    def tpc_begin(self, transaction):
        pass

    def commit(self, transaction):
        pass

    def tpc_vote(self, transaction):
        self.session.commit()
        self.session.reset()

    def tpc_finish(self, transaction):
        pass

    def tpc_abort(self, transaction):
        self.session.reset()

    def sortKey(self):
        # sort after gocept.amqprun.session
        return "~gocept.amqprun.readfiles:%f" % time.time()


class IReadFilesDirective(zope.interface.Interface):

    directory = zope.configuration.fields.Path(
        title=u"Path to the directory from which to read files")

    routing_key = zope.schema.TextLine(
        title=u"Routing key to send to")


def readfiles_directive(_context, directory, routing_key):
    reader = FileStoreReader(directory, routing_key)
    zope.component.zcml.subscriber(
        _context,
        for_=(gocept.amqprun.interfaces.IProcessStarted,),
        handler=lambda event: reader.start())
    zope.component.zcml.subscriber(
        _context,
        for_=(gocept.amqprun.interfaces.IProcessStopping,),
        handler=lambda event: reader.stop())
