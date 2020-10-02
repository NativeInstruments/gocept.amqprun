from .main import create_configured_server
import gocept.amqprun.interfaces
import gocept.filestore
import logging
import os
import os.path
import signal
import time
import transaction
import zope.component
import zope.component.zcml
import zope.configuration.fields
import zope.interface
import zope.schema


log = logging.getLogger(__name__)


class FileStoreReader:

    def __init__(self, path, routing_key):
        self.routing_key = routing_key
        self.filestore = gocept.filestore.FileStore(path)
        self.filestore.prepare()
        self.session = Session(self)

    def run(self):
        log.info('starting to watch %s', self.filestore.path)
        while True:
            try:
                self.scan()
                time.sleep(1)
            except Exception:
                log.error('Unhandled exception, terminating.', exc_info=True)
                os.kill(os.getpid(), signal.SIGUSR1)

    def scan(self):
        for filename in self.filestore.list('new'):
            transaction.begin()
            log.info('sending %r to %r', filename, self.routing_key)
            try:
                self.send(filename)
                self.session.mark_done(filename)
            except Exception:
                log.error('Sending message failed', exc_info=True)
                transaction.abort()
            else:
                transaction.commit()

    def send(self, filename):
        with open(filename) as f:
            body = f.read()
        # XXX make content-type configurable?
        headers = {
            'content_type': 'text/xml',
            'X-Filename': os.path.basename(filename).encode('utf-8'),
        }
        if isinstance(body, str):
            headers['content_encoding'] = 'utf-8'
        message = gocept.amqprun.message.Message(
            headers, body, routing_key=self.routing_key)
        sender = zope.component.getUtility(gocept.amqprun.interfaces.ISender)
        sender.send(message)


class Session:

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


@zope.interface.implementer(transaction.interfaces.IDataManager)
class FileStoreDataManager:

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


def main(config_file, path, routing_key):
    server = create_configured_server(config_file)
    server.connect()
    FileStoreReader(path, routing_key).run()
