# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import logging
import threading
import transaction

try:
    from zope.security.management import endInteraction as end_interaction
    from zope.security.testing import create_interaction
except ImportError:
    def create_interaction(principal):
        log.warn(
            'create_interaction(%s) called but zope.security is not available',
            principal)

    def end_interaction():
        pass


log = logging.getLogger(__name__)


class PrefixingLogger(object):
    """Convenience for spelling log.foo(prefix + message)"""

    def __init__(self, log, prefix):
        self.log = log
        self.prefix = prefix

    def __getattr__(self, name):
        def write(message, *args, **kw):
            log_method = getattr(self.log, name)
            return log_method(self.prefix + message, *args, **kw)
        return write


class Worker(threading.Thread):

    timeout = 5

    def __init__(self, queue):
        self.queue = queue
        self.running = False
        super(Worker, self).__init__()
        self.daemon = True
        # just in case we want to log something while not running
        self.log = log

    def run(self):
        self.log = PrefixingLogger(log, 'Worker[%s] ' % self.ident)
        self.log.info('starting')
        self.running = True
        while self.running:
            try:
                session, handler = self.queue.get(timeout=self.timeout)
            except Queue.Empty:
                pass
            else:
                try:
                    message = session.received_message
                    self.log.info('Processing message %s %s (%s)',
                                  message.delivery_tag,
                                  message.header.message_id,
                                  message.routing_key)
                    self.log.debug(str(message.body))
                    transaction.begin()
                    if handler.principal is not None:
                        create_interaction(handler.principal)
                    session.join_transaction()
                    try:
                        response = handler(message)
                        for msg in response:
                            self.log.info(
                                'Sending message to %s in response '
                                'to message %s', msg.routing_key,
                                message.delivery_tag)
                            session.send(msg)
                        transaction.commit()
                    except:
                        self.log.error(
                            'Error while processing message %s',
                            message.delivery_tag,
                            exc_info=True)
                        transaction.abort()
                except:
                    self.log.error(
                        'Unhandled exception, prevent thread from crashing',
                        exc_info=True)
                finally:
                    end_interaction()

    def stop(self):
        self.log.info('stopping')
        self.running = False
        self.join()
