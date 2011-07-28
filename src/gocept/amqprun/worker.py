# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import logging
import threading
import transaction


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

    def __init__(self, queue, session_factory):
        self.queue = queue
        self.session_factory = session_factory
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
                handler = self.queue.get(timeout=self.timeout)
            except Queue.Empty:
                pass
            else:
                try:
                    self.log.info('Processing message %s %s (%s)',
                                  handler.message.delivery_tag,
                                  handler.message.header.message_id,
                                  handler.message.routing_key)
                    self.log.debug(str(handler.message.body))
                    transaction.begin()
                    session = self.session_factory(handler)
                    try:
                        response = handler()
                        for msg in response:
                            self.log.info(
                                'Sending message (%s)', msg.routing_key)
                            session.send(msg)
                        transaction.commit()
                    except:
                        self.log.error(
                            'Error while processing message %s',
                            handler.message.delivery_tag,
                            exc_info=True)
                        transaction.abort()
                except:
                    self.log.error(
                        'Unhandled exception, prevent thread from crashing',
                        exc_info=True)

    def stop(self):
        self.log.info('stopping')
        self.running = False
        self.join()
