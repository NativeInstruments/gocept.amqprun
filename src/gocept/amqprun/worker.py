# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import logging
import threading
import transaction


log = logging.getLogger(__name__)


class Worker(threading.Thread):

    timeout = 1

    def __init__(self, queue, handler, datamanager_factory):
        self.queue = queue
        self.handler = handler
        self.datamanager_factory = datamanager_factory
        super(Worker, self).__init__()

    def run(self):
        log.info('started worker')
        self.running = True
        while self.running:
            try:
                message = self.queue.get(timeout=self.timeout)
            except Queue.Empty:
                pass
            else:
                transaction.begin()
                datamanager = self.datamanager_factory(message)
                transaction.get().join(datamanager)
                try:
                    self.handler(message)
                    # for msg in response:
                    #     datamanager.send(msg)
                except:
                    transaction.abort()
                else:
                    transaction.commit()

    def stop(self):
        self.running = False
        self.join()


def logging_handler(message):
    log.debug(message.body)
