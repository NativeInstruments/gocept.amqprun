# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import logging
import threading
import transaction


log = logging.getLogger(__name__)


class Worker(threading.Thread):

    timeout = 1

    def __init__(self, queue, datamanager_factory):
        self.queue = queue
        self.datamanager_factory = datamanager_factory
        super(Worker, self).__init__()

    def run(self):
        log.info('started worker')
        self.running = True
        while self.running:
            try:
                handler = self.queue.get(timeout=self.timeout)
            except Queue.Empty:
                pass
            else:
                transaction.begin()
                datamanager = self.datamanager_factory(handler)
                transaction.get().join(datamanager)
                try:
                    handler()
                    # for msg in response:
                    #     datamanager.send(msg)
                except:
                    log.error("Error while processing message",
                              exc_info=True)
                    transaction.abort()
                else:
                    transaction.commit()

    def stop(self):
        self.running = False
        self.join()
