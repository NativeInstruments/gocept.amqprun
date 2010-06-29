# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import logging
import threading


log = logging.getLogger(__name__)


class Worker(threading.Thread):

    timeout = 1

    def __init__(self, queue, handler):
        self.queue = queue
        self.handler = handler
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
                self.handler(message)

    def stop(self):
        self.running = False
        self.join()


def logging_handler(message):
    log.debug(message)
