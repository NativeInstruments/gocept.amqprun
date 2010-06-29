# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import Queue
import threading


class Worker(threading.Thread):

    timeout = 1

    def __init__(self, queue, handler):
        self.queue = queue
        self.handler = handler
        super(Worker, self).__init__()

    def run(self):
        self.running = True
        while self.running:
            try:
                message = self.queue.get(timeout=self.timeout)
                self.handler(message)
            except Queue.Empty:
                pass

    def stop(self):
        self.running = False
        self.join()
