# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import pika.channel
import zope.component
import zope.interface


class Manager(object):

    zope.interface.implements(gocept.amqprun.interfaces.IChannelManager)
    zope.component.adapts(pika.channel.Channel)

    def __init__(self, channel):
        self.channel = channel
        try:
            self.channel._gocept_amqprun_refcount
        except AttributeError:
            self.channel._gocept_amqprun_refcount = 0

    def acquire(self):
        self.channel._gocept_amqprun_refcount += 1

    def release(self):
        self.channel._gocept_amqprun_refcount -= 1

    def get_count(self):
        return self.channel._gocept_amqprun_refcount

    def close_if_possible(self):
        if self.channel._gocept_amqprun_refcount:
            return False
        self.channel.close()
        return True
