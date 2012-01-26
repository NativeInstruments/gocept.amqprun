# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

from datetime import datetime
import email.utils
import gocept.amqprun.interfaces
import pika.spec
import string
import time
import zope.interface


class Message(object):

    zope.interface.implements(gocept.amqprun.interfaces.IMessage)

    exchange = 'amq.topic'

    def __init__(self, header, body, delivery_tag=None, routing_key=None):
        if not isinstance(header, pika.spec.BasicProperties):
            header = self.convert_header(header)
        self.header = header
        self.body = body
        self.delivery_tag = delivery_tag
        self.routing_key = (
            unicode(routing_key).encode('UTF-8') if routing_key else None)

    def convert_header(self, header):
        header = header.copy()
        result = pika.spec.BasicProperties()
        result.timestamp = time.time()
        result.delivery_mode = 2 # persistent
        result.message_id = email.utils.make_msgid('gocept.amqprun')
        for key in dir(result):
            value = header.pop(key, self)
            if isinstance(value, unicode):
                value = value.encode('UTF-8')
            if value is not self:
                setattr(result, key, value)
        result.headers = header
        return result

    def generate_filename(self, pattern):
        pattern = string.Template(pattern)
        if self.header.timestamp is not None:
            timestamp = datetime.fromtimestamp(self.header.timestamp)
        else:
            timestamp = datetime.now()
        variables = dict(
            date=timestamp.strftime('%Y-%m-%d'),
            msgid=self.header.message_id,
            routing_key=self.routing_key,
            # since CPython doesn't use OS-level threads, there won't be actual
            # concurrency, so we can get away with using the current time to
            # uniquify the filename -- we have to take care about the
            # precision, though: '%s' loses digits, but '%f' doesn't.
            unique='%f' % time.time(),
        )
        return pattern.substitute(variables)
