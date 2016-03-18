from datetime import datetime
import email.utils
import gocept.amqprun.interfaces
import logging
import pika.spec
import string
import time
import types
import zope.interface


log = logging.getLogger(__name__)


class Message(object):
    """A message received from or sent to the queue."""

    zope.interface.implements(gocept.amqprun.interfaces.IMessage)

    exchange = 'amq.topic'

    def __init__(self, header, body, delivery_tag=None, routing_key=None,
                 channel=None):
        if not isinstance(header, pika.spec.BasicProperties):
            header = self.convert_header(header)
        self.header = header
        if not isinstance(body, (basestring, types.NoneType)):
            raise ValueError(
                'Message body must be basestring, not %s' % type(body))
        self.body = body
        self.delivery_tag = delivery_tag
        self.routing_key = (
            unicode(routing_key).encode('UTF-8') if routing_key else None)
        self._channel = channel  # received message from this channel

    def convert_header(self, header):
        header = header.copy()
        result = pika.spec.BasicProperties()
        result.timestamp = time.time()
        result.delivery_mode = 2  # persistent
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
            xfilename=(self.header.headers and
                       self.header.headers.get('X-Filename')),
            routing_key=self.routing_key,
            # since CPython doesn't use OS-level threads, there won't be actual
            # concurrency, so we can get away with using the current time to
            # uniquify the filename -- we have to take care about the
            # precision, though: '%s' loses digits, but '%f' doesn't.
            unique='%f' % time.time(),
        )
        return pattern.substitute(variables)

    def reference(self, message):
        """Make the current message referencing `message`."""
        if not gocept.amqprun.interfaces.IMessage.providedBy(message):
            return
        if not message.header.message_id:
            return
        if not self.header.correlation_id:
            self.header.correlation_id = message.header.message_id
        if self.header.headers is None:
            self.header.headers = {}
        if 'references' not in self.header.headers:
            if (message.header.headers and
                    message.header.headers.get('references')):
                parent_references = message.header.headers['references'] + '\n'
            else:
                parent_references = ''
            self.header.headers['references'] = (
                parent_references + message.header.message_id)

    def acknowledge(self):
        """Acknowledge handling of a received message to the queue."""
        if self._channel is None:
            raise RuntimeError('No channel set for acknowledge.')
        log.debug("Ack'ing message %s.", self.delivery_tag)
        self._channel.basic_ack(self.delivery_tag)
