from datetime import datetime
import amqp
import email.utils
import gocept.amqprun.interfaces
import logging
import string
import time
import types
import zope.interface


log = logging.getLogger(__name__)


class Properties(object):
    """Header properties for our Message class.
    Partly taken from pika.spec.BasicProperties."""

    def __init__(
            self,
            content_type=None,
            content_encoding=None,
            application_headers=None,
            delivery_mode=None,
            priority=None,
            correlation_id=None,
            reply_to=None,
            expiration=None,
            message_id=None,
            timestamp=None,
            type=None,
            user_id=None,
            app_id=None,
            cluster_id=None):
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.application_headers = application_headers or {}
        self.delivery_mode = delivery_mode
        self.priority = priority
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.expiration = expiration
        self.message_id = message_id
        self.timestamp = timestamp
        self.type = type
        self.user_id = user_id
        self.app_id = app_id
        self.cluster_id = cluster_id

    def as_dict(self):
        return self.__dict__.copy()


class Message(object):
    """A message received from or sent to the queue."""

    zope.interface.implements(gocept.amqprun.interfaces.IMessage)

    exchange = 'amq.topic'

    def __init__(self, header, body, delivery_tag=None, routing_key=None,
                 channel=None):
        if not isinstance(header, Properties):
            header = self.convert_header(header)
        self.header = header
        if not isinstance(body, (basestring, types.NoneType)):
            raise ValueError(
                'Message body must be basestring, not %s' % type(body))

        # We don't want empty unicode strings. also u'' == ''
        if body == u'':
            body = ''

        if isinstance(body, unicode) and not header.content_encoding:
            raise ValueError('If body is unicode provide a content_encoding.')

        self.body = body
        self.delivery_tag = delivery_tag
        self.routing_key = (
            unicode(routing_key).encode('UTF-8') if routing_key else None)
        self._channel = channel  # received message from this channel

    def convert_header(self, header):
        header = header.copy()
        result = Properties(
            timestamp=int(time.time()),
            delivery_mode=2,
            message_id=email.utils.make_msgid('gocept.amqprun'))

        for key in result.as_dict():
            value = header.pop(key, self)
            if isinstance(value, unicode):
                value = value.encode('UTF-8')
            if value is not self:
                setattr(result, key, value)
        result.application_headers.update(header)
        return result

    def as_amqp_message(self):
        """Return an amqp.Message from our data."""
        return amqp.Message(self.body, **self.header.as_dict())

    def generate_filename(self, pattern):
        pattern = string.Template(pattern)
        if self.header.timestamp is not None:
            timestamp = datetime.fromtimestamp(self.header.timestamp)
        else:
            timestamp = datetime.now()
        variables = dict(
            date=timestamp.strftime('%Y-%m-%d'),
            msgid=self.header.message_id,
            xfilename=(self.header.application_headers and
                       self.header.application_headers.get('X-Filename')),
            routing_key=self.routing_key,
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
        if self.header.application_headers is None:
            self.header.application_headers = {}
        if 'references' not in self.header.application_headers:
            if (message.header.application_headers and
                    message.header.application_headers.get('references')):
                parent_references = (
                    message.header.application_headers['references'] + '\n')
            else:
                parent_references = ''
            self.header.application_headers['references'] = (
                parent_references + message.header.message_id)

    def acknowledge(self):
        """Acknowledge handling of a received message to the queue."""
        if self._channel is None:
            raise RuntimeError('No channel set for acknowledge.')
        log.debug("Ack'ing message %s.", self.delivery_tag)
        self._channel.basic_ack(self.delivery_tag)
