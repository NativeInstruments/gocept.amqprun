# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import zope.interface


class IHandlerDeclaration(zope.interface.Interface):

    queue_name = zope.interface.Attribute(
        'Name of the queue to declare/bind')

    routing_key = zope.interface.Attribute(
        'Routing key of messages this handler is interested in')

    def __call__(message):
        """Return IHandler for given message."""


class IHandler(zope.interface.Interface):

    message = zope.interface.Attribute(
        "IMessage instance the handler handles.")

    def __call__():
        """Handle message.

        Returns a sequence of IMessage objects to be passed to AMQP.

        """


class IMessage(zope.interface.Interface):
    """A message received from or sent to the queue."""

    header = zope.interface.Attribute('Message headers/properties')
    body = zope.interface.Attribute('Message body')
    delivery_tag = zope.interface.Attribute('Message delivery tag (if any)')
    routing_key = zope.interface.Attribute('Routing key (if any)')
    exchange = zope.interface.Attribute('Exchange (default: amqp.topic)')

    @zope.interface.invariant
    def delivery_tag_xor_routing_key(obj):
        if (obj.delivery_tag is None) == (obj.routing_key is None):
            raise zope.interface.Invalid(
                'Exactly one of {delivery_tag, routing_key}'
                ' is required, got {%r, %r}'
                % (obj.delivery_tag, obj.routing_key))


class ISession(zope.interface.Interface):
    """AMQP session."""

    messages = zope.interface.Attribute('List of messages to send.')

    def send(message):
        """Queues up message for sending upon transaction commit."""

    def clear():
        """Discards all queued messages."""
