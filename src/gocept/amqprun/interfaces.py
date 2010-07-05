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
