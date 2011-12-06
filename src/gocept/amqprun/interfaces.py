# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import zope.interface
import zope.interface.common.mapping


class IHandlerDeclaration(zope.interface.Interface):

    queue_name = zope.interface.Attribute(
        'Name of the queue to declare/bind')

    routing_key = zope.interface.Attribute(
        'Routing key of messages this handler is interested in. '
        'May be a list of several routing keys.')

    arguments = zope.interface.Attribute(
        'Arguments that are forwarded to the queue declare process')

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


class ISession(zope.interface.Interface):
    """AMQP session."""

    messages = zope.interface.Attribute('List of messages to send.')

    def send(message):
        """Queues up message for sending upon transaction commit."""

    def clear():
        """Discards all queued messages."""


class ISettings(zope.interface.common.mapping.IReadMapping):
    """Runner settings.

    Keys must be dotted names.

    """

    def update(a_dict):
        """Update the settings with ``a_dict``."""


class ILoop(zope.interface.Interface):

    running = zope.interface.Attribute('bool: is the loop running?')

    def start():
        pass

    def stop():
        pass


class IChannelManager(zope.interface.Interface):
    """Manage channel livecycle."""

    def acquire():
        """Use the channel."""

    def release():
        """Indicate channel is no longer being used."""

    def get_count():
        """Return how often the channel is currenlty being used."""

    def close_if_possible():
        """Close the channle if it is no longer used by anyone."""


class IProcessStarting(zope.interface.Interface):
    """Event to indicate the process is being started."""


class ProcessStarting(object):
    """Event to indicate the process is being started."""

    zope.interface.implements(IProcessStarting)


class IMessageStored(zope.interface.Interface):
    """Event the FileWriter sends after it has written a message to the
    filesystem."""

    path = zope.interface.Attribute(
        'The absolute path of the file the message was written to')

    message = zope.interface.Attribute(
        'The gocept.amqprun.message.Message object')


class MessageStored(object):

    zope.interface.implements(IMessageStored)

    def __init__(self, message, path):
        self.path = path
        self.message = message
