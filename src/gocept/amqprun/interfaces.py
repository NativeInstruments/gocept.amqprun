import re
import zope.interface
import zope.interface.common.mapping
import zope.schema
import zope.schema.interfaces


class IHandler(zope.interface.Interface):

    queue_name = zope.interface.Attribute(
        'Name of the queue to declare/bind')

    routing_key = zope.interface.Attribute(
        'Routing key of messages this handler is interested in. '
        'May be a list of several routing keys.')

    arguments = zope.interface.Attribute(
        'Arguments that are forwarded to the queue declare process')

    principal = zope.interface.Attribute(
        'If set, message is handled inside a zope.security interaction with '
        'the given principal_id.')

    settings = zope.interface.Attribute(
        'Dictionary of configuration settings (ISettings utility)')

    def __call__(message):
        """Handle message.

        Returns either a sequence of IMessage objects to be passed to AMQP,
        or an IResponse object.

        """


class IResponse(zope.interface.Interface):

    responses = zope.interface.Attribute(
        'Sequence of IMessage objects (responses to original message)')

    def exception():
        """Handle an exception that occurred during the transaction commit.

        Returns a sequence of IMessage objects (error messages) to be passed to
        AMQP. Note: The original message will also be acked -- if that's not
        desired, implementers should raise an exception instead.

        """


class IMessage(zope.interface.Interface):
    """A message received from or sent to the queue."""

    header = zope.interface.Attribute('Message headers/properties')
    body = zope.interface.Attribute('Message body')
    delivery_tag = zope.interface.Attribute('Message delivery tag (if any)')
    routing_key = zope.interface.Attribute('Routing key (if any)')
    exchange = zope.interface.Attribute('Exchange (default: amq.topic)')

    def generate_filename(pattern):
        """``pattern`` performs as ``string.Template`` substitution.
        The following variables are available:

          :date: The date the message arrived, formatted ``%Y-%m-%d``
          :msgid: The value of the message-id header
          :routing_key: The routing key of the message
          :unique: A token that guarantees the filename will be unique
          in its directory (using the current timestamp)
        """

    def reference(message):
        """Make the current message referencing `message`.

        Sets `correlation_id` and a `references` list.
        """

    def acknowledge():
        """Acknowledge handling of a received message to the queue.

        Raises `RuntimeError` if message was not recived.
        """


class ISession(zope.interface.Interface):
    """AMQP session."""

    messages = zope.interface.Attribute('List of messages to send.')

    def send(message):
        """Queues up message for sending upon transaction commit."""

    def reset():
        """Discards all queued messages."""


class ISettings(zope.interface.common.mapping.IReadMapping):
    """Runner settings.

    Keys must be dotted names.

    """

    def update(a_dict):
        """Update the settings with ``a_dict``."""


class ILoop(zope.interface.Interface):

    running = zope.interface.Attribute('bool: is the loop running?')

    def wait_until_running(timeout=None):
        """Block until the loop is running.

        Optionally apply a timeout given in seconds.

        """

    def start():
        pass

    def stop():
        pass


class ISender(zope.interface.Interface):
    """Sender for AMQP messages."""

    def send(message):
        """Send a message using AMQP."""


class IChannelManager(zope.interface.Interface):
    """Manage channel livecycle."""

    def acquire():
        """Use the channel."""

    def release():
        """Indicate channel is no longer being used."""

    def close_if_possible():
        """Close the channel if it is no longer used by anyone."""


class CounterBelowZero(Exception):
    """The counter has fallen below zero."""


class IConfigFinished(zope.interface.Interface):
    """Event indicating that the configuration of gocept.amqprun was loaded."""


class ConfigFinished(object):

    zope.interface.implements(IConfigFinished)


class IProcessStarted(zope.interface.Interface):
    """Event to indicate the server has been started."""


class ProcessStarted(object):

    zope.interface.implements(IProcessStarted)


class IProcessStopping(zope.interface.Interface):
    """Event to indicate the process is being stopped."""


class ProcessStopping(object):

    zope.interface.implements(IProcessStopping)


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


@zope.interface.implementer(zope.schema.interfaces.IDict)
class RepresentableDict(zope.schema.Dict):
    """A field representing a Dict, but representable in ZCML."""

    key_value_regex = re.compile(r'^(?P<key>[^:=\s[][^:=]*)'
                                 r'(?P<sep>[:=]\s*)'
                                 r'(?P<value>.*)$')

    def fromUnicode(self, raw_value):
        retval = {}
        for line in raw_value.strip().splitlines():
            m = self.key_value_regex.match(line.strip())
            if m is None:
                continue

            key = m.group('key').rstrip()
            value = m.group('value')
            retval[key] = value

        self.validate(retval)
        return retval
