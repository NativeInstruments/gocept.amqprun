# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.handler
import zope.testing.cleanup


messages_received = []


def _clear_msgs():
    messages_received[:] = []
zope.testing.cleanup.addCleanUp(_clear_msgs)


def handle_message(message):
    messages_received.append(message)


def handle_message_and_error(message):
    messages_received.append(message)
    raise RuntimeError('Error')


handler = gocept.amqprun.handler.HandlerDeclaration(
    'test.queue', 'test.routing', handle_message)


handler_error = gocept.amqprun.handler.HandlerDeclaration(
    'test.queue.error', 'test.error', handle_message_and_error)
