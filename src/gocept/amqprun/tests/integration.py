# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.handler


messages_received = None


def handle_message(message):
    messages_received.append(message)
    return [gocept.amqprun.message.Message(
            {}, '', routing_key='test.response')]


def handle_message_and_error(message):
    messages_received.append(message)
    raise RuntimeError('Error')


handler = gocept.amqprun.handler.Handler(
    'test.queue', 'test.routing', handle_message)


handler_error = gocept.amqprun.handler.Handler(
    'test.queue.error', 'test.error', handle_message_and_error)
