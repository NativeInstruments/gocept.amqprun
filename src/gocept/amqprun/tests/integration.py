# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.handler
import gocept.amqprun.interfaces
import zope.interface


messages_received = None


def handle_message(message):
    messages_received.append(message)
    return [gocept.amqprun.message.Message(
            {}, '', routing_key='test.response')]


handler = gocept.amqprun.handler.Handler(
    'test.queue', 'test.routing', handle_message)


def handle_message_and_error(message):
    messages_received.append(message)
    raise RuntimeError('Error')


handler_error = gocept.amqprun.handler.Handler(
    'test.queue.error', 'test.error', handle_message_and_error)


class Response(object):

    zope.interface.implements(gocept.amqprun.interfaces.IResponse)

    responses = []
    _exception = False

    def exception(self):
        self._exception = True
        return [gocept.amqprun.message.Message(
            {}, '', routing_key='test.iresponse-error')]


def handle_message_iresponse(message):
    response = Response()
    messages_received.append(response)
    return response


handler_iresponse = gocept.amqprun.handler.Handler(
    'test.queue.iresponse', 'test.iresponse', handle_message_iresponse)
