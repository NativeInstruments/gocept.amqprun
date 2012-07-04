# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import zope.interface


class Handler(object):

    zope.interface.implements(gocept.amqprun.interfaces.IHandler)

    def __init__(self, queue_name, routing_key, handler_function,
                 arguments=None, principal=None):
        self.queue_name = queue_name
        self.routing_key = routing_key
        if not callable(handler_function):
            raise TypeError('handler_function not callable')
        self.handler_function = handler_function
        self.arguments = arguments
        self.principal = principal

    def __call__(self, message):
        return self.handler_function(message) or []


def declare(queue_name, routing_key, arguments=None, principal=None):
    return lambda handler_function: Handler(
        queue_name, routing_key, handler_function, arguments, principal)

# BBB
handle = declare
