# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import zope.interface


class HandlerDeclaration(object):

    zope.interface.implements(gocept.amqprun.interfaces.IHandlerDeclaration)

    def __init__(self, queue_name, routing_key, handler_function, arguments=None):
        self.queue_name = queue_name
        self.routing_key = routing_key
        if not callable(handler_function):
            raise TypeError('handler_function not callable')
        self.handler_function = handler_function
        self.arguments = arguments

    def __call__(self, message):
        return FactoredHandler(self.handler_function, message)


def declare(queue_name, routing_key, arguments=None):
    return lambda handler_function: HandlerDeclaration(
        queue_name, routing_key, handler_function, arguments)

handle = declare


class FactoredHandler(object):

    zope.interface.implements(gocept.amqprun.interfaces.IHandler)

    def __init__(self, function, message):
        self.function = function
        self.message = message

    def __call__(self):
        result = self.function(self.message)
        return result if result else []
