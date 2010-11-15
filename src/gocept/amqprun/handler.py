# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import zope.interface


class HandlerDeclaration(object):

    zope.interface.implements(gocept.amqprun.interfaces.IHandlerDeclaration)

    def __init__(self, queue_name, routing_key, handler_function):
        self.queue_name = queue_name
        self.routing_key = routing_key
        if not callable(handler_function):
            raise TypeError('handler_function not callable')
        self.handler_function = handler_function

    def __call__(self, message):
        return FactoredHandler(self.handler_function, message)


def declare(queue_name, routing_key):
    return lambda handler_function: HandlerDeclaration(
        queue_name, routing_key, handler_function)

handle = declare


class FactoredHandler(object):

    zope.interface.implements(gocept.amqprun.interfaces.IHandler)

    def __init__(self, function, message):
        self.function = function
        self.message = message

    def __call__(self):
        result = self.function(self.message)
        return result if result else []
