# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.interfaces
import logging
import sys
import traceback
import zope.interface
import transaction


log = logging.getLogger(__name__)


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


class ErrorHandlingHandler(object):

    queue_name = NotImplemented
    routing_key = NotImplemented

    arguments = None
    principal = None

    # Error responses are sent to this routing key.
    error_routing_key = NotImplemented
    # Exception(s) which are *not* treated in a special way.
    error_reraise = None

    zope.interface.implements(
        gocept.amqprun.interfaces.IHandler,
        gocept.amqprun.interfaces.IResponse)

    def __init__(self, message=None):
        self.message = message
        self.responses = []

    def __call__(self, message):
        handler = self.__class__(message)
        handler.handle()
        return handler

    def handle(self):
        try:
            self._decode_message()
            self.run()
        except self.error_reraise:
            raise
        except:
            # Solution for https://bitbucket.org/gocept/gocept.amqprun/issue/4
            # Abort the transaction which caused the error to prevent it from
            # writing any data e. g. to a relational database when committing
            # the error handling later on.
            transaction.abort()
            transaction.begin()
            log.warning(
                "Processing message %s caused an error. Sending '%s'",
                self.message.delivery_tag, self.error_routing_key,
                exc_info=True)
            self.responses[:] = []
            self.responses.append(self._create_error_message())
        return self.responses

    def _decode_message(self):
        pass

    def run(self):
        raise NotImplementedError()

    def exception(self):
        return [self._create_error_message()]

    def send(self, content, routing_key):
        self.responses.append(self._create_message(content, routing_key))

    def _create_message(self, content, routing_key):
        return gocept.amqprun.message.Message(
            {}, content, routing_key=routing_key)

    def _create_error_message(self):
        content = '%s\n%s' % self._format_traceback()
        return self._create_message(content, self.error_routing_key)

    def _format_traceback(self):
        class_, exc, tb = sys.exc_info()
        message = str(exc).replace('\x00', '')
        message = '%s: %s' % (class_.__name__, message)
        detail = ''.join(
            traceback.format_exception(class_, exc, tb)).replace(
                '\x00', '')
        return message, detail
