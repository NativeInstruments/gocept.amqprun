import gocept.amqprun.interfaces
import logging
import sys
import traceback
import transaction
import zope.component
import zope.interface


log = logging.getLogger(__name__)


@zope.interface.implementer(gocept.amqprun.interfaces.IHandler)
class Handler:

    def __init__(self, queue_name, routing_key, handler_function,
                 arguments=None, principal=None):
        self.queue_name = queue_name
        self.routing_key = routing_key
        if not callable(handler_function):
            raise TypeError('handler_function not callable')
        self.handler_function = handler_function
        self.arguments = arguments
        self.principal = principal

    @property
    def settings(self):
        return zope.component.queryUtility(gocept.amqprun.interfaces.ISettings)

    def __call__(self, message):
        return self.handler_function(message) or []

    def __repr__(self):
        return '<gocept.amqprun.handler.Handler({!r}, {!r}, {!r})>'.format(
            self.handler_function, self.queue_name, self.routing_key)


def declare(queue_name, routing_key, arguments=None, principal=None):
    return lambda handler_function: Handler(
        queue_name, routing_key, handler_function, arguments, principal)


# BBB
handle = declare


@zope.interface.implementer(
    gocept.amqprun.interfaces.IHandler,
    gocept.amqprun.interfaces.IResponse)
class ErrorHandlingHandler:

    queue_name = NotImplemented
    routing_key = NotImplemented

    arguments = None
    principal = None

    # Error responses are sent to this routing key.
    error_routing_key = NotImplemented
    # Exception(s) which are *not* treated in a special way.
    error_reraise = None

    def __init__(self, message=None):
        self.message = message
        self.responses = []

    @property
    def settings(self):
        return zope.component.queryUtility(gocept.amqprun.interfaces.ISettings)

    def __call__(self, message):
        handler = self.__class__(message)
        handler.handle()
        return handler

    def handle(self):
        # The only possible Exception raised from this method is
        # RetryException. Other Exceptions will be handled and reported by an
        # deliverable error message.
        try:
            self._decode_message()
            self.run()
        except Exception as e:
            self._handle_retry(e)
            # Solution for https://bitbucket.org/gocept/gocept.amqprun/issue/4:
            # Abort the transaction which caused the error to prevent it from
            # writing any data e. g. to a relational database when committing
            # the error handling later on.
            transaction.abort()
            # Solution for https://bitbucket.org/gocept/gocept.amqprun/issue/5:
            # `transaction.abort()` above removed the message from the session,
            # too, so we have to acknowledge it manually as otherwise we have
            # to process it over and over again.
            self.message.acknowledge()
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
        try:
            exc = sys.exc_info()[1]
            self._handle_retry(exc)
        finally:
            del exc
        return [self._create_error_message()]

    def _handle_retry(self, exc):
        """(Re-)raise only configured exceptions."""
        if self.error_reraise and isinstance(exc, self.error_reraise):
            raise gocept.amqprun.interfaces.RetryException(
                exc).with_traceback(exc.__traceback__)

    def send(self, content, routing_key, content_encoding=None):
        self.responses.append(
            self._create_message(
                content, routing_key, content_encoding=content_encoding))

    def _create_message(self, content, routing_key, content_encoding=None):
        if content_encoding:
            headers = {'content_encoding': content_encoding}
        else:
            headers = {}
        return gocept.amqprun.message.Message(
            headers, content, routing_key=routing_key)

    def _create_error_message(self):
        content = '%s\n%s' % self._format_traceback()
        message = self._create_message(
            content, self.error_routing_key, content_encoding='utf-8')
        message.reference(self.message)
        return message

    def _format_traceback(self):
        class_, exc, tb = sys.exc_info()
        message = str(exc).replace('\x00', '')
        message = f'{class_.__name__}: {message}'
        detail = ''.join(
            traceback.format_exception(class_, exc, tb)).replace(
                '\x00', '')
        return message, detail
