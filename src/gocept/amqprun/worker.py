from gocept.amqprun.interfaces import IResponse
from gocept.amqprun.interfaces import RetryException
import logging
import transaction

try:
    from zope.security.management import endInteraction as end_interaction
    from zope.security.testing import create_interaction
except ImportError:  # pragma: no cover
    def create_interaction(principal):
        log.warn(
            'create_interaction(%s) called but zope.security is not available',
            principal)

    def end_interaction():
        pass


log = logging.getLogger(__name__)


class PrefixingLogger:
    """Convenience for spelling log.foo(prefix + message)"""

    def __init__(self, log, prefix):
        self.log = log
        self.prefix = prefix

    def __getattr__(self, name):
        def write(message, *args, **kw):
            log_method = getattr(self.log, name)
            return log_method(self.prefix + message, *args, **kw)
        return write


class Worker:

    def __init__(self, session, handler):
        self.session = session
        self.handler = handler

    def __call__(self):
        session = self.session
        handler = self.handler
        self.log = PrefixingLogger(log, 'Worker ')
        try:
            message = session.received_message
            self.log.info('Processing message %s %s (%s)',
                          message.delivery_tag,
                          message.header.message_id,
                          message.routing_key)
            self.log.debug(str(message.body))
            transaction.begin()
            if handler.principal is not None:
                create_interaction(handler.principal)
            session.join_transaction()
            response = None

            # In general an exception leads to an un-acked message, which will
            # be re-queued by RabbitMQ at channel switch. In case of an
            # ErrorHandlingHandler, only RetryExceptions is allowed to be
            # raised. Other exceptions are expected to be handled and should
            # lead to acknowledging messages with content about the error. This
            # can happen once during `handle()` and during
            # `transaction.commit()`.
            try:
                response = handler(message)
            except RetryException as e:
                transaction.abort()
                self.log.info(
                    'Retrying message %s after retryable exception %r.',
                    message.delivery_tag, e)
                # We can leave already, as we have an aborted transaction.
                return
            except Exception:
                # This Exception is only possible, if we have no
                # ErrorHandlingHandler. So we are not able to process it
                # further.
                transaction.abort()
                self.log.error(
                    'Error while processing message %s.'
                    ' It will be re-queued at RabbitMQ and retried.',
                    message.delivery_tag, exc_info=True)
                # We can leave already, as we have an aborted transaction.
                return

            # We are either in the happy path or handled an non retryable error
            # within ErrorHandlingHandler.
            if IResponse.providedBy(response):
                response_messages = response.responses
            else:
                response_messages = response
            self._send_response(session, message, response_messages)
            try:
                transaction.commit()
            except Exception:
                transaction.abort()
                if IResponse.providedBy(response):
                    # We have an ErrorHandlingHandler here. In case the
                    # exception is retryable, exception() will tell us.
                    try:
                        error_messages = response.exception()
                    except RetryException as e:
                        self.log.info(
                            'Retrying message %s after retryable'
                            ' exception %r.', message.delivery_tag, e)
                    else:
                        # No retry, we send a message and acknowledge.
                        session.received_message = message
                        self._send_response(session, message, error_messages)
                        try:
                            transaction.commit()
                        except Exception:
                            transaction.abort()
                            self.log.error(
                                'Error during exception handling',
                                exc_info=True)
                else:
                    self.log.error(
                        'Error while processing message %s.'
                        ' It will be re-queued at RabbitMQ and retried.',
                        message.delivery_tag, exc_info=True)

        except Exception:
            self.log.error(
                'Unhandled exception, prevent process from crashing.'
                ' This is probably caused by a programming error.',
                exc_info=True)
        finally:
            end_interaction()

    def _send_response(self, session, message, response):
        for msg in response:
            self.log.info(
                'Sending message to %s in response to message %s',
                msg.routing_key, message.delivery_tag)
            session.send(msg)
