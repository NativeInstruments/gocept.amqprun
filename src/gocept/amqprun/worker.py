from gocept.amqprun.interfaces import IResponse
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


class PrefixingLogger(object):
    """Convenience for spelling log.foo(prefix + message)"""

    def __init__(self, log, prefix):
        self.log = log
        self.prefix = prefix

    def __getattr__(self, name):
        def write(message, *args, **kw):
            log_method = getattr(self.log, name)
            return log_method(self.prefix + message, *args, **kw)
        return write


class Worker(object):

    def __init__(self, session, handler):
        self.session = session
        self.handler = handler

    def __call__(self):
        session = self.session
        handler = self.handler
        self.log = PrefixingLogger(log, 'Worker ')
        self.log.info('starting')
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
            try:
                response = handler(message)
                if IResponse.providedBy(response):
                    response_messages = response.responses
                else:
                    response_messages = response
                self._send_response(session, message, response_messages)
                transaction.commit()
            except Exception:
                self.log.error(
                    'Error while processing message %s',
                    message.delivery_tag, exc_info=True)
                transaction.abort()
                if IResponse.providedBy(response):
                    try:
                        session.received_message = message
                        error_messages = response.exception()
                        self._send_response(session, message, error_messages)
                        transaction.commit()
                    except Exception:
                        self.log.error(
                            'Error during exception handling', exc_info=True)
                        transaction.abort()
        except Exception:
            self.log.error(
                'Unhandled exception, prevent thread from crashing',
                exc_info=True)
        finally:
            end_interaction()

    def _send_response(self, session, message, response):
        for msg in response:
            self.log.info(
                'Sending message to %s in response to message %s',
                msg.routing_key, message.delivery_tag)
            session.send(msg)
