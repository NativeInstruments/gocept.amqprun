# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.testing.assertion
import mock
import unittest
import zope.interface.verify


class TestHandler(unittest.TestCase):

    def get_handler(self, handler):
        from gocept.amqprun.handler import Handler
        return Handler('queue.name', 'routing.key', handler)

    def test_factory_should_create_handler(self):
        import gocept.amqprun.interfaces
        handler = self.get_handler(lambda x: None)
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandler, handler))
        self.assertEquals('queue.name', handler.queue_name)
        self.assertEquals('routing.key', handler.routing_key)

    def test_calling_handler_should_return_messages(self):
        handler = mock.Mock()
        handler.return_value = [mock.sentinel.msg1, mock.sentinel.msg2]
        message = mock.Mock()
        handler = self.get_handler(handler)
        result = handler(message)
        self.assertEquals([mock.sentinel.msg1, mock.sentinel.msg2], result)

    def test_invalid_handler_function_should_raise_typeerror(self):
        self.assertRaises(TypeError, self.get_handler, 'i-am-not-callable')

    def test_decorator_should_create_handler(self):
        import gocept.amqprun.handler
        import gocept.amqprun.interfaces

        @gocept.amqprun.handler.declare('queue.name', 'routing.key')
        def handler(message):
            return None  # pragma: no cover
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandler, handler))
        self.assertEquals('queue.name', handler.queue_name)
        self.assertEquals('routing.key', handler.routing_key)

    def test_handle_should_be_alias_for_declare(self):
        # for backwards compatibility reasons we want to keep the alternate
        # name
        import gocept.amqprun.handler
        self.assertEqual(
            gocept.amqprun.handler.declare,
            gocept.amqprun.handler.handle)

    def test_decorator_supports_arguments(self):
        import gocept.amqprun.handler
        import gocept.amqprun.interfaces

        @gocept.amqprun.handler.declare(
            'queue.name', 'routing.key', arguments={'x-ha-policy': 'all'})
        def handler(message):
            return None  # pragma: no cover
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandler, handler))
        self.assertEquals('queue.name', handler.queue_name)
        self.assertEquals('routing.key', handler.routing_key)
        self.assertEquals({'x-ha-policy': 'all'}, handler.arguments)

    def test_decorator_supports_principal(self):
        import gocept.amqprun.handler
        import gocept.amqprun.interfaces

        @gocept.amqprun.handler.declare(
            'queue.name', 'routing.key', principal='zope.user')
        def handler(message):
            return None  # pragma: no cover
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandler, handler))
        self.assertEquals('queue.name', handler.queue_name)
        self.assertEquals('routing.key', handler.routing_key)
        self.assertEquals('zope.user', handler.principal)


class ErrorHandlingHandlerTest(
        unittest.TestCase, gocept.testing.assertion.String):

    def setUp(self):
        import gocept.amqprun.message
        self.message = gocept.amqprun.message.Message(
            {}, 'foo', channel=mock.Mock())

    def get_declaration(self):
        import gocept.amqprun.handler

        class ExampleHandler(gocept.amqprun.handler.ErrorHandlingHandler):

            queue_name = 'test.foo'
            routing_key = 'routing.key'
            error_routing_key = 'rounting.key.error'

            run = mock.Mock()

        return ExampleHandler

    def get_handler(self):
        return self.get_declaration()(self.message)

    def test_should_conform_to_protocol(self):
        import gocept.amqprun.interfaces
        import gocept.amqprun.handler
        self.assertTrue(
            zope.interface.verify.verifyObject(
                gocept.amqprun.interfaces.IHandler,
                gocept.amqprun.handler.ErrorHandlingHandler()))
        self.assertTrue(
            zope.interface.verify.verifyObject(
                gocept.amqprun.interfaces.IResponse,
                gocept.amqprun.handler.ErrorHandlingHandler()))

    def test_call_should_call_run(self):
        handler = self.get_handler()
        handler.handle()
        handler.run.assert_called_with()

    def test_returns_error_response_on_exception(self):
        handler = self.get_handler()

        def run(self):
            self.send('foo', 'success')
            raise RuntimeError('asdf')
        handler.run = run.__get__(handler)
        handler.handle()
        self.assertEqual(1, len(handler.responses))
        error = handler.responses[0]
        self.assertEqual(handler.error_routing_key, error.routing_key)
        self.assertStartsWith('RuntimeError: asdf\nTraceback', error.body)

    def test_aborts_transaction_on_non_recoverable_error(self):
        handler = self.get_handler()

        def run(self):
            self.send('foo', 'success')
            raise RuntimeError('asdf')
        handler.run = run.__get__(handler)
        with mock.patch('transaction.abort') as abort:
            handler.handle()
        abort.assert_called_once_with()

    def test_acknowledges_message_on_non_recoverable_error(self):
        handler = self.get_handler()

        def run(self):
            self.send('foo', 'success')
            raise RuntimeError('asdf')
        handler.run = run.__get__(handler)
        with mock.patch.object(self.message, 'acknowledge') as acknowledge:
            handler.handle()
        acknowledge.assert_called_once_with()

    def test_error_message_references_message_on_non_recoverable_error(self):
        self.message.header.message_id = 'orig message id'
        self.message.header.application_headers = {'references': 'a\nb'}
        handler = self.get_handler()
        handler.run.side_effect = RuntimeError('asdf')
        handler.handle()
        error_msg = handler.responses[0]
        self.assertEqual('orig message id', error_msg.header.correlation_id)
        self.assertEqual(
            'a\nb\norig message id',
            error_msg.header.application_headers['references'])

    def test_raises_exceptions_defined_in_error_reraise(self):
        handler = self.get_handler()
        handler.run.side_effect = RuntimeError('asdf')
        handler.error_reraise = RuntimeError
        with self.assertRaises(RuntimeError):
            handler.handle()

    def test_wrapping_exceptions_should_not_fail_with_null_bytes(self):
        handler = self.get_handler()
        handler.run.side_effect = RuntimeError(
            'E: ONT\x00OE_SCH_ZERO_QTY\x00; Cannot perform scheduling'
            ' on a line with 0 quantity.')
        handler.handle()
        error = handler.responses[0]
        self.assertIn('E: ONTOE_SCH_ZERO_QTY; Cannot perform', error.body)
