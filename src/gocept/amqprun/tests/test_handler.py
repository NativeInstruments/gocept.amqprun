# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import mock
import unittest
import zope.interface.verify


class TestDeclaration(unittest.TestCase):

    def get_decl(self, handler):
        from gocept.amqprun.handler import HandlerDeclaration
        return HandlerDeclaration('queue.name', 'routing.key', handler)

    def test_factory_should_create_handler_declaration(self):
        import gocept.amqprun.interfaces
        decl = self.get_decl(lambda x: None)
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandlerDeclaration, decl))
        self.assertEquals('queue.name', decl.queue_name)
        self.assertEquals('routing.key', decl.routing_key)

    def test_factored_handler_should_have_message_context(self):
        import gocept.amqprun.interfaces
        handler = mock.Mock()
        handler.return_value = None
        message = mock.Mock()
        decl = self.get_decl(handler)
        factored_handler = decl(message)
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandler, factored_handler))
        self.assertFalse(handler.called)
        # Call the handler
        result = factored_handler()
        self.assertEquals([], result)
        self.assertTrue(handler.called)
        handler.assert_called_with(message)

    def test_factored_handler_should_return_messages(self):
        handler = mock.Mock()
        handler.return_value = [mock.sentinel.msg1, mock.sentinel.msg2]
        message = mock.Mock()
        decl = self.get_decl(handler)
        factored_handler = decl(message)
        result = factored_handler()
        self.assertEquals([mock.sentinel.msg1, mock.sentinel.msg2], result)

    def test_invalid_handler_function_should_raise_typeerror(self):
        self.assertRaises(TypeError, self.get_decl, 'i-am-not-callable')

    def test_decorator_should_create_handler(self):
        import gocept.amqprun.handler
        import gocept.amqprun.interfaces

        @gocept.amqprun.handler.declare('queue.name', 'routing.key')
        def decl(message):
            return None
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandlerDeclaration, decl))
        self.assertEquals('queue.name', decl.queue_name)
        self.assertEquals('routing.key', decl.routing_key)

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

        @gocept.amqprun.handler.declare('queue.name', 'routing.key', arguments={'x-ha-policy': 'all'})
        def decl(message):
            return None
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandlerDeclaration, decl))
        self.assertEquals('queue.name', decl.queue_name)
        self.assertEquals('routing.key', decl.routing_key)
        self.assertEquals({'x-ha-policy': 'all'}, decl.arguments)
