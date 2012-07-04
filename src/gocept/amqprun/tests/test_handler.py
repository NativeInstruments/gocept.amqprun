# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

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
            return None
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
            return None
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
            return None
        self.assertTrue(zope.interface.verify.verifyObject(
            gocept.amqprun.interfaces.IHandler, handler))
        self.assertEquals('queue.name', handler.queue_name)
        self.assertEquals('routing.key', handler.routing_key)
        self.assertEquals('zope.user', handler.principal)
