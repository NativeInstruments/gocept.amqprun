import gocept.amqprun.channel
import gocept.amqprun.interfaces
import gocept.testing.assertion
import mock
import unittest
import zope.interface.verify


class TestChannel(unittest.TestCase):

    def setUp(self):
        handler = mock.Mock()
        handler.async_map = {}
        self.channel = gocept.amqprun.channel.Channel(handler)

    def test_close_should_close_if_no_user(self):
        channel = self.channel
        channel.close = mock.Mock()
        channel.acquire()
        self.assertFalse(channel.close_if_possible())
        self.assertFalse(channel.close.called)
        channel.release()
        self.assertTrue(channel.close_if_possible())
        self.assertTrue(channel.close.called)

    def test_interface(self):
        from gocept.amqprun.interfaces import IChannelManager
        zope.interface.verify.verifyObject(IChannelManager, self.channel)


class ThreadSafeCounterTests(unittest.TestCase):
    """Testing ..channel.ThreadSafeCounter."""

    def test_channel__ThreadSafeCounter__dec__1(self):
        """It raises an exception if the value falls below zero."""
        counter = gocept.amqprun.channel.ThreadSafeCounter()
        with self.assertRaises(gocept.amqprun.interfaces.CounterBelowZero):
            counter.dec()


class CallerName(unittest.TestCase,
                 gocept.testing.assertion.Ellipsis):

    def test_caller_name(self):
        def foo():
            return gocept.amqprun.channel.caller_name()

        self.assertEllipsis(
            '<gocept.amqprun.tests.test_channel.CallerName...>'
            ':test_caller_name', foo())
