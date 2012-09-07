# Copyright (c) 2010-2012 gocept gmbh & co. kg
# See also LICENSE.txt

import gocept.amqprun.channel
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


class CallerName(unittest.TestCase,
                 gocept.testing.assertion.Ellipsis):

    def test_caller_name(self):
        def foo():
            return gocept.amqprun.channel.caller_name()

        self.assertEllipsis(
            '<gocept.amqprun.tests.test_channel.CallerName...>'
            ':test_caller_name', foo())
