# Copyright (c) 2010 gocept gmbh & co. kg
# See also LICENSE.txt

import mock
import pkg_resources
import unittest
import zope.component.testing
import zope.configuration.xmlconfig
import zope.interface.verify


class TestChannel(unittest.TestCase):

    def setUp(self):
        zope.component.testing.setUp()

    def tearDown(self):
        zope.component.testing.tearDown()

    def get_channel(self):
        import pika.channel
        return mock.Mock(pika.channel.Channel)

    def test_reference_count_should_be_per_channel(self):
        from gocept.amqprun.channel import Manager
        channel = self.get_channel()
        manager = Manager(channel)
        manager.acquire()
        manager.acquire()
        self.assertEqual(2, manager.get_count())
        manager = Manager(channel)
        self.assertEqual(2, manager.get_count())
        manager.release()
        self.assertEqual(1, manager.get_count())
        manager = Manager(channel)
        self.assertEqual(1, manager.get_count())

    def test_close_should_close_if_no_user(self):
        from gocept.amqprun.channel import Manager
        channel = self.get_channel()
        manager = Manager(channel)
        manager.acquire()
        self.assertFalse(manager.close_if_possible())
        self.assertFalse(channel.close.called)
        manager.release()
        self.assertTrue(manager.close_if_possible())
        self.assertTrue(channel.close.called)

    def test_interface(self):
        from gocept.amqprun.channel import Manager
        from gocept.amqprun.interfaces import IChannelManager
        channel = self.get_channel()
        manager = Manager(channel)
        zope.interface.verify.verifyObject(IChannelManager, manager)

    def test_channel_should_be_adaptable_to_manager(self):
        from gocept.amqprun.channel import Manager
        from gocept.amqprun.interfaces import IChannelManager
        import gocept.amqprun
        import pika.channel
        zope.configuration.xmlconfig.file(
            pkg_resources.resource_filename(__name__, '../configure.zcml'),
            package=gocept.amqprun)
        handler = mock.Mock()
        handler.async_map = {}
        channel = pika.channel.Channel(handler)
        manager = IChannelManager(channel)
