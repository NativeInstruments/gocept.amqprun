# Copyright (c) 2010-2011 gocept gmbh & co. kg
# See also LICENSE.txt

import mock
import unittest


class ConnectionTest(unittest.TestCase):

    def test_parameters_should_be_converted_to_pika(self):
        from gocept.amqprun.connection import Connection
        with mock.patch('pika.AsyncoreConnection.__init__') as init:
            conn = Connection(mock.sentinel)
            conn.finish_init()
            params = init.call_args[0][1]
            self.assertEqual(mock.sentinel.hostname, params.host)
            self.assertEqual(mock.sentinel.port, params.port)
            self.assertEqual(mock.sentinel.virtual_host, params.virtual_host)
            self.assertEqual(
                mock.sentinel.heartbeat_interval, params.heartbeat)
            self.assertEqual(
                mock.sentinel.username, params.credentials.username)
            self.assertEqual(
                mock.sentinel.password, params.credentials.password)

    def test_no_username_and_password_should_yield_default_credentials(self):
        from gocept.amqprun.connection import Connection
        with mock.patch('pika.AsyncoreConnection.__init__') as init:
            parameters = mock.Mock()
            parameters.username = None
            parameters.password = None
            conn = Connection(parameters)
            conn.finish_init()
            params = init.call_args[0][1]
            self.assertIsNone(params.credentials)
