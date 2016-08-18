import mock
import unittest


class ConnectionTest(unittest.TestCase):

    def test_parameters_should_be_converted_to_pika(self):
        from gocept.amqprun.connection import Connection
        is_alive = 'gocept.amqprun.connection.Connection.is_alive'
        with mock.patch('pika.AsyncoreConnection.__init__') as init, \
                mock.patch(is_alive, return_value=True):
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
        is_alive = 'gocept.amqprun.connection.Connection.is_alive'
        with mock.patch('pika.AsyncoreConnection.__init__') as init, \
                mock.patch(is_alive, return_value=True):
            parameters = mock.Mock()
            parameters.username = None
            parameters.password = None
            conn = Connection(parameters)
            conn.finish_init()
            params = init.call_args[0][1]
            self.assertIsNone(params.credentials)

    def test_finish_init_raises_RuntimeError_when_connection_cannot_be_opened(
            self):
        from gocept.amqprun.connection import Connection
        is_alive = 'gocept.amqprun.connection.Connection.is_alive'
        with mock.patch('pika.AsyncoreConnection.__init__'), \
                mock.patch(is_alive, return_value=False):
            conn = Connection(mock.sentinel)
            with self.assertRaises(RuntimeError) as err:
                conn.finish_init()
            self.assertEqual(
                'Connection not alive after connect, maybe the credentials '
                'are wrong.', str(err.exception))


class ParametersTest(unittest.TestCase):
    """Testing ..connection.Parameters."""

    def test_bails_on_unknown_keyword_argument(self):
        from ..connection import Parameters
        with self.assertRaises(TypeError) as err:
            Parameters(virtualhost='vhost3')
        self.assertEqual(
            "__init__() got an unexpected keyword argument 'virtualhost'",
            str(err.exception))

    def test_connection__Parameters__1(self):
        """It converts a strings to an integer as port number if necessary."""
        from ..connection import Parameters
        params = Parameters(port='1234')
        self.assertEqual(1234, params.port)
