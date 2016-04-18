import mock
import unittest
from gocept.amqprun.connection import Parameters


class ConnectionTest(unittest.TestCase):

    def get_parameters(self):
        return Parameters(
            hostname='amqp.gocept.com',
            port='2342',
            virtual_host='vhost_1',
            heartbeat_interval='23',
            username='amq_user',
            password='secret',
        )

    def test_parameters_should_be_converted_to_pika(self):
        from gocept.amqprun.connection import Connection
        with mock.patch('pika.AsyncoreConnection.__init__') as init, \
                mock.patch.object(Connection, 'is_open', return_value=True,
                                  new_callable=mock.PropertyMock):
            conn = Connection(self.get_parameters())
            conn.finish_init()
            params = init.call_args[0][1]
            assert 'amqp.gocept.com' == params.host
            assert 2342 == params.port
            assert 'vhost_1' == params.virtual_host
            assert 23 == params.heartbeat
            assert 'amq_user' == params.credentials.username
            assert 'secret' == params.credentials.password

    def test_no_username_and_password_should_yield_default_credentials(self):
        from gocept.amqprun.connection import Connection
        with mock.patch('pika.AsyncoreConnection.__init__') as init, \
                mock.patch.object(Connection, 'is_open', return_value=True,
                                  new_callable=mock.PropertyMock):
            parameters = self.get_parameters()
            parameters.username = None
            parameters.password = None
            conn = Connection(parameters)
            conn.finish_init()
            params = init.call_args[0][1]
            assert 'guest' == params.credentials.username
            assert 'guest' == params.credentials.password

    def test_finish_init_raises_RuntimeError_when_connection_cannot_be_opened(
            self):
        from gocept.amqprun.connection import Connection
        with mock.patch('pika.AsyncoreConnection.__init__'), \
                mock.patch.object(Connection, 'is_open', return_value=False,
                                  new_callable=mock.PropertyMock):
            conn = Connection(self.get_parameters())
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
