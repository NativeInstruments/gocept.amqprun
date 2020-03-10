import unittest


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
