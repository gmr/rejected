import unittest

from rejected import errors


class RejectedExceptionTestCase(unittest.TestCase):
    def test_error_repr(self):
        err = errors.RejectedException('test {}', 'test-metric', 1)
        self.assertEqual(repr(err), 'RejectedException(test 1)')


class RabbitMQExceptionTestCase(unittest.TestCase):
    def test_error_repr(self):
        err = errors.RabbitMQException('conn1', 400, 'Test')
        self.assertEqual(
            repr(err), 'RabbitMQException(conn1 Error: (400) Test)')
