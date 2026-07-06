import unittest

from rejected import exceptions


class RejectedExceptionTestCase(unittest.TestCase):
    def test_value_kwarg_is_used_in_str(self):
        error = exceptions.MessageException(value='boom')
        self.assertEqual(str(error), 'boom')

    def test_kwargs_without_value_does_not_raise(self):
        error = exceptions.MessageException(code=42)
        # Should not raise IndexError; falls back to repr.
        self.assertEqual(str(error), 'MessageException()')

    def test_plain_message(self):
        error = exceptions.MessageException('a plain message')
        self.assertEqual(str(error), 'a plain message')

    def test_positional_format_string(self):
        error = exceptions.MessageException('bad value: {}', 42)
        self.assertEqual(str(error), 'bad value: 42')

    def test_single_positional_with_placeholder(self):
        # The lone positional is the template with no substitution args,
        # so the unfilled placeholder is returned verbatim rather than
        # the template being fed back into its own format() call.
        error = exceptions.MessageException('bad value: {0}')
        self.assertEqual(str(error), 'bad value: {0}')

    def test_keyword_format_string(self):
        error = exceptions.MessageException('value {v}', v=7)
        self.assertEqual(str(error), 'value 7')

    def test_value_kwarg_with_positional_format_args(self):
        error = exceptions.MessageException(5, value='code {0}')
        self.assertEqual(str(error), 'code 5')

    def test_no_args(self):
        error = exceptions.MessageException()
        self.assertEqual(str(error), 'MessageException()')
        self.assertEqual(repr(error), 'MessageException()')

    def test_bad_format_string_falls_back_to_value(self):
        error = exceptions.MessageException(value='needs {missing}')
        self.assertEqual(str(error), 'needs {missing}')

    def test_metric_captured(self):
        error = exceptions.ConsumerException('x', metric='my-metric')
        self.assertEqual(error.metric, 'my-metric')
        self.assertEqual(str(error), 'x')

    def test_repr_with_value(self):
        error = exceptions.MessageException(value='boom')
        self.assertEqual(repr(error), 'MessageException(boom)')
