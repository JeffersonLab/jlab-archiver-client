import unittest

from jlab_archiver_client.exceptions import MyqueryException


class TestMyqueryException(unittest.TestCase):
    """Test cases for MyqueryException class."""

    def test_init_with_message(self):
        """Test MyqueryException initialization with a message."""
        message = "Test error message"
        exception = MyqueryException(message)

        self.assertEqual(exception.message, message)

    def test_str_representation(self):
        """Test string representation of MyqueryException."""
        message = "Test error message"
        exception = MyqueryException(message)

        self.assertEqual(str(exception), message)

    def test_exception_inheritance(self):
        """Test that MyqueryException inherits from Exception."""
        exception = MyqueryException("Test")

        self.assertIsInstance(exception, Exception)

    def test_raise_and_catch(self):
        """Test raising and catching MyqueryException."""
        message = "Something went wrong"

        with self.assertRaises(MyqueryException) as context:
            raise MyqueryException(message)

        self.assertEqual(str(context.exception), message)
        self.assertEqual(context.exception.message, message)

    def test_exception_with_empty_message(self):
        """Test MyqueryException with an empty message."""
        exception = MyqueryException("")

        self.assertEqual(exception.message, "")
        self.assertEqual(str(exception), "")

    def test_exception_with_multiline_message(self):
        """Test MyqueryException with a multiline message."""
        message = "Line 1\nLine 2\nLine 3"
        exception = MyqueryException(message)

        self.assertEqual(exception.message, message)
        self.assertEqual(str(exception), message)

    def test_exception_with_special_characters(self):
        """Test MyqueryException with special characters in message."""
        message = "Error: 'value' is invalid! @#$%^&*()"
        exception = MyqueryException(message)

        self.assertEqual(exception.message, message)
        self.assertEqual(str(exception), message)

    def test_exception_message_attribute_accessible(self):
        """Test that the message attribute is accessible after creation."""
        message = "Alert"
        exception = MyqueryException(message)

        # Access message attribute directly
        self.assertEqual(exception.message, "Alert")

    def test_catch_as_base_exception(self):
        """Test catching MyqueryException as a base Exception."""
        message = "Generic error"

        try:
            raise MyqueryException(message)
        except Exception as e:
            self.assertIsInstance(e, MyqueryException)
            self.assertEqual(str(e), message)

    def test_exception_args(self):
        """Test that exception args are properly set."""
        message = "Test message"
        exception = MyqueryException(message)

        # Exception should have args tuple
        self.assertEqual(exception.args, (message,))


if __name__ == '__main__':
    unittest.main()
