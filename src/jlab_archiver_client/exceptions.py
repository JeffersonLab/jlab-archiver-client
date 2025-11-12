"""Custom exceptions for the jlab_archiver_client package.

This module defines custom exception classes used throughout the jlab_archiver_client
package for handling errors specific to archiver query operations and responses.

These exceptions provide more specific error handling than generic Python exceptions,
allowing users to catch and handle archiver-related errors separately from other
application errors.

Classes:
    MyqueryException: Base exception for myquery request and response errors.

Example::

    >>> from jlab_archiver_client.exceptions import MyqueryException
    >>> try:
    ...     # Some archiver operation that might fail
    ...     raise MyqueryException("Alert")
    ... except MyqueryException as e:
    ...     print(f"Archiver error: {e.message}")
    Archiver error: Alert

See Also:
    jlab_archiver_client.interval: Interval class that may raise these exceptions
    jlab_archiver_client.query: Query classes that may raise these exceptions
"""


class MyqueryException(Exception):
    """Exception representing an error while processing a myquery request or response."""

    def __init__(self, message: str) -> None:
        """Construct an instance of MyqueryException.

        Args:
            message: Description of exception cause
"""
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return self.message
