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
