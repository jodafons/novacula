
class RuntimeError(Exception):
    """Base class for errors raised by Qiskit."""

    def __init__(self, *message):
        """Set the error message."""
        super().__init__(" ".join(message))
        self.message = " ".join(message)

    def __str__(self):
        """Return the message."""
        return repr(self.message)

class RemoteCreationError(RuntimeError):
    """Raised when an error"""

    message = "its not possible to create a session. please set the remote first."

class ConnectionError(RuntimeError):
    """Raised when an error"""

    message = "the server connection is not found."

class TokenNotValidError(RuntimeError):
    """Raised when an error"""

    message = "the token is not valid."

class DownloadError(RuntimeError):
    """Raised when an error"""

    message = "failed to download content from server"

class DatasetNotFound(RuntimeError):
    """Raised when an error"""

    message = "dataset not found into the server"
    
    