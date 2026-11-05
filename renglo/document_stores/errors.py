"""Errors shared by the document stores."""


class StoreError(Exception):
    """The document store rejected the call."""

    def __init__(self, message, status=500, code=""):
        super().__init__(message)
        self.message = message
        self.status = int(status or 500)
        self.code = code or ""
        self.response = {
            "Error": {"Code": self.code, "Message": self.message},
            "ResponseMetadata": {"HTTPStatusCode": self.status},
        }


class Conflict(StoreError):
    """Exclusive create found an existing document."""


class PreconditionFailed(StoreError):
    """Compare-and-set failed because the document changed."""
