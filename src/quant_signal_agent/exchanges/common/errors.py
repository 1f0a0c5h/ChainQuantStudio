"""Errors raised by public exchange adapters."""


class ExchangeError(RuntimeError):
    """Base error for public exchange communication or payload validation."""


class ExchangeResponseError(ExchangeError):
    """The exchange returned an error response or malformed payload."""


class ExchangeConnectionError(ExchangeError):
    """A public REST or WebSocket connection could not be maintained."""

    def __init__(
        self,
        message: str,
        *,
        failure_stage: str | None = None,
        root_error_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.failure_stage = failure_stage
        self.root_error_type = root_error_type


class RetryableExchangeError(ExchangeConnectionError):
    """A bounded retry may recover this public exchange request."""

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None = None,
        status_code: int | None = None,
        failure_stage: str | None = None,
        root_error_type: str | None = None,
    ) -> None:
        super().__init__(
            message,
            failure_stage=failure_stage,
            root_error_type=root_error_type,
        )
        self.retry_after = retry_after
        self.status_code = status_code
