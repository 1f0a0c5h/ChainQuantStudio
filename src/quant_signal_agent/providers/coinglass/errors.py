"""CoinGlass provider errors that never expose response bodies or credentials."""


class CoinGlassError(RuntimeError):
    """Base error for CoinGlass communication or payload validation."""


class CoinGlassResponseError(CoinGlassError):
    """CoinGlass returned a permanent HTTP or schema error."""


class RetryableCoinGlassError(CoinGlassError):
    """A bounded retry may recover this CoinGlass request."""

    def __init__(
        self,
        message: str,
        *,
        retry_after: float | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_after = retry_after
        self.status_code = status_code
