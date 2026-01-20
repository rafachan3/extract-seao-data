"""
Downloader module - HTTP client with retries and rate limiting.

Responsibility: Handle HTTP downloads with resilience (retries, backoff)
and politeness (rate limiting).
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import logging
import ssl
import time
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

USER_AGENT = "SEAO-Downloader/1.0 (Quebec-OpenData-Client; Production)"


def create_ssl_context(verify: bool = True) -> ssl.SSLContext:
    """Create an SSL context for HTTPS requests."""
    if verify:
        return ssl.create_default_context()
    else:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx


@dataclass
class DownloadResult:
    """Result of a download attempt."""

    success: bool
    url: str
    local_path: Optional[Path]
    http_status: int
    file_size: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0


class DownloadError(Exception):
    """Raised when a download fails after all retries."""

    pass


class RateLimitExceededError(Exception):
    """Raised when server returns 429 - signals to stop entirely."""

    pass


class AccessDeniedError(Exception):
    """Raised when server returns 403 - signals to stop entirely."""

    pass


@dataclass
class RateLimiter:
    """
    Simple rate limiter using token bucket algorithm.

    Ensures minimum delay between requests to be polite to the server.
    """

    requests_per_second: float
    _last_request_time: float = field(default=0.0, init=False)

    def wait(self) -> None:
        """Block until it's safe to make another request."""
        if self.requests_per_second <= 0:
            return

        min_interval = 1.0 / self.requests_per_second
        elapsed = time.time() - self._last_request_time

        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
            time.sleep(sleep_time)

        self._last_request_time = time.time()


class ResourceDownloader:
    """
    HTTP downloader with exponential backoff and rate limiting.

    Handles transient failures gracefully while respecting server limits.
    Stops immediately on 403/429 to avoid IP banning.
    """

    def __init__(
        self,
        rate_limit: float = 1.0,
        max_retries: int = 3,
        base_backoff: float = 2.0,
        timeout: int = 60,
        verify_ssl: bool = True,
    ):
        """
        Initialize the downloader.

        Args:
            rate_limit: Maximum requests per second.
            max_retries: Number of retry attempts for transient failures.
            base_backoff: Base delay for exponential backoff (seconds).
            timeout: HTTP request timeout (seconds).
            verify_ssl: Whether to verify SSL certificates.
        """
        self.rate_limiter = RateLimiter(rate_limit)
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.timeout = timeout
        self.ssl_context = create_ssl_context(verify_ssl)

    def download(self, url: str, dest_path: Path) -> DownloadResult:
        """
        Download a resource to the specified path.

        Implements exponential backoff for transient errors (5xx, timeouts).
        Raises immediately on 403/429 to signal the caller to stop.

        Args:
            url: Source URL to download.
            dest_path: Local filesystem path to save the file.

        Returns:
            DownloadResult with status and metadata.

        Raises:
            AccessDeniedError: On HTTP 403.
            RateLimitExceededError: On HTTP 429.
        """
        last_error: Optional[str] = None
        retry_count = 0

        for attempt in range(self.max_retries + 1):
            self.rate_limiter.wait()

            try:
                return self._attempt_download(url, dest_path, attempt)

            except AccessDeniedError:
                raise  # Don't retry, bubble up immediately

            except RateLimitExceededError:
                raise  # Don't retry, bubble up immediately

            except urllib.error.HTTPError as e:
                last_error = f"HTTP {e.code}: {e.reason}"
                retry_count = attempt + 1

                # Retry on server errors
                if e.code >= 500:
                    backoff = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Server error {e.code} for {url}. "
                        f"Retry {retry_count}/{self.max_retries} in {backoff:.1f}s"
                    )
                    time.sleep(backoff)
                else:
                    # Client errors (4xx except 403/429) - don't retry
                    break

            except urllib.error.URLError as e:
                last_error = f"Network error: {e.reason}"
                retry_count = attempt + 1
                backoff = self._calculate_backoff(attempt)
                logger.warning(
                    f"Network error for {url}: {e.reason}. "
                    f"Retry {retry_count}/{self.max_retries} in {backoff:.1f}s"
                )
                time.sleep(backoff)

            except TimeoutError:
                last_error = "Request timed out"
                retry_count = attempt + 1
                backoff = self._calculate_backoff(attempt)
                logger.warning(
                    f"Timeout for {url}. "
                    f"Retry {retry_count}/{self.max_retries} in {backoff:.1f}s"
                )
                time.sleep(backoff)

        # All retries exhausted
        logger.error(f"Download failed after {retry_count} attempts: {url}")
        return DownloadResult(
            success=False,
            url=url,
            local_path=dest_path,
            http_status=0,
            error_message=last_error,
            retry_count=retry_count,
        )

    def _attempt_download(
        self, url: str, dest_path: Path, attempt: int
    ) -> DownloadResult:
        """Single download attempt."""
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json, */*",
            },
        )

        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout, context=self.ssl_context
            ) as response:
                http_status = response.status

                # Stream to file to handle large downloads
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with open(dest_path, "wb") as f:
                    while chunk := response.read(8192):
                        f.write(chunk)

                file_size = dest_path.stat().st_size
                logger.debug(f"Downloaded {file_size} bytes to {dest_path}")

                return DownloadResult(
                    success=True,
                    url=url,
                    local_path=dest_path,
                    http_status=http_status,
                    file_size=file_size,
                    retry_count=attempt,
                )

        except urllib.error.HTTPError as e:
            if e.code == 403:
                logger.critical(
                    f"ACCESS DENIED (403) for {url}. "
                    "The server is blocking requests. Possible causes:\n"
                    "  - IP rate limiting\n"
                    "  - Authentication required\n"
                    "  - Geographic restrictions\n"
                    "ACTION: Wait and retry later, or check if auth is needed."
                )
                raise AccessDeniedError(f"HTTP 403 for {url}")

            elif e.code == 429:
                logger.critical(
                    f"RATE LIMITED (429) for {url}. "
                    "Too many requests. The server is throttling.\n"
                    "ACTION: Reduce --rate-limit and wait before retrying."
                )
                raise RateLimitExceededError(f"HTTP 429 for {url}")

            raise  # Re-raise for retry logic

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff delay."""
        return self.base_backoff * (2**attempt)
