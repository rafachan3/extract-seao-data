"""
Discovery module - CKAN API client for resource URL discovery.

Responsibility: Communicate with the CKAN API to fetch dataset metadata
and filter resources by format (JSON).
"""

from dataclasses import dataclass
from typing import Optional
import logging
import ssl
import urllib.request
import urllib.error
import json

logger = logging.getLogger(__name__)

# Constants
CKAN_API_BASE = "https://www.donneesquebec.ca/recherche/api/3/action"
DEFAULT_DATASET_ID = "systeme-electronique-dappel-doffres-seao"
USER_AGENT = "SEAO-Downloader/1.0 (Quebec-OpenData-Client; Production)"


def create_ssl_context(verify: bool = True) -> ssl.SSLContext:
    """
    Create an SSL context for HTTPS requests.
    
    Args:
        verify: If True, verify SSL certificates. If False, skip verification
                (not recommended for production, but useful for testing).
    """
    if verify:
        return ssl.create_default_context()
    else:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        logger.warning("SSL verification disabled - not recommended for production")
        return ctx


@dataclass(frozen=True)
class Resource:
    """Immutable representation of a dataset resource."""

    id: str
    name: str
    url: str
    format: str
    description: Optional[str] = None
    size: Optional[int] = None
    last_modified: Optional[str] = None


class DiscoveryError(Exception):
    """Raised when resource discovery fails."""

    pass


class CKANDiscoveryClient:
    """
    Client for discovering resources via the CKAN Action API.

    The CKAN API provides structured metadata about datasets and their
    resources, which is more reliable than HTML scraping.

    API Reference: https://docs.ckan.org/en/latest/api/index.html
    """

    def __init__(
        self,
        dataset_id: str = DEFAULT_DATASET_ID,
        base_url: str = CKAN_API_BASE,
        timeout: int = 30,
        verify_ssl: bool = True,
    ):
        self.dataset_id = dataset_id
        self.base_url = base_url
        self.timeout = timeout
        self.ssl_context = create_ssl_context(verify_ssl)

    def _make_request(self, endpoint: str, params: dict) -> dict:
        """Make a request to the CKAN API."""
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{self.base_url}/{endpoint}?{query_string}"

        logger.debug(f"Requesting: {url}")

        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout, context=self.ssl_context
            ) as response:
                data = json.loads(response.read().decode("utf-8"))

                if not data.get("success"):
                    error_msg = data.get("error", {}).get("message", "Unknown API error")
                    raise DiscoveryError(f"CKAN API error: {error_msg}")

                return data["result"]

        except urllib.error.HTTPError as e:
            if e.code == 403:
                raise DiscoveryError(
                    f"Access denied (403). The dataset may require authentication "
                    f"or your IP may be rate-limited. URL: {url}"
                )
            elif e.code == 404:
                raise DiscoveryError(
                    f"Dataset not found (404). Verify the dataset ID: {self.dataset_id}"
                )
            elif e.code == 429:
                raise DiscoveryError(
                    f"Rate limited (429). Wait before retrying. "
                    f"Consider reducing --rate-limit."
                )
            else:
                raise DiscoveryError(f"HTTP error {e.code}: {e.reason}")

        except urllib.error.URLError as e:
            reason = str(e.reason)
            if "CERTIFICATE_VERIFY_FAILED" in reason:
                raise DiscoveryError(
                    f"SSL certificate verification failed. This is often a system "
                    f"configuration issue.\n"
                    f"  - On macOS: Run 'Install Certificates.command' from your "
                    f"Python installation folder\n"
                    f"  - Or use --no-verify-ssl to bypass (not recommended for production)\n"
                    f"Original error: {reason}"
                )
            raise DiscoveryError(f"Network error: {reason}")

        except json.JSONDecodeError as e:
            raise DiscoveryError(f"Invalid JSON response: {e}")

    def get_dataset_metadata(self) -> dict:
        """Fetch full dataset metadata from CKAN."""
        logger.info(f"Fetching metadata for dataset: {self.dataset_id}")
        return self._make_request("package_show", {"id": self.dataset_id})

    def discover_json_resources(self) -> list[Resource]:
        """
        Discover all JSON resources in the dataset.

        Filters resources where:
        - format field equals "JSON" (case-insensitive), OR
        - URL ends with ".json"

        Returns:
            List of Resource objects representing JSON resources.
        """
        metadata = self.get_dataset_metadata()
        resources = metadata.get("resources", [])

        logger.info(f"Found {len(resources)} total resources in dataset")

        json_resources = []
        for res in resources:
            res_format = res.get("format", "").upper()
            res_url = res.get("url", "")

            is_json = res_format == "JSON" or res_url.lower().endswith(".json")

            if is_json:
                resource = Resource(
                    id=res.get("id", ""),
                    name=res.get("name", res.get("id", "unknown")),
                    url=res_url,
                    format=res_format or "JSON",
                    description=res.get("description"),
                    size=res.get("size"),
                    last_modified=res.get("last_modified"),
                )
                json_resources.append(resource)
                logger.debug(f"Discovered JSON resource: {resource.name}")

        logger.info(f"Filtered to {len(json_resources)} JSON resources")
        return json_resources

    def discover_all_resources(self) -> list[Resource]:
        """Discover all resources regardless of format (for debugging)."""
        metadata = self.get_dataset_metadata()
        resources = metadata.get("resources", [])

        return [
            Resource(
                id=res.get("id", ""),
                name=res.get("name", res.get("id", "unknown")),
                url=res.get("url", ""),
                format=res.get("format", "UNKNOWN"),
                description=res.get("description"),
                size=res.get("size"),
                last_modified=res.get("last_modified"),
            )
            for res in resources
        ]
