"""
Persistence module - Filesystem and manifest management.

Responsibility: Handle file naming, path management, and maintain
the download manifest for audit and resume support.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import json
import logging
import re

from .discovery import Resource
from .downloader import DownloadResult

logger = logging.getLogger(__name__)


@dataclass
class ManifestEntry:
    """Single entry in the download manifest."""

    resource_id: str
    resource_name: str
    source_url: str
    local_path: str
    download_timestamp: str
    http_status: int
    file_size_bytes: int
    is_valid_json: bool
    error_message: Optional[str] = None
    retry_count: int = 0


@dataclass
class Manifest:
    """Download manifest containing all resource entries."""

    version: str = "1.0"
    dataset_id: str = ""
    created_at: str = ""
    last_updated: str = ""
    entries: list[ManifestEntry] = None

    def __post_init__(self):
        if self.entries is None:
            self.entries = []


class FileNamer:
    """
    Deterministic filename generator.

    Creates collision-free filenames using resource ID prefix
    and sanitized resource name.
    """

    # Characters allowed in filenames (alphanumeric, dash, underscore, dot)
    SAFE_CHARS = re.compile(r"[^a-zA-Z0-9_\-.]")

    @classmethod
    def generate(cls, resource: Resource, extension: str = ".json") -> str:
        """
        Generate a deterministic, collision-free filename.

        Format: {resource_id_prefix}_{sanitized_name}{extension}

        Args:
            resource: Resource object with id and name.
            extension: File extension (default: .json).

        Returns:
            Safe filename string.
        """
        # Use first 8 chars of resource ID as prefix for uniqueness
        id_prefix = resource.id[:8] if resource.id else "unknown"

        # Sanitize the resource name
        name = resource.name or "resource"
        
        # Remove existing extension if present (avoid .json.json)
        if name.lower().endswith(extension.lower()):
            name = name[: -len(extension)]
        
        name = cls.SAFE_CHARS.sub("_", name)  # Replace unsafe chars
        name = re.sub(r"_+", "_", name)  # Collapse multiple underscores
        name = name.strip("_")[:50]  # Limit length

        if not name:
            name = "resource"

        filename = f"{id_prefix}_{name}{extension}"
        return filename


class ManifestManager:
    """
    Manages the download manifest for tracking and resume support.

    The manifest is a JSON file that records:
    - All download attempts (success and failure)
    - Validation status
    - Timestamps for audit purposes
    """

    MANIFEST_FILENAME = "manifest.json"

    def __init__(self, out_dir: Path, dataset_id: str):
        """
        Initialize the manifest manager.

        Args:
            out_dir: Output directory for downloads and manifest.
            dataset_id: CKAN dataset identifier.
        """
        self.out_dir = Path(out_dir)
        self.dataset_id = dataset_id
        self.manifest_path = self.out_dir / self.MANIFEST_FILENAME
        self.manifest = self._load_or_create()

    def _load_or_create(self) -> Manifest:
        """Load existing manifest or create a new one."""
        if self.manifest_path.exists():
            try:
                data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
                entries = [ManifestEntry(**e) for e in data.get("entries", [])]
                return Manifest(
                    version=data.get("version", "1.0"),
                    dataset_id=data.get("dataset_id", self.dataset_id),
                    created_at=data.get("created_at", ""),
                    last_updated=data.get("last_updated", ""),
                    entries=entries,
                )
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.warning(f"Could not parse existing manifest: {e}. Creating new.")

        now = datetime.now(timezone.utc).isoformat()
        return Manifest(
            dataset_id=self.dataset_id,
            created_at=now,
            last_updated=now,
        )

    def is_downloaded(self, resource_id: str) -> bool:
        """Check if a resource was successfully downloaded."""
        for entry in self.manifest.entries:
            if entry.resource_id == resource_id and entry.is_valid_json:
                return True
        return False

    def get_successful_downloads(self) -> set[str]:
        """Get set of resource IDs that were successfully downloaded."""
        return {
            e.resource_id for e in self.manifest.entries if e.is_valid_json
        }

    def add_entry(
        self,
        resource: Resource,
        result: DownloadResult,
        is_valid_json: bool,
    ) -> ManifestEntry:
        """
        Add a download result to the manifest.

        Args:
            resource: The resource that was downloaded.
            result: The download result.
            is_valid_json: Whether the downloaded file is valid JSON.

        Returns:
            The created ManifestEntry.
        """
        entry = ManifestEntry(
            resource_id=resource.id,
            resource_name=resource.name,
            source_url=resource.url,
            local_path=str(result.local_path) if result.local_path else "",
            download_timestamp=datetime.now(timezone.utc).isoformat(),
            http_status=result.http_status,
            file_size_bytes=result.file_size,
            is_valid_json=is_valid_json,
            error_message=result.error_message,
            retry_count=result.retry_count,
        )

        self.manifest.entries.append(entry)
        self.manifest.last_updated = datetime.now(timezone.utc).isoformat()

        return entry

    def save(self) -> None:
        """Persist manifest to disk."""
        self.out_dir.mkdir(parents=True, exist_ok=True)

        data = {
            "version": self.manifest.version,
            "dataset_id": self.manifest.dataset_id,
            "created_at": self.manifest.created_at,
            "last_updated": self.manifest.last_updated,
            "entries": [asdict(e) for e in self.manifest.entries],
        }

        self.manifest_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.debug(f"Manifest saved to {self.manifest_path}")

    def get_summary(self) -> dict:
        """Get summary statistics for reporting."""
        entries = self.manifest.entries
        successful = [e for e in entries if e.is_valid_json]
        failed = [e for e in entries if not e.is_valid_json]

        return {
            "total": len(entries),
            "succeeded": len(successful),
            "failed": len(failed),
            "total_bytes": sum(e.file_size_bytes for e in successful),
            "manifest_path": str(self.manifest_path),
        }


def validate_json_file(file_path: Path) -> bool:
    """
    Validate that a file contains valid JSON.

    Args:
        file_path: Path to the file to validate.

    Returns:
        True if file is valid JSON, False otherwise.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            json.load(f)
        return True
    except (json.JSONDecodeError, UnicodeDecodeError, IOError) as e:
        logger.warning(f"JSON validation failed for {file_path}: {e}")
        return False
