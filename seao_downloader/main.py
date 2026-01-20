#!/usr/bin/env python3
"""
SEAO JSON Downloader - Main CLI Entrypoint

Downloads JSON resources from the Québec SEAO open data portal
using the CKAN API for reliable resource discovery.

Usage:
    python -m seao_downloader.main --help
    python -m seao_downloader.main --out-dir ./data --rate-limit 0.5
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from .discovery import CKANDiscoveryClient, DiscoveryError, Resource
from .downloader import (
    ResourceDownloader,
    DownloadResult,
    AccessDeniedError,
    RateLimitExceededError,
)
from .persistence import (
    ManifestManager,
    FileNamer,
    validate_json_file,
)

# Default configuration
DEFAULT_OUT_DIR = "./seao_data"
DEFAULT_RATE_LIMIT = 1.0  # requests per second
DEFAULT_MAX_WORKERS = 2  # conservative default for politeness
DEFAULT_DATASET_ID = "systeme-electronique-dappel-doffres-seao"


def setup_logging(verbose: bool = False) -> None:
    """Configure logging based on verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="seao-downloader",
        description=(
            "Download JSON resources from the Québec SEAO open data portal. "
            "Uses the CKAN API for reliable resource discovery."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Download to ./seao_data
  %(prog)s --out-dir ./data          # Custom output directory
  %(prog)s --dry-run                 # List resources without downloading
  %(prog)s --resume                  # Skip already downloaded files
  %(prog)s --rate-limit 0.5          # Slower rate (0.5 req/sec)
  %(prog)s --max-workers 4           # Parallel downloads (use carefully)
        """,
    )

    parser.add_argument(
        "--out-dir",
        type=str,
        default=DEFAULT_OUT_DIR,
        help=f"Output directory for downloads (default: {DEFAULT_OUT_DIR})",
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=(
            f"Maximum parallel downloads (default: {DEFAULT_MAX_WORKERS}). "
            "Higher values may trigger rate limiting."
        ),
    )

    parser.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        help=(
            f"Maximum requests per second (default: {DEFAULT_RATE_LIMIT}). "
            "Lower values are more polite to the server."
        ),
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip resources already successfully downloaded (based on manifest).",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover resources and create manifest without downloading.",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (debug) logging.",
    )

    parser.add_argument(
        "--dataset-id",
        type=str,
        default=DEFAULT_DATASET_ID,
        help=f"CKAN dataset ID (default: {DEFAULT_DATASET_ID})",
    )

    parser.add_argument(
        "--list-all",
        action="store_true",
        help="List all resources (not just JSON) and exit.",
    )

    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help=(
            "Disable SSL certificate verification. "
            "NOT recommended for production; use only for testing."
        ),
    )

    return parser.parse_args()


class Orchestrator:
    """
    Main application orchestrator.

    Coordinates discovery, downloading, and persistence components
    according to CLI options.
    """

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out_dir = Path(args.out_dir)
        self.logger = logging.getLogger(__name__)

        # SSL verification setting
        verify_ssl = not args.no_verify_ssl
        if not verify_ssl:
            self.logger.warning(
                "SSL verification disabled. This is not recommended for production."
            )

        # Initialize components
        self.discovery = CKANDiscoveryClient(
            dataset_id=args.dataset_id,
            verify_ssl=verify_ssl,
        )
        self.downloader = ResourceDownloader(
            rate_limit=args.rate_limit,
            max_retries=3,
            verify_ssl=verify_ssl,
        )
        self.manifest = ManifestManager(
            out_dir=self.out_dir,
            dataset_id=args.dataset_id,
        )

        # Track statistics
        self.success_count = 0
        self.fail_count = 0
        self.skip_count = 0
        self.total_bytes = 0

    def run(self) -> int:
        """
        Execute the download workflow.

        Returns:
            Exit code (0 = success, 1 = error).
        """
        try:
            # Handle --list-all mode
            if self.args.list_all:
                return self._list_all_resources()

            # Discovery phase
            self.logger.info("=" * 50)
            self.logger.info("SEAO JSON Downloader")
            self.logger.info("=" * 50)

            resources = self._discover_resources()
            if not resources:
                self.logger.warning("No JSON resources found.")
                return 0

            self.logger.info(f"Discovered {len(resources)} JSON resources")

            # Filter already downloaded if resuming
            if self.args.resume:
                resources = self._filter_resume(resources)
                if not resources:
                    self.logger.info("All resources already downloaded. Nothing to do.")
                    self._print_summary()
                    return 0

            # Dry run mode
            if self.args.dry_run:
                return self._dry_run(resources)

            # Download phase
            return self._download_all(resources)

        except DiscoveryError as e:
            self.logger.error(f"Discovery failed: {e}")
            return 1

        except KeyboardInterrupt:
            self.logger.warning("Interrupted by user. Saving manifest...")
            self.manifest.save()
            return 130

    def _discover_resources(self) -> list[Resource]:
        """Discover JSON resources from CKAN API."""
        self.logger.info(f"Querying CKAN API for dataset: {self.args.dataset_id}")
        return self.discovery.discover_json_resources()

    def _filter_resume(self, resources: list[Resource]) -> list[Resource]:
        """Filter out already downloaded resources."""
        downloaded = self.manifest.get_successful_downloads()
        remaining = [r for r in resources if r.id not in downloaded]
        self.skip_count = len(resources) - len(remaining)

        if self.skip_count > 0:
            self.logger.info(
                f"Resuming: skipping {self.skip_count} already downloaded resources"
            )

        return remaining

    def _dry_run(self, resources: list[Resource]) -> int:
        """Execute dry run - list resources without downloading."""
        self.logger.info("\n[DRY RUN] Would download the following resources:\n")

        for i, res in enumerate(resources, 1):
            filename = FileNamer.generate(res)
            print(f"  {i:3}. {res.name}")
            print(f"       ID:  {res.id}")
            print(f"       URL: {res.url}")
            print(f"       -> {self.out_dir / filename}")
            print()

        print(f"Total: {len(resources)} JSON resources")
        print(f"Output directory: {self.out_dir}")
        return 0

    def _list_all_resources(self) -> int:
        """List all resources in the dataset (not just JSON)."""
        resources = self.discovery.discover_all_resources()

        print(f"\nAll resources in dataset '{self.args.dataset_id}':\n")

        for i, res in enumerate(resources, 1):
            print(f"  {i:3}. [{res.format:>8}] {res.name}")
            print(f"       URL: {res.url}")
            print()

        # Summary by format
        formats = {}
        for res in resources:
            fmt = res.format or "UNKNOWN"
            formats[fmt] = formats.get(fmt, 0) + 1

        print("Summary by format:")
        for fmt, count in sorted(formats.items(), key=lambda x: -x[1]):
            print(f"  {fmt}: {count}")

        return 0

    def _download_all(self, resources: list[Resource]) -> int:
        """Download all resources with optional parallelism."""
        self.logger.info(f"Starting download of {len(resources)} resources")
        self.logger.info(f"Output directory: {self.out_dir}")
        self.logger.info(f"Rate limit: {self.args.rate_limit} req/sec")
        self.logger.info(f"Max workers: {self.args.max_workers}")
        print()

        # Ensure output directory exists
        self.out_dir.mkdir(parents=True, exist_ok=True)

        try:
            if self.args.max_workers == 1:
                # Sequential download
                for res in resources:
                    self._download_single(res)
            else:
                # Parallel download
                with ThreadPoolExecutor(max_workers=self.args.max_workers) as executor:
                    futures = {
                        executor.submit(self._download_single, res): res
                        for res in resources
                    }
                    for future in as_completed(futures):
                        # Exceptions are handled inside _download_single
                        pass

        except (AccessDeniedError, RateLimitExceededError) as e:
            self.logger.critical(f"Stopping due to server restriction: {e}")
            self.manifest.save()
            self._print_summary()
            return 1

        finally:
            self.manifest.save()

        self._print_summary()
        return 0 if self.fail_count == 0 else 1

    def _download_single(self, resource: Resource) -> Optional[DownloadResult]:
        """Download a single resource."""
        filename = FileNamer.generate(resource)
        dest_path = self.out_dir / filename

        self.logger.info(f"Downloading: {resource.name}")
        self.logger.debug(f"  URL: {resource.url}")
        self.logger.debug(f"  Dest: {dest_path}")

        try:
            result = self.downloader.download(resource.url, dest_path)

            # Validate JSON if download succeeded
            is_valid = False
            if result.success:
                is_valid = validate_json_file(dest_path)
                if not is_valid:
                    self.logger.warning(
                        f"Downloaded file is not valid JSON: {dest_path}"
                    )

            # Record in manifest
            self.manifest.add_entry(resource, result, is_valid)

            # Update statistics
            if result.success and is_valid:
                self.success_count += 1
                self.total_bytes += result.file_size
                self.logger.info(
                    f"  ✓ Success: {filename} ({result.file_size:,} bytes)"
                )
            else:
                self.fail_count += 1
                self.logger.error(
                    f"  ✗ Failed: {filename} - {result.error_message or 'Invalid JSON'}"
                )

            return result

        except (AccessDeniedError, RateLimitExceededError):
            raise  # Propagate to stop all downloads

        except Exception as e:
            self.logger.error(f"  ✗ Unexpected error for {resource.name}: {e}")
            self.fail_count += 1
            return None

    def _print_summary(self) -> None:
        """Print human-readable summary."""
        summary = self.manifest.get_summary()

        print()
        print("=" * 50)
        print("DOWNLOAD SUMMARY")
        print("=" * 50)
        print(f"  Succeeded:    {self.success_count}")
        print(f"  Failed:       {self.fail_count}")
        print(f"  Skipped:      {self.skip_count}")
        print(f"  Total bytes:  {self.total_bytes:,} ({self._format_size(self.total_bytes)})")
        print(f"  Manifest:     {summary['manifest_path']}")
        print("=" * 50)

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format bytes as human-readable string."""
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"


def main() -> int:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    orchestrator = Orchestrator(args)
    return orchestrator.run()


if __name__ == "__main__":
    sys.exit(main())
