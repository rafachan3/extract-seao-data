#!/usr/bin/env python3
"""
Silver Layer Extraction CLI.

Extracts complete flattened data from bronze (raw JSON) to silver (clean CSV/Parquet).

Usage:
    python -m seao_downloader.extract_silver --help
    python -m seao_downloader.extract_silver --data-dir ./data
    python -m seao_downloader.extract_silver --data-dir ./data --format parquet
"""

import argparse
import logging
import sys
from pathlib import Path

from .silver_layer import SilverLayerExtractor, extract_silver_layer


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="seao-extract-silver",
        description=(
            "Extract silver layer (complete flattened CSV/Parquet) from bronze layer (raw JSON). "
            "Includes ALL fields from the OCDS data for comprehensive analysis."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --data-dir ./data                      # Extract to CSV
  %(prog)s --data-dir ./data --format parquet     # Extract to Parquet
  %(prog)s --data-dir ./data --output ./silver.csv
        """,
    )

    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Directory containing bronze layer JSON files.",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output file path (default: data-dir/silver_layer.csv).",
    )

    parser.add_argument(
        "--format",
        type=str,
        choices=["csv", "parquet"],
        default="csv",
        help="Output format (default: csv). Parquet requires pyarrow.",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging.",
    )

    return parser.parse_args()


def format_number(n: int) -> str:
    """Format number with commas."""
    return f"{n:,}"


def main() -> int:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)
    
    logger = logging.getLogger(__name__)
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory does not exist: {data_dir}")
        return 1
    
    output_path = Path(args.output) if args.output else None
    
    print()
    print("=" * 70)
    print("SEAO SILVER LAYER EXTRACTION")
    print("=" * 70)
    print(f"  Data directory: {data_dir}")
    print(f"  Output format:  {args.format.upper()}")
    print()
    
    try:
        extractor = extract_silver_layer(
            data_dir=data_dir,
            output_path=output_path,
            format=args.format,
        )
        
        # Print summary
        print()
        print("=" * 70)
        print("EXTRACTION COMPLETE")
        print("=" * 70)
        print(f"  Files processed:    {format_number(extractor.files_processed)}")
        print(f"  Records extracted:  {format_number(extractor.releases_processed)}")
        print()
        
        # Print column summary
        from .silver_layer import SilverRecord
        fields = list(SilverRecord.__dataclass_fields__.keys())
        print(f"  Columns in output:  {len(fields)}")
        print()
        print("  Column groups:")
        print("    - Release metadata:    6 columns")
        print("    - Buyer info:          8 columns")
        print("    - Tender info:        14 columns")
        print("    - Classifications:     8 columns")
        print("    - Bids:                6 columns")
        print("    - Awards:              6 columns")
        print("    - Supplier info:       8 columns")
        print("    - Contract info:       6 columns")
        print("    - Documents:           1 column")
        print("    - Source metadata:     2 columns")
        print()
        
        if output_path:
            print(f"  Output file: {output_path}")
        else:
            ext = ".parquet" if args.format == "parquet" else ".csv"
            print(f"  Output file: {data_dir / f'silver_layer{ext}'}")
        
        print("=" * 70)
        print()
        print("NOTE: CSV includes UTF-8 BOM for proper French character display in Excel.")
        print()
        
        return 0
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
