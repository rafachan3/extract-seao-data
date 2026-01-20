#!/usr/bin/env python3
"""
SEAO Data Analyzer - CLI for analyzing downloaded SEAO data.

Aggregates bid/award data by UNSPSC category and exports to CSV/JSON.

Usage:
    python -m seao_downloader.analyze --help
    python -m seao_downloader.analyze --data-dir ./data
    python -m seao_downloader.analyze --data-dir ./data --export-records
"""

import argparse
import logging
import sys
from pathlib import Path

from .analyzer import (
    OCDSParser,
    DataAggregator,
    AnalysisExporter,
    analyze_data_directory,
)


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
        prog="seao-analyzer",
        description=(
            "Analyze downloaded SEAO JSON data. "
            "Aggregates bid amounts by UNSPSC category and exports summaries."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --data-dir ./data                  # Analyze data in ./data
  %(prog)s --data-dir ./data --export-records # Also export all bid records
  %(prog)s --data-dir ./data --output-dir ./analysis
  %(prog)s --data-dir ./data --top 20         # Show top 20 categories
        """,
    )

    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        help="Directory containing downloaded JSON files.",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for analysis files (default: same as data-dir).",
    )

    parser.add_argument(
        "--export-records",
        action="store_true",
        help="Export all individual bid records to CSV (can be large).",
    )

    parser.add_argument(
        "--top",
        type=int,
        default=25,
        help="Number of top categories to display in summary (default: 25).",
    )

    parser.add_argument(
        "--sort-by",
        type=str,
        choices=["total_award_value", "total_tenders", "unspsc_code"],
        default="total_award_value",
        help="Field to sort summaries by (default: total_award_value).",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose (debug) logging.",
    )

    return parser.parse_args()


def format_currency(amount: float) -> str:
    """Format amount as currency string."""
    if amount >= 1_000_000_000:
        return f"${amount/1_000_000_000:.2f}B"
    elif amount >= 1_000_000:
        return f"${amount/1_000_000:.2f}M"
    elif amount >= 1_000:
        return f"${amount/1_000:.1f}K"
    else:
        return f"${amount:.2f}"


def print_summary(aggregator: DataAggregator, top_n: int, sort_by: str) -> None:
    """Print analysis summary to console."""
    summaries = aggregator.get_summaries_sorted(by=sort_by)
    
    print()
    print("=" * 80)
    print("SEAO DATA ANALYSIS SUMMARY")
    print("=" * 80)
    print()
    print(f"  Files processed:     {aggregator.total_files_processed}")
    print(f"  Total releases:      {aggregator.total_releases_processed:,}")
    print(f"  Unique UNSPSC codes: {len(summaries)}")
    print()
    
    # Calculate totals
    total_bids = sum(s.total_bids for s in summaries)
    total_awards = sum(s.total_awards for s in summaries)
    total_bid_value = sum(s.total_bid_value for s in summaries)
    total_award_value = sum(s.total_award_value for s in summaries)
    
    print(f"  Total bids:          {total_bids:,}")
    print(f"  Total awards:        {total_awards:,}")
    print(f"  Total bid value:     {format_currency(total_bid_value)}")
    print(f"  Total award value:   {format_currency(total_award_value)}")
    print()
    
    # Top categories
    print("-" * 80)
    print(f"TOP {top_n} UNSPSC CATEGORIES BY {sort_by.upper().replace('_', ' ')}")
    print("-" * 80)
    print()
    print(f"{'Rank':<5} {'UNSPSC':<12} {'Tenders':>10} {'Awards':>10} {'Total Value':>15}  Description")
    print("-" * 80)
    
    for i, s in enumerate(summaries[:top_n], 1):
        code = s.unspsc_code[:10] if s.unspsc_code else "N/A"
        desc = (s.unspsc_description or "Unknown")[:35]
        value_str = format_currency(s.total_award_value)
        
        print(f"{i:<5} {code:<12} {s.total_tenders:>10,} {s.total_awards:>10,} {value_str:>15}  {desc}")
    
    print("-" * 80)
    print()
    
    # Category breakdown (Quebec-specific categories)
    category_totals: dict[str, float] = {}
    category_counts: dict[str, int] = {}
    
    for s in summaries:
        cat = s.category_code or "UNCLASSIFIED"
        category_totals[cat] = category_totals.get(cat, 0) + s.total_award_value
        category_counts[cat] = category_counts.get(cat, 0) + s.total_tenders
    
    print("BREAKDOWN BY QUEBEC CATEGORY CODE")
    print("-" * 80)
    print(f"{'Category':<15} {'Tenders':>12} {'Total Award Value':>20}")
    print("-" * 80)
    
    for cat, value in sorted(category_totals.items(), key=lambda x: -x[1]):
        count = category_counts[cat]
        print(f"{cat:<15} {count:>12,} {format_currency(value):>20}")
    
    print("-" * 80)
    print()


def main() -> int:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)
    
    logger = logging.getLogger(__name__)
    
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error(f"Data directory does not exist: {data_dir}")
        return 1
    
    output_dir = Path(args.output_dir) if args.output_dir else data_dir
    
    print()
    print("=" * 80)
    print("SEAO DATA ANALYZER")
    print("=" * 80)
    print(f"  Data directory:   {data_dir}")
    print(f"  Output directory: {output_dir}")
    print()
    
    try:
        # Run analysis
        aggregator = analyze_data_directory(
            data_dir=data_dir,
            output_dir=output_dir,
            export_records=args.export_records,
        )
        
        # Print summary
        print_summary(aggregator, args.top, args.sort_by)
        
        # Print output files
        print("OUTPUT FILES")
        print("-" * 80)
        print(f"  UNSPSC Summary (CSV):  {output_dir / 'unspsc_summary.csv'}")
        print(f"  UNSPSC Summary (JSON): {output_dir / 'unspsc_summary.json'}")
        if args.export_records:
            print(f"  All Bid Records (CSV): {output_dir / 'all_bids.csv'}")
        print()
        print("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
