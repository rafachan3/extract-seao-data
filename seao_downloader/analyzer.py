"""
Analyzer module - Extract and aggregate bid data by UNSPSC category.

Responsibility: Parse downloaded OCDS JSON files and produce structured
summaries of bid amounts grouped by UNSPSC classification codes.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Iterator
import csv
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class BidRecord:
    """Single bid/award record with UNSPSC classification."""

    ocid: str  # Open Contracting ID
    release_id: str
    release_date: Optional[str]
    
    # Tender info
    tender_id: str
    tender_title: str
    tender_status: str
    procurement_category: str  # goods, works, services
    
    # UNSPSC classification
    unspsc_code: Optional[str]
    unspsc_description: Optional[str]
    
    # Additional category (Quebec-specific)
    category_code: Optional[str]
    category_description: Optional[str]
    
    # Buyer info
    buyer_id: str
    buyer_name: str
    
    # Value info
    bid_value: Optional[float]
    award_value: Optional[float]
    currency: str = "CAD"
    
    # Supplier info (from award)
    supplier_id: Optional[str] = None
    supplier_name: Optional[str] = None


@dataclass
class UNSPSCSummary:
    """Aggregated statistics for a single UNSPSC code."""

    unspsc_code: str
    unspsc_description: str
    category_code: Optional[str] = None
    category_description: Optional[str] = None
    
    # Counts
    total_tenders: int = 0
    total_bids: int = 0
    total_awards: int = 0
    
    # Values (in CAD)
    total_bid_value: float = 0.0
    total_award_value: float = 0.0
    min_bid_value: Optional[float] = None
    max_bid_value: Optional[float] = None
    min_award_value: Optional[float] = None
    max_award_value: Optional[float] = None
    
    def add_bid(self, value: float) -> None:
        """Add a bid value to the summary."""
        self.total_bids += 1
        self.total_bid_value += value
        if self.min_bid_value is None or value < self.min_bid_value:
            self.min_bid_value = value
        if self.max_bid_value is None or value > self.max_bid_value:
            self.max_bid_value = value
    
    def add_award(self, value: float) -> None:
        """Add an award value to the summary."""
        self.total_awards += 1
        self.total_award_value += value
        if self.min_award_value is None or value < self.min_award_value:
            self.min_award_value = value
        if self.max_award_value is None or value > self.max_award_value:
            self.max_award_value = value
    
    @property
    def avg_bid_value(self) -> Optional[float]:
        """Average bid value."""
        return self.total_bid_value / self.total_bids if self.total_bids > 0 else None
    
    @property
    def avg_award_value(self) -> Optional[float]:
        """Average award value."""
        return self.total_award_value / self.total_awards if self.total_awards > 0 else None


class OCDSParser:
    """
    Parser for Open Contracting Data Standard (OCDS) JSON files.
    
    Extracts bid records with UNSPSC classifications from SEAO data.
    """
    
    def parse_file(self, file_path: Path) -> Iterator[BidRecord]:
        """
        Parse a single OCDS JSON file and yield BidRecords.
        
        Args:
            file_path: Path to the JSON file.
            
        Yields:
            BidRecord for each release with classification data.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to parse {file_path}: {e}")
            return
        
        releases = data.get("releases", [])
        logger.debug(f"Processing {len(releases)} releases from {file_path.name}")
        
        for release in releases:
            yield from self._parse_release(release)
    
    def _parse_release(self, release: dict) -> Iterator[BidRecord]:
        """Parse a single OCDS release."""
        ocid = release.get("ocid", "")
        release_id = release.get("id", "")
        release_date = release.get("date")
        
        # Get tender info
        tender = release.get("tender", {})
        tender_id = tender.get("id", "")
        tender_title = tender.get("title", "")
        tender_status = tender.get("status", "")
        procurement_category = tender.get("mainProcurementCategory", "")
        
        # Get buyer info
        buyer = release.get("buyer", {})
        buyer_id = buyer.get("id", "")
        buyer_name = buyer.get("name", "")
        
        # Extract UNSPSC classifications from items
        items = tender.get("items", [])
        classifications = self._extract_classifications(items)
        
        # If no items with classification, use a default entry
        if not classifications:
            classifications = [(None, None, None, None)]
        
        # Get bid values
        bids = release.get("bids", [])
        bid_values = [b.get("value") for b in bids if b.get("value") is not None]
        
        # Get award info
        awards = release.get("awards", [])
        
        # Create records for each classification
        for unspsc_code, unspsc_desc, cat_code, cat_desc in classifications:
            # Aggregate bid values for this release
            total_bid = sum(bid_values) if bid_values else None
            
            # Get award value and supplier
            award_value = None
            supplier_id = None
            supplier_name = None
            
            for award in awards:
                if award.get("status") == "active":
                    value_obj = award.get("value", {})
                    if isinstance(value_obj, dict):
                        award_value = value_obj.get("amount")
                    suppliers = award.get("suppliers", [])
                    if suppliers:
                        supplier_id = suppliers[0].get("id")
                        supplier_name = suppliers[0].get("name")
                    break
            
            yield BidRecord(
                ocid=ocid,
                release_id=release_id,
                release_date=release_date,
                tender_id=tender_id,
                tender_title=tender_title,
                tender_status=tender_status,
                procurement_category=procurement_category,
                unspsc_code=unspsc_code,
                unspsc_description=unspsc_desc,
                category_code=cat_code,
                category_description=cat_desc,
                buyer_id=buyer_id,
                buyer_name=buyer_name,
                bid_value=total_bid,
                award_value=award_value,
                supplier_id=supplier_id,
                supplier_name=supplier_name,
            )
    
    def _extract_classifications(
        self, items: list
    ) -> list[tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        """
        Extract UNSPSC and category classifications from tender items.
        
        Returns:
            List of tuples: (unspsc_code, unspsc_desc, category_code, category_desc)
        """
        classifications = []
        
        for item in items:
            unspsc_code = None
            unspsc_desc = None
            cat_code = None
            cat_desc = None
            
            # Primary classification (UNSPSC)
            classification = item.get("classification", {})
            if classification.get("scheme") == "UNSPSC":
                unspsc_code = classification.get("id")
                unspsc_desc = classification.get("description")
            
            # Additional classifications (Category)
            additional = item.get("additionalClassifications", [])
            for add_class in additional:
                if add_class.get("scheme") == "CATEGORY":
                    cat_code = add_class.get("id")
                    cat_desc = add_class.get("description")
                    break
            
            if unspsc_code or cat_code:
                classifications.append((unspsc_code, unspsc_desc, cat_code, cat_desc))
        
        return classifications


class DataAggregator:
    """
    Aggregates bid records by UNSPSC category.
    """
    
    def __init__(self):
        self.summaries: dict[str, UNSPSCSummary] = {}
        self.records: list[BidRecord] = []
        self.total_files_processed = 0
        self.total_releases_processed = 0
    
    def add_record(self, record: BidRecord) -> None:
        """Add a bid record to the aggregation."""
        self.records.append(record)
        self.total_releases_processed += 1
        
        # Aggregate by UNSPSC code
        key = record.unspsc_code or "UNCLASSIFIED"
        
        if key not in self.summaries:
            self.summaries[key] = UNSPSCSummary(
                unspsc_code=key,
                unspsc_description=record.unspsc_description or "Unclassified",
                category_code=record.category_code,
                category_description=record.category_description,
            )
        
        summary = self.summaries[key]
        summary.total_tenders += 1
        
        if record.bid_value is not None and record.bid_value > 0:
            summary.add_bid(record.bid_value)
        
        if record.award_value is not None and record.award_value > 0:
            summary.add_award(record.award_value)
    
    def get_summaries_sorted(self, by: str = "total_award_value") -> list[UNSPSCSummary]:
        """
        Get summaries sorted by specified field.
        
        Args:
            by: Field to sort by (total_award_value, total_tenders, unspsc_code)
        """
        summaries = list(self.summaries.values())
        
        if by == "total_award_value":
            return sorted(summaries, key=lambda s: s.total_award_value, reverse=True)
        elif by == "total_tenders":
            return sorted(summaries, key=lambda s: s.total_tenders, reverse=True)
        elif by == "unspsc_code":
            return sorted(summaries, key=lambda s: s.unspsc_code)
        else:
            return summaries


class AnalysisExporter:
    """
    Exports analysis results to various formats.
    """
    
    @staticmethod
    def to_csv(
        records: list[BidRecord],
        output_path: Path,
        include_header: bool = True,
    ) -> None:
        """Export raw bid records to CSV."""
        fieldnames = [
            "ocid", "release_id", "release_date",
            "tender_id", "tender_title", "tender_status", "procurement_category",
            "unspsc_code", "unspsc_description",
            "category_code", "category_description",
            "buyer_id", "buyer_name",
            "bid_value", "award_value", "currency",
            "supplier_id", "supplier_name",
        ]
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if include_header:
                writer.writeheader()
            
            for record in records:
                writer.writerow({
                    "ocid": record.ocid,
                    "release_id": record.release_id,
                    "release_date": record.release_date,
                    "tender_id": record.tender_id,
                    "tender_title": record.tender_title,
                    "tender_status": record.tender_status,
                    "procurement_category": record.procurement_category,
                    "unspsc_code": record.unspsc_code,
                    "unspsc_description": record.unspsc_description,
                    "category_code": record.category_code,
                    "category_description": record.category_description,
                    "buyer_id": record.buyer_id,
                    "buyer_name": record.buyer_name,
                    "bid_value": record.bid_value,
                    "award_value": record.award_value,
                    "currency": record.currency,
                    "supplier_id": record.supplier_id,
                    "supplier_name": record.supplier_name,
                })
        
        logger.info(f"Exported {len(records)} records to {output_path}")
    
    @staticmethod
    def summaries_to_csv(
        summaries: list[UNSPSCSummary],
        output_path: Path,
    ) -> None:
        """Export UNSPSC summaries to CSV."""
        fieldnames = [
            "unspsc_code", "unspsc_description",
            "category_code", "category_description",
            "total_tenders", "total_bids", "total_awards",
            "total_bid_value", "avg_bid_value", "min_bid_value", "max_bid_value",
            "total_award_value", "avg_award_value", "min_award_value", "max_award_value",
        ]
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for s in summaries:
                writer.writerow({
                    "unspsc_code": s.unspsc_code,
                    "unspsc_description": s.unspsc_description,
                    "category_code": s.category_code,
                    "category_description": s.category_description,
                    "total_tenders": s.total_tenders,
                    "total_bids": s.total_bids,
                    "total_awards": s.total_awards,
                    "total_bid_value": round(s.total_bid_value, 2),
                    "avg_bid_value": round(s.avg_bid_value, 2) if s.avg_bid_value else None,
                    "min_bid_value": round(s.min_bid_value, 2) if s.min_bid_value else None,
                    "max_bid_value": round(s.max_bid_value, 2) if s.max_bid_value else None,
                    "total_award_value": round(s.total_award_value, 2),
                    "avg_award_value": round(s.avg_award_value, 2) if s.avg_award_value else None,
                    "min_award_value": round(s.min_award_value, 2) if s.min_award_value else None,
                    "max_award_value": round(s.max_award_value, 2) if s.max_award_value else None,
                })
        
        logger.info(f"Exported {len(summaries)} UNSPSC summaries to {output_path}")
    
    @staticmethod
    def summaries_to_json(
        summaries: list[UNSPSCSummary],
        output_path: Path,
        metadata: Optional[dict] = None,
    ) -> None:
        """Export UNSPSC summaries to JSON."""
        data = {
            "generated_at": datetime.now().isoformat(),
            "metadata": metadata or {},
            "summaries": [
                {
                    "unspsc_code": s.unspsc_code,
                    "unspsc_description": s.unspsc_description,
                    "category_code": s.category_code,
                    "category_description": s.category_description,
                    "statistics": {
                        "total_tenders": s.total_tenders,
                        "total_bids": s.total_bids,
                        "total_awards": s.total_awards,
                    },
                    "bid_values": {
                        "total": round(s.total_bid_value, 2),
                        "average": round(s.avg_bid_value, 2) if s.avg_bid_value else None,
                        "min": round(s.min_bid_value, 2) if s.min_bid_value else None,
                        "max": round(s.max_bid_value, 2) if s.max_bid_value else None,
                    },
                    "award_values": {
                        "total": round(s.total_award_value, 2),
                        "average": round(s.avg_award_value, 2) if s.avg_award_value else None,
                        "min": round(s.min_award_value, 2) if s.min_award_value else None,
                        "max": round(s.max_award_value, 2) if s.max_award_value else None,
                    },
                }
                for s in summaries
            ],
        }
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(summaries)} UNSPSC summaries to {output_path}")


def analyze_data_directory(
    data_dir: Path,
    output_dir: Optional[Path] = None,
    export_records: bool = False,
) -> DataAggregator:
    """
    Analyze all JSON files in a directory.
    
    Args:
        data_dir: Directory containing downloaded JSON files.
        output_dir: Directory for output files (default: data_dir).
        export_records: Whether to export individual records to CSV.
    
    Returns:
        DataAggregator with all processed data.
    """
    if output_dir is None:
        output_dir = data_dir
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    parser = OCDSParser()
    aggregator = DataAggregator()
    
    # Find all JSON files (excluding manifest)
    json_files = sorted(data_dir.glob("*.json"))
    json_files = [f for f in json_files if f.name != "manifest.json"]
    
    logger.info(f"Found {len(json_files)} JSON files to process")
    
    for i, file_path in enumerate(json_files, 1):
        logger.info(f"Processing [{i}/{len(json_files)}]: {file_path.name}")
        
        for record in parser.parse_file(file_path):
            aggregator.add_record(record)
        
        aggregator.total_files_processed += 1
    
    # Export results
    summaries = aggregator.get_summaries_sorted(by="total_award_value")
    
    # Export summaries
    AnalysisExporter.summaries_to_csv(summaries, output_dir / "unspsc_summary.csv")
    AnalysisExporter.summaries_to_json(
        summaries,
        output_dir / "unspsc_summary.json",
        metadata={
            "files_processed": aggregator.total_files_processed,
            "total_releases": aggregator.total_releases_processed,
            "unique_unspsc_codes": len(summaries),
        },
    )
    
    # Optionally export raw records
    if export_records:
        AnalysisExporter.to_csv(aggregator.records, output_dir / "all_bids.csv")
    
    return aggregator
