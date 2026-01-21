"""
Silver Layer Extractor - Complete flattened extraction of SEAO OCDS data.

Transforms bronze layer (raw JSON) into silver layer (clean, flat CSV)
with ALL fields preserved for downstream analysis.

The silver layer contains:
- One row per release (contract/tender)
- All nested objects flattened with dot notation
- Multiple items/bids/awards expanded or aggregated
- UTF-8 with BOM for Excel compatibility
"""

import csv
import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

logger = logging.getLogger(__name__)

# UTF-8 BOM for Excel compatibility
UTF8_BOM = '\ufeff'


@dataclass
class SilverRecord:
    """
    Complete flattened record from OCDS release.
    
    This represents the "silver layer" - clean, typed, tabular data
    ready for analysis and transformation into gold layer.
    """
    
    # === Release Metadata ===
    ocid: str = ""
    release_id: str = ""
    release_date: str = ""
    language: str = ""
    tags: str = ""  # comma-separated
    initiation_type: str = ""
    
    # === Buyer Information ===
    buyer_id: str = ""
    buyer_name: str = ""
    buyer_street: str = ""
    buyer_city: str = ""
    buyer_region: str = ""
    buyer_country: str = ""
    buyer_postal_code: str = ""
    buyer_is_municipal: str = ""
    
    # === Tender Information ===
    tender_id: str = ""
    tender_title: str = ""
    tender_description: str = ""
    tender_status: str = ""
    tender_delivery_area: str = ""
    procurement_method: str = ""
    procurement_method_details: str = ""
    main_procurement_category: str = ""
    additional_procurement_categories: str = ""  # comma-separated
    
    # === Tender Period ===
    tender_start_date: str = ""
    tender_end_date: str = ""
    tender_duration_days: Optional[int] = None
    
    # === Tender Statistics ===
    number_of_tenderers: Optional[int] = None
    tenderers_ids: str = ""  # comma-separated
    tenderers_names: str = ""  # comma-separated
    
    # === Classification (UNSPSC) ===
    # Primary item classification
    item_count: int = 0
    primary_unspsc_code: str = ""
    primary_unspsc_description: str = ""
    all_unspsc_codes: str = ""  # comma-separated
    
    # Quebec category
    primary_category_code: str = ""
    primary_category_description: str = ""
    all_category_codes: str = ""  # comma-separated
    
    # === Bids ===
    bid_count: int = 0
    total_bid_value: Optional[float] = None
    min_bid_value: Optional[float] = None
    max_bid_value: Optional[float] = None
    all_bidder_ids: str = ""  # comma-separated
    all_bid_values: str = ""  # comma-separated
    
    # === Award Information ===
    award_id: str = ""
    award_status: str = ""
    award_date: str = ""
    award_amount: Optional[float] = None
    award_currency: str = "CAD"
    
    # === Supplier (Winner) ===
    supplier_id: str = ""
    supplier_name: str = ""
    supplier_street: str = ""
    supplier_city: str = ""
    supplier_region: str = ""
    supplier_country: str = ""
    supplier_postal_code: str = ""
    supplier_neq: str = ""  # Quebec business number
    
    # === Contract Information ===
    contract_id: str = ""
    contract_status: str = ""
    contract_date_signed: str = ""
    contract_amount: Optional[float] = None
    contract_currency: str = "CAD"
    contract_end_date: str = ""
    
    # === Document Reference ===
    document_url: str = ""
    
    # === Source Metadata ===
    source_file: str = ""
    extraction_timestamp: str = ""


class SilverLayerExtractor:
    """
    Extracts and flattens OCDS JSON data into silver layer format.
    """
    
    def __init__(self):
        self.records: list[SilverRecord] = []
        self.files_processed = 0
        self.releases_processed = 0
        self.extraction_time = datetime.now().isoformat()
    
    def extract_file(self, file_path: Path) -> int:
        """
        Extract all releases from a single JSON file.
        
        Returns:
            Number of records extracted.
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to parse {file_path}: {e}")
            return 0
        
        releases = data.get("releases", [])
        count = 0
        
        for release in releases:
            record = self._extract_release(release, file_path.name)
            self.records.append(record)
            count += 1
        
        self.files_processed += 1
        self.releases_processed += count
        logger.debug(f"Extracted {count} records from {file_path.name}")
        
        return count
    
    def _extract_release(self, release: dict, source_file: str) -> SilverRecord:
        """Extract a single release into a SilverRecord."""
        record = SilverRecord()
        
        # === Release Metadata ===
        record.ocid = release.get("ocid", "")
        record.release_id = str(release.get("id", ""))
        record.release_date = release.get("date", "")
        record.language = release.get("language", "")
        record.tags = ",".join(release.get("tag", []))
        record.initiation_type = release.get("initiationType", "")
        
        # === Buyer ===
        buyer = release.get("buyer", {})
        record.buyer_id = buyer.get("id", "")
        record.buyer_name = buyer.get("name", "")
        
        # Get buyer details from parties
        parties = release.get("parties", [])
        buyer_party = self._find_party(parties, buyer.get("id"), "buyer")
        if buyer_party:
            addr = buyer_party.get("address", {})
            record.buyer_street = addr.get("streetAddress", "")
            record.buyer_city = addr.get("locality", "")
            record.buyer_region = addr.get("region", "")
            record.buyer_country = addr.get("countryName", "")
            record.buyer_postal_code = addr.get("postalCode", "")
            details = buyer_party.get("details", {})
            record.buyer_is_municipal = details.get("Municipal", "")
        
        # === Tender ===
        tender = release.get("tender", {})
        record.tender_id = str(tender.get("id", ""))
        record.tender_title = tender.get("title", "")
        record.tender_description = tender.get("description", "")
        record.tender_status = tender.get("status", "")
        record.tender_delivery_area = tender.get("deliveryarea", "")
        record.procurement_method = tender.get("procurementMethod", "")
        record.procurement_method_details = tender.get("procurementMethodDetails", "")
        record.main_procurement_category = tender.get("mainProcurementCategory", "")
        record.additional_procurement_categories = ",".join(
            tender.get("additionalProcurementCategories", [])
        )
        
        # Tender Period
        period = tender.get("tenderPeriod", {})
        record.tender_start_date = period.get("startDate", "")
        record.tender_end_date = period.get("endDate", "")
        record.tender_duration_days = period.get("durationInDays")
        
        # Tenderers
        record.number_of_tenderers = tender.get("numberOfTenderers")
        tenderers = tender.get("tenderers", [])
        record.tenderers_ids = ",".join(t.get("id", "") for t in tenderers)
        record.tenderers_names = "|".join(t.get("name", "") for t in tenderers)
        
        # === Items & Classifications ===
        items = tender.get("items", [])
        record.item_count = len(items)
        
        unspsc_codes = []
        unspsc_descs = []
        cat_codes = []
        cat_descs = []
        
        for item in items:
            classification = item.get("classification", {})
            if classification.get("scheme") == "UNSPSC":
                code = classification.get("id", "")
                desc = classification.get("description", "")
                if code:
                    unspsc_codes.append(code)
                    unspsc_descs.append(desc)
            
            for add_class in item.get("additionalClassifications", []):
                if add_class.get("scheme") == "CATEGORY":
                    cat_code = add_class.get("id", "")
                    cat_desc = add_class.get("description", "")
                    if cat_code:
                        cat_codes.append(cat_code)
                        cat_descs.append(cat_desc)
        
        if unspsc_codes:
            record.primary_unspsc_code = unspsc_codes[0]
            record.primary_unspsc_description = unspsc_descs[0] if unspsc_descs else ""
            record.all_unspsc_codes = ",".join(unspsc_codes)
        
        if cat_codes:
            record.primary_category_code = cat_codes[0]
            record.primary_category_description = cat_descs[0] if cat_descs else ""
            record.all_category_codes = ",".join(cat_codes)
        
        # === Bids ===
        bids = release.get("bids", [])
        record.bid_count = len(bids)
        
        bid_values = []
        bidder_ids = []
        
        for bid in bids:
            value = bid.get("value")
            if value is not None:
                bid_values.append(value)
            bidder_id = bid.get("id", "")
            if bidder_id:
                bidder_ids.append(bidder_id)
        
        if bid_values:
            record.total_bid_value = sum(bid_values)
            record.min_bid_value = min(bid_values)
            record.max_bid_value = max(bid_values)
            record.all_bid_values = ",".join(str(v) for v in bid_values)
        
        record.all_bidder_ids = ",".join(bidder_ids)
        
        # === Awards ===
        awards = release.get("awards", [])
        active_award = None
        for award in awards:
            if award.get("status") == "active":
                active_award = award
                break
        if not active_award and awards:
            active_award = awards[0]
        
        if active_award:
            record.award_id = str(active_award.get("id", ""))
            record.award_status = active_award.get("status", "")
            record.award_date = active_award.get("date", "")
            
            value_obj = active_award.get("value", {})
            if isinstance(value_obj, dict):
                record.award_amount = value_obj.get("amount")
                record.award_currency = value_obj.get("currency", "CAD")
            
            suppliers = active_award.get("suppliers", [])
            if suppliers:
                supplier = suppliers[0]
                record.supplier_id = supplier.get("id", "")
                record.supplier_name = supplier.get("name", "")
                
                # Get supplier details from parties
                supplier_party = self._find_party(parties, supplier.get("id"), "supplier")
                if supplier_party:
                    addr = supplier_party.get("address", {})
                    record.supplier_street = addr.get("streetAddress", "")
                    record.supplier_city = addr.get("locality", "")
                    record.supplier_region = addr.get("region", "")
                    record.supplier_country = addr.get("countryName", "")
                    record.supplier_postal_code = addr.get("postalCode", "")
                    details = supplier_party.get("details", {})
                    record.supplier_neq = details.get("NEQ", "")
        
        # === Contracts ===
        contracts = release.get("contracts", [])
        if contracts:
            contract = contracts[0]
            record.contract_id = str(contract.get("id", ""))
            record.contract_status = contract.get("status", "")
            record.contract_date_signed = contract.get("dateSigned", "")
            
            value_obj = contract.get("value", {})
            if isinstance(value_obj, dict):
                record.contract_amount = value_obj.get("amount")
                record.contract_currency = value_obj.get("currency", "CAD")
            
            period = contract.get("period", {})
            record.contract_end_date = period.get("endDate", "")
        
        # === Documents ===
        documents = tender.get("documents", [])
        if documents:
            record.document_url = documents[0].get("url", "")
        
        # === Source Metadata ===
        record.source_file = source_file
        record.extraction_timestamp = self.extraction_time
        
        return record
    
    def _find_party(self, parties: list, party_id: str, role: str) -> Optional[dict]:
        """Find a party by ID or role."""
        if not party_id:
            return None
        
        for party in parties:
            if party.get("id") == party_id:
                return party
            if role in party.get("roles", []):
                return party
        
        return None
    
    def export_csv(self, output_path: Path) -> None:
        """
        Export all records to CSV with UTF-8 BOM for Excel.
        """
        if not self.records:
            logger.warning("No records to export")
            return
        
        # Get field names from dataclass
        fieldnames = list(SilverRecord.__dataclass_fields__.keys())
        
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            # Write BOM for Excel UTF-8 recognition
            f.write(UTF8_BOM)
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for record in self.records:
                writer.writerow(asdict(record))
        
        logger.info(f"Exported {len(self.records)} records to {output_path}")
    
    def export_parquet(self, output_path: Path) -> None:
        """
        Export to Parquet format (if pyarrow is available).
        Parquet is better for large datasets and preserves types.
        """
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Convert to dict of lists
            data = {field: [] for field in SilverRecord.__dataclass_fields__.keys()}
            for record in self.records:
                for field, value in asdict(record).items():
                    data[field].append(value)
            
            table = pa.Table.from_pydict(data)
            pq.write_table(table, output_path)
            logger.info(f"Exported {len(self.records)} records to {output_path}")
            
        except ImportError:
            logger.warning("pyarrow not installed. Install with: pip install pyarrow")
            raise


def extract_silver_layer(
    data_dir: Path,
    output_path: Optional[Path] = None,
    format: str = "csv",
) -> SilverLayerExtractor:
    """
    Extract silver layer from all JSON files in a directory.
    
    Args:
        data_dir: Directory containing bronze layer JSON files.
        output_path: Output file path (default: data_dir/silver_layer.csv).
        format: Output format ('csv' or 'parquet').
    
    Returns:
        SilverLayerExtractor with all processed data.
    """
    if output_path is None:
        ext = ".parquet" if format == "parquet" else ".csv"
        output_path = data_dir / f"silver_layer{ext}"
    
    extractor = SilverLayerExtractor()
    
    # Find all JSON files (excluding manifest and analysis outputs)
    json_files = sorted(data_dir.glob("*.json"))
    json_files = [
        f for f in json_files 
        if f.name not in ("manifest.json", "unspsc_summary.json")
    ]
    
    logger.info(f"Found {len(json_files)} JSON files to process")
    
    for i, file_path in enumerate(json_files, 1):
        logger.info(f"Processing [{i}/{len(json_files)}]: {file_path.name}")
        extractor.extract_file(file_path)
    
    # Export
    if format == "parquet":
        extractor.export_parquet(output_path)
    else:
        extractor.export_csv(output_path)
    
    return extractor
