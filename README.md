# Extract SEAO Data

A production-ready Python tool for downloading JSON resources from SEAO (Système électronique d'appel d'offres du Québec) via the Québec Open Data Portal.

## Overview

This project provides a reliable, polite downloader for extracting public procurement data from SEAO. It uses the **CKAN API** (the underlying platform for donneesquebec.ca) for robust resource discovery rather than fragile HTML scraping.

### Features

- **CKAN API integration** — Reliable resource discovery via official API
- **Rate limiting** — Configurable requests/second to respect server limits
- **Exponential backoff** — Automatic retries on transient failures
- **Resume support** — Skip already-downloaded files based on manifest
- **JSON validation** — Post-download verification of data integrity
- **Machine-readable manifest** — Full audit trail with timestamps and status
- **Dry-run mode** — Preview what will be downloaded without downloading
- **Parallel downloads** — Optional concurrency (use conservatively)
- **Silver layer extraction** — Transform raw JSON to clean, flat CSV/Parquet

## Project Structure

```
seao_downloader/
├── __init__.py        # Package marker
├── discovery.py       # CKAN API client for resource discovery
├── downloader.py      # HTTP client with retries and rate limiting
├── persistence.py     # Filesystem and manifest management
├── main.py            # Download CLI entrypoint and orchestration
├── silver_layer.py    # Silver layer extraction logic (JSON → CSV/Parquet)
└── extract_silver.py  # Silver layer extraction CLI
```

## Prerequisites

- **Python 3.11+** (uses modern typing features and walrus operator)
- **No external dependencies** — uses Python standard library only
- **Optional: pyarrow** — for Parquet output format

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/extract-seao-data.git
cd extract-seao-data

# (Optional) Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Linux/macOS
# or: venv\Scripts\activate  # Windows

# (Optional) Install pyarrow for Parquet support
pip install pyarrow
```

## Usage

### Basic Download

Download all JSON resources to the default directory (`./seao_data`):

```bash
python -m seao_downloader.main
```

### Custom Output Directory

```bash
python -m seao_downloader.main --out-dir ./my_data
```

### Dry Run (Preview)

List resources that would be downloaded without actually downloading:

```bash
python -m seao_downloader.main --dry-run
```

### Resume Interrupted Download

Skip files that were successfully downloaded in a previous run:

```bash
python -m seao_downloader.main --resume
```

### Slower Rate Limit

Be more polite to the server (0.5 requests per second):

```bash
python -m seao_downloader.main --rate-limit 0.5
```

### Parallel Downloads

Use multiple workers (use with caution — may trigger rate limiting):

```bash
python -m seao_downloader.main --max-workers 4 --rate-limit 2.0
```

### List All Resources

Show all resources in the dataset (not just JSON):

```bash
python -m seao_downloader.main --list-all
```

### Verbose Logging

Enable debug output:

```bash
python -m seao_downloader.main -v
```

### SSL Certificate Issues

If you encounter SSL certificate verification errors (common on macOS), you can either:

1. **Fix the root cause** (recommended): Run the "Install Certificates.command" script from your Python installation folder
2. **Bypass verification** (for testing only):

```bash
python -m seao_downloader.main --no-verify-ssl
```

### Full Download Options

```
usage: seao-downloader [-h] [--out-dir OUT_DIR] [--max-workers MAX_WORKERS]
                       [--rate-limit RATE_LIMIT] [--resume] [--dry-run]
                       [--verbose] [--dataset-id DATASET_ID] [--list-all]
                       [--no-verify-ssl]

Download JSON resources from the Québec SEAO open data portal.

options:
  -h, --help            show this help message and exit
  --out-dir OUT_DIR     Output directory for downloads (default: ./seao_data)
  --max-workers N       Maximum parallel downloads (default: 2)
  --rate-limit RATE     Maximum requests per second (default: 1.0)
  --resume              Skip resources already successfully downloaded
  --dry-run             Discover resources without downloading
  --verbose, -v         Enable verbose (debug) logging
  --dataset-id ID       CKAN dataset ID (default: systeme-electronique-dappel-doffres-seao)
  --list-all            List all resources (not just JSON) and exit
  --no-verify-ssl       Disable SSL verification (not recommended for production)
```

## Output

### Downloaded Files

Files are saved with deterministic names to avoid collisions:

```
{resource_id_prefix}_{sanitized_name}.json
```

Example: `a1b2c3d4_Avis_publies_2024.json`

### Manifest

A `manifest.json` file is created in the output directory containing:

```json
{
  "version": "1.0",
  "dataset_id": "systeme-electronique-dappel-doffres-seao",
  "created_at": "2025-01-20T12:00:00+00:00",
  "last_updated": "2025-01-20T12:05:30+00:00",
  "entries": [
    {
      "resource_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "resource_name": "Avis publiés 2024",
      "source_url": "https://example.com/data.json",
      "local_path": "./seao_data/a1b2c3d4_Avis_publies_2024.json",
      "download_timestamp": "2025-01-20T12:01:15+00:00",
      "http_status": 200,
      "file_size_bytes": 1234567,
      "is_valid_json": true,
      "error_message": null,
      "retry_count": 0
    }
  ]
}
```

### Summary Output

After completion, a human-readable summary is printed:

```
==================================================
DOWNLOAD SUMMARY
==================================================
  Succeeded:    15
  Failed:       0
  Skipped:      0
  Total bytes:  45,678,901 (43.6 MB)
  Manifest:     ./seao_data/manifest.json
==================================================
```

---

## Silver Layer Extraction

After downloading, you can transform the raw JSON (bronze layer) into a clean, flattened CSV or Parquet file (silver layer) for analysis.

### Extract Silver Layer

```bash
python -m seao_downloader.extract_silver --data-dir ./data
```

### Extraction Options

```bash
# Extract to CSV (default)
python -m seao_downloader.extract_silver --data-dir ./data

# Extract to Parquet (requires pyarrow)
python -m seao_downloader.extract_silver --data-dir ./data --format parquet

# Custom output path
python -m seao_downloader.extract_silver --data-dir ./data --output ./silver.csv

# Verbose logging
python -m seao_downloader.extract_silver --data-dir ./data -v
```

### Full Extraction Options

```
usage: seao-extract-silver [-h] --data-dir DATA_DIR [--output OUTPUT]
                           [--format {csv,parquet}] [--verbose]

Extract silver layer (complete flattened CSV/Parquet) from bronze layer (raw JSON).

options:
  -h, --help            show this help message and exit
  --data-dir DATA_DIR   Directory containing bronze layer JSON files.
  --output OUTPUT       Output file path (default: data-dir/silver_layer.csv).
  --format {csv,parquet}
                        Output format (default: csv). Parquet requires pyarrow.
  --verbose, -v         Enable verbose logging.
```

### Silver Layer Output

The extraction produces one row per release (contract/tender) with all nested OCDS data flattened into columns:

| Column Group | Description | Columns |
|--------------|-------------|---------|
| Release metadata | OCID, release ID, date, language, tags | 6 |
| Buyer info | Buyer ID, name, address details | 8 |
| Tender info | Title, status, procurement method, dates | 14 |
| Classifications | UNSPSC codes, Quebec category codes | 8 |
| Bids | Bid counts, values (total/min/max) | 6 |
| Awards | Award ID, status, date, amount | 6 |
| Supplier info | Supplier ID, name, address, NEQ | 8 |
| Contract info | Contract ID, status, amount, dates | 6 |
| Documents | Document URL | 1 |
| Source metadata | Source file, extraction timestamp | 2 |

**Note:** CSV files include a UTF-8 BOM for proper French character display in Excel.

---

## Architecture

The project follows **separation of concerns** with 6 focused modules:

| Module | Responsibility |
|--------|----------------|
| `discovery.py` | CKAN API communication and resource filtering |
| `downloader.py` | HTTP requests, rate limiting, retries |
| `persistence.py` | File naming, paths, manifest management |
| `silver_layer.py` | OCDS JSON parsing and flattening to tabular format |
| `main.py` | Download CLI orchestration |
| `extract_silver.py` | Silver layer extraction CLI |

### Design Principles

- **Occam's Razor** — Minimal complexity; classes only where they add clarity
- **Composition over inheritance** — Components are composed, not subclassed
- **Fail-safe** — Stops on 403/429 with actionable guidance
- **Idempotent** — Re-running with `--resume` is safe and efficient

## Error Handling

### 403 Forbidden

The script stops immediately and prints:

```
ACCESS DENIED (403). The server is blocking requests. Possible causes:
  - IP rate limiting
  - Authentication required
  - Geographic restrictions
ACTION: Wait and retry later, or check if auth is needed.
```

### 429 Too Many Requests

The script stops immediately and prints:

```
RATE LIMITED (429). Too many requests. The server is throttling.
ACTION: Reduce --rate-limit and wait before retrying.
```

### Transient Errors (5xx, Timeouts)

Automatically retried with exponential backoff (2s → 4s → 8s).

## Assumptions

1. **CKAN API availability** — The portal uses CKAN and exposes the standard `package_show` endpoint.
2. **Public access** — The SEAO dataset is publicly accessible without authentication.
3. **JSON format declaration** — Resources are correctly labeled with `format: "JSON"` in CKAN metadata.

## Limitations

1. **No authentication** — If the portal adds authentication, you'll need to modify the `User-Agent` header or add API key support in `discovery.py`.
2. **Memory for validation** — JSON validation loads files into memory. Very large files (>1GB) may cause issues.
3. **Portal changes** — If donneesquebec.ca changes their API structure, the discovery module may need updates.
4. **ZIP archives** — Some JSON data may be distributed as ZIP files. These are currently excluded but could be handled by adding `zipfile` extraction.

## Alternative Approaches Considered

| Approach | Verdict |
|----------|---------|
| **HTML Scraping (BeautifulSoup)** | Rejected — fragile, breaks on UI changes |
| **Playwright/Selenium** | Rejected — overkill for a site with a clean API |
| **CKAN API (chosen)** | Best — official, structured, stable |

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
