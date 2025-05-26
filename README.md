# SEC EDGAR Bulk Data ETL Scripts

## 1. Overview

This repository contains Python ETL (Extract, Transform, Load) scripts designed to process bulk data downloaded from the U.S. Securities and Exchange Commission's (SEC) EDGAR system. The primary goal is to extract valuable company and financial information from the SEC's JSON-formatted bulk datasets (`submissions.zip` and `companyfacts.zip`) and transform it into analysis-friendly Apache Parquet files.

These scripts are optimized for handling large datasets by processing files directly from ZIP archives in batches, thus minimizing disk I/O for temporary files and managing memory efficiently. The output Parquet files are structured to be compatible with data analysis tools like Pandas and distributed processing frameworks like Apache Spark.

## 2. Background & Developer Notes

*   **Genesis:** These scripts were developed iteratively to address the need for structured, queryable data from SEC EDGAR bulk downloads. They are intended to provide a robust foundation for financial data extraction and preparation.
*   **Data Source:**
    *   `submissions.zip`: Contains metadata for all entities (filers) and a history of their filings. (Download: `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip`)
    *   `companyfacts.zip`: Contains extracted XBRL data (standardized financial facts) for companies. (Download: `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip`)
*   **Key Challenges Addressed During Development:**
    *   **Large Data Handling:** Processing multi-gigabyte ZIP archives containing millions of JSON files without full intermediate extraction.
    *   **Memory Management:** Implementing batch processing to prevent out-of-memory errors.
    *   **JSON Parsing:** Efficiently parsing complex nested JSON structures from SEC data.
    *   **Data Type Compatibility:** Ensuring numeric and nullable types (e.g., `value`, `fiscal_year`) are handled correctly for robust Parquet file creation, specifically addressing compatibility with Apache Spark's schema interpretation (e.g., `BIGINT` vs. `DOUBLE` for nullable integers). Solutions involved using Pandas nullable dtypes (`pd.Int64Dtype()`) and careful type casting.
    *   **Specific Data Extraction Needs:** Tailoring parsing logic to extract precise fields and structures as required for downstream analysis (e.g., company master data, ticker-exchange mapping, aggregated filing-level facts, and granular time-series financial metrics).
*   **Iterative Process:** The scripts evolved through several iterations, refining parsing logic, error handling, and output schemas based on debugging and specific requirements for data analysis and tool compatibility. This history is important for understanding the design choices made.

## 3. Scripts Overview

### 3.1. `etl_00_submissions_company_master_data.py`

*   **Purpose:** Extracts company master information from the `submissions.zip` archive. This script focuses on creating a clean, foundational dataset of company identifiers and basic attributes.
*   **Input:** `submissions.zip` (containing individual `{CIK}.json` files).
*   **Core Logic:**
    *   Reads each `{CIK}.json` file directly from the ZIP archive.
    *   Parses company-level metadata: CIK (Central Index Key), official company name, SIC (Standard Industrial Classification) code, and SIC description.
    *   Extracts stock ticker symbols and their corresponding stock exchanges. If a company has multiple tickers and exchanges listed in parallel arrays, the script "explodes" these into separate rows, creating a distinct record for each ticker-exchange pair associated with the company.
    *   Intentionally skips the detailed "filings" history section within each company's JSON file to keep the output focused on master data.
*   **Output:** A set of Parquet files, processed and saved in batches. Each file contains a segment of the company master data.
    *   **Schema Highlights:**
        *   `cik` (string): Unique 10-digit company identifier.
        *   `company_name` (string): The official registered name of the company.
        *   `sic` (string): Four-digit Standard Industrial Classification code.
        *   `sic_description` (string): Textual description of the SIC code.
        *   `ticker` (string): Stock market ticker symbol (can be null if not applicable).
        *   `exchange` (string): Stock exchange where the ticker is listed (can be null).

### 3.2. `etl_01_company_filings_form_10k_10q.py`

*   **Purpose:** Extracts detailed financial facts from 10-K (annual) and 10-Q (quarterly) filings, as reported in the `companyfacts.zip` archive. This archive contains XBRL data pre-processed by the SEC.
*   **Input:** `companyfacts.zip` (containing individual `{CIK}.json` files).
*   **Core Logic:**
    *   Reads each `{CIK}.json` file from the ZIP archive.
    *   Navigates the nested JSON structure to access individual XBRL facts (e.g., `us-gaap:Assets`, `us-gaap:Revenues`).
    *   Specifically filters for facts that originate from '10-K' or '10-Q' forms.
    *   Offers two distinct types of output structures, controlled by global Python flags (`CREATE_FILING_LEVEL_OUTPUT`, `CREATE_TIME_SERIES_OUTPUT`) at the beginning of the script.
*   **Outputs (controlled by flags):**
    *   **Filing-Level Aggregated Facts (if `CREATE_FILING_LEVEL_OUTPUT = True`):**
        *   Generates Parquet files where each row represents a single, unique company filing (e.g., Apple Inc.'s 10-K for fiscal year 2023).
        *   This output is useful for analyses that require all reported facts for a specific filing to be grouped together.
        *   **Schema Highlights:**
            *   `cik` (string): Central Index Key.
            *   `entity_name` (string): Company name.
            *   `accession_number` (string): Unique SEC identifier for the specific filing.
            *   `form_type` (string): The type of SEC form (e.g., '10-K', '10-Q').
            *   `filed_date` (string): Date the filing was submitted to the SEC (YYYY-MM-DD).
            *   `fiscal_year` (int/string/pd.NA): The fiscal year covered by the report, as identified in the filing's context.
            *   `fiscal_period` (string): The fiscal period covered (e.g., 'FY' for full year, 'Q1', 'Q2', 'Q3').
            *   `all_facts_json_array` (string): A JSON string. This string itself is an array containing all individual XBRL fact dictionaries (value, units, period end date, etc.) reported within this specific filing.

    *   **Time-Series Individual Metrics (if `CREATE_TIME_SERIES_OUTPUT = True`):**
        *   Generates Parquet files where each row represents a single reported financial fact instance (e.g., one row for Apple's "Revenues" in Q1 2023, another for "NetIncomeLoss" in Q1 2023, etc.).
        *   This granular output is ideal for time-series analysis of specific financial metrics across companies and periods.
        *   **Schema Highlights:**
            *   `cik` (string): Central Index Key.
            *   `entity_name` (string): Company name.
            *   `taxonomy` (string): The XBRL taxonomy the fact belongs to (e.g., 'us-gaap', 'ifrs-full').
            *   `concept` (string): The specific XBRL concept name (e.g., 'Assets', 'Revenues', 'NetIncomeLoss').
            *   `label` (string): Human-readable label for the XBRL concept.
            *   `unit` (string): The unit of measurement for the fact (e.g., 'USD', 'shares').
            *   `period_end_date` (string): The end date of the period for which the fact is reported (YYYY-MM-DD).
            *   `value` (float64): The numerical value of the reported fact (can be NaN if not applicable or unconvertible).
            *   `accession_number` (string): Accession number of the SEC filing from which this fact was extracted.
            *   `fiscal_year` (Int64/pd.NA): The fiscal year related to this fact (uses Pandas nullable integer type for Spark compatibility).
            *   `fiscal_period` (string): The fiscal period related to this fact.
            *   `form_type` (string): The SEC form type where this fact was reported ('10-K' or '10-Q').
            *   `filed_date` (string): The date the SEC filing was submitted.
            *   `frame` (string, nullable): Additional XBRL context or "frame" information, if available (can be null).

## 4. Features

*   **Direct ZIP Processing:** Reads data directly from compressed `submissions.zip` and `companyfacts.zip` archives, minimizing disk I/O for temporary files and saving storage space.
*   **Batch Processing:** Designed to handle very large volumes of data by processing JSON files in manageable batches. This approach optimizes memory usage and allows the scripts to run on systems with moderate RAM.
*   **Apache Parquet Output:** Saves processed data in Apache Parquet format, a highly efficient columnar storage format ideal for analytical queries and use with big data tools.
*   **Spark Compatibility Focus:** Output schemas, particularly for numeric (e.g., `value`) and nullable integer (e.g., `fiscal_year`) types, have been refined to ensure smoother schema inference and data loading into Apache Spark.
*   **Configurable Outputs:** The `etl_01_company_filings_form_10k_10q.py` script allows users to select, via global boolean flags, whether to generate filing-level aggregated data, time-series individual metrics, or both.

## 5. Prerequisites

*   Python 3.7 or higher.
*   Required Python libraries:
    *   `pandas`: For data manipulation and DataFrame creation.
    *   `numpy`: For numerical operations, particularly `np.nan`.
    *   `pyarrow`: The engine used by Pandas for writing Parquet files.

## 6. Setup

1.  **Clone the Repository (if this is part of a Git repository):**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```
2.  **Install Dependencies:**
    It's recommended to use a virtual environment.
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    pip install pandas numpy pyarrow
    ```
    Alternatively, if a `requirements.txt` file is provided in the future:
    ```bash
    # pip install -r requirements.txt
    ```
3.  **Download SEC EDGAR Data:**
    *   **Submissions Data:** Download `submissions.zip` from the official SEC link: [submissions.zip](https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip).
    *   **Company Facts Data:** Download `companyfacts.zip` from the official SEC link: [companyfacts.zip](https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip).
    *   Place both downloaded ZIP files into your designated raw data directory. The scripts require these files to be present.

## 7. Configuration

Before running the scripts, you **must** update the path variables defined at the beginning of each Python file to point to your local file system locations.

*   **In `etl_00_submissions_company_master_data.py`:**
    *   `SUBMISSIONS_ZIP_PATH`: Set this to the absolute path of your downloaded `submissions.zip` file.
    *   `OUTPUT_DIR`: Specify the directory where the Parquet files containing company master data will be saved.
    *   `BATCH_SIZE` (in `if __name__ == "__main__":` block): This is hardcoded to `50000` in the provided script. Adjust if needed based on system memory (number of CIK JSON files to process per batch).

*   **In `etl_01_company_filings_form_10k_10q.py`:**
    *   `COMPANYFACTS_ZIP_PATH`: Set this to the absolute path of your downloaded `companyfacts.zip` file.
    *   `OUTPUT_DIR_FILING_LEVEL`: Specify the directory for filing-level aggregated Parquet files.
    *   `OUTPUT_DIR_TIME_SERIES`: Specify the directory for time-series metrics Parquet files.
    *   **Global Flags (at the top of the script):**
        *   `CREATE_FILING_LEVEL_OUTPUT = True` or `False` (Controls generation of filing-level data).
        *   `CREATE_TIME_SERIES_OUTPUT = True` or `False` (Controls generation of time-series data).
    *   **Batch Sizes (Configuration section):**
        *   `FILING_LEVEL_BATCH_SIZE`: Number of aggregated filing records to process per batch.
        *   `TIME_SERIES_BATCH_SIZE`: Number of individual time-series fact records to process per batch.
        *   Adjust these based on your system's available RAM. Larger batch sizes can improve I/O efficiency but require more memory.

## 8. Usage (Running the Scripts)

After configuring the paths and settings, execute the scripts using a Python interpreter from your terminal:

1.  **To process company master data from `submissions.zip`:**
    ```bash
    python etl_00_submissions_company_master_data.py
    ```
    This script will read `submissions.zip`, process the data in batches, and write Parquet files to the `OUTPUT_DIR` specified within the script.

2.  **To process 10-K/10-Q financial facts from `companyfacts.zip`:**
    ```bash
    python etl_01_company_filings_form_10k_10q.py
    ```
    This script will read `companyfacts.zip`. Based on the `CREATE_FILING_LEVEL_OUTPUT` and `CREATE_TIME_SERIES_OUTPUT` flags set inside the script, it will generate the corresponding Parquet files in their respective output directories.

## 9. Output Description (Parquet File Schemas)

The scripts generate Apache Parquet files, which are columnar and self-describing regarding schema. Below is a summary of the key fields.

### 9.1. Output from `etl_00_submissions_company_master_data.py`
Files named like `sec_company_master_data_batch_N.parquet`.

*   `cik` (string): Central Index Key, 10 digits with leading zeros. Example: "0000320193".
*   `company_name` (string): Official name of the company. Example: "Apple Inc."
*   `sic` (string, nullable): Standard Industrial Classification code. Example: "3571".
*   `sic_description` (string, nullable): Description of the SIC code. Example: "ELECTRONIC COMPUTERS".
*   `ticker` (string, nullable): Stock ticker symbol. Example: "AAPL".
*   `exchange` (string, nullable): Stock exchange for the ticker. Example: "Nasdaq".

### 9.2. Output from `etl_01_company_filings_form_10k_10q.py`

#### 9.2.1. Filing-Level Aggregated Facts
Files named like `company_form_data_filing_level_batch_N.parquet`.

*   `cik` (string): Central Index Key.
*   `entity_name` (string): Company name.
*   `accession_number` (string): Unique identifier for the filing. Example: "0000320193-23-000106".
*   `form_type` (string): e.g., '10-K', '10-Q'.
*   `filed_date` (string): Date the filing was submitted (YYYY-MM-DD). Example: "2023-10-30".
*   `fiscal_year` (int, nullable): Fiscal year of the report, derived from the filing's context. Example: 2023.
*   `fiscal_period` (string, nullable): Fiscal period (e.g., 'FY', 'Q1', 'Q2', 'Q3'). Example: "FY".
*   `all_facts_json_array` (string): A JSON string. This string represents a list of dictionaries, where each dictionary is an individual XBRL fact (containing 'val', 'unit', 'end', 'fp', 'fy', 'form', 'filed', 'accn' etc.) reported in this specific filing.

#### 9.2.2. Time-Series Individual Metrics
Files named like `company_form_data_time_series_batch_N.parquet`.

*   `cik` (string): Central Index Key.
*   `entity_name` (string): Company name.
*   `taxonomy` (string): XBRL taxonomy. Example: "us-gaap".
*   `concept` (string): XBRL concept name. Example: "Revenues".
*   `label` (string, nullable): Human-readable label for the XBRL concept. Example: "Revenues".
*   `unit` (string): Unit of measurement. Example: "USD".
*   `period_end_date` (string): End date of the period for the reported fact (YYYY-MM-DD). Example: "2023-09-30".
*   `value` (float64, nullable): The numerical value of the fact. Example: 383285000000.0. Can be NaN.
*   `accession_number` (string): Accession number of the filing the fact belongs to.
*   `fiscal_year` (Int64, nullable): Fiscal year of the fact (Pandas nullable integer for Spark compatibility). Example: 2023. Can be NA.
*   `fiscal_period` (string, nullable): Fiscal period of the fact. Example: "FY".
*   `form_type` (string): Form type of the filing ('10-K', '10-Q').
*   `filed_date` (string): Filing date of the report.
*   `frame` (string, nullable): XBRL frame/context information. Can be null.