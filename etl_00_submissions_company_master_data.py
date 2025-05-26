import zipfile
import json
import pandas as pd
import os
import re

# --- Configuration ---
# Info: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
# Download: https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip
SUBMISSIONS_ZIP_PATH = '/Users/akhil/Documents/Work/PI/RAW_DATA/submissions.zip'
OUTPUT_DIR = '/Users/akhil/Documents/Work/PI/PROCESSED_DATA/output/sec_company_master_data'
OUTPUT_FILE_PREFIX = 'sec_company_master_data'

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Helper Function to Sanitize CIK ---
def sanitize_cik(cik_string):
    """Ensure CIK is a 10-digit string with leading zeros."""
    if cik_string is None:
        return None
    return str(cik_string).zfill(10)

# --- Revised Parsing Function for submission JSON (Company Master Focus) ---
def parse_company_master_from_submission(json_data, cik_from_filename):
    """
    Parses a single JSON object from a submissions file for company master data.
    Expands ticker-exchange pairs into separate records.
    Returns a list of dictionaries.
    """
    records = []
    try:
        # Use CIK from JSON if available, otherwise from filename
        raw_cik = json_data.get('cik')
        cik = sanitize_cik(raw_cik if raw_cik else cik_from_filename)

        company_name = json_data.get('name')
        sic = json_data.get('sic')
        sic_description = json_data.get('sicDescription')
        tickers = json_data.get('tickers', []) # Default to empty list
        exchanges = json_data.get('exchanges', []) # Default to empty list

        # Ensure tickers and exchanges are lists
        if not isinstance(tickers, list):
            tickers = []
        if not isinstance(exchanges, list):
            exchanges = []
        
        base_record = {
            'cik': cik,
            'company_name': company_name,
            'sic': sic,
            'sic_description': sic_description,
        }

        if tickers and exchanges and len(tickers) == len(exchanges):
            for i in range(len(tickers)):
                record = base_record.copy()
                record['ticker'] = tickers[i]
                record['exchange'] = exchanges[i]
                records.append(record)
        elif tickers: # Only tickers, no exchanges or mismatched length
            for ticker in tickers:
                record = base_record.copy()
                record['ticker'] = ticker
                record['exchange'] = None # Or a placeholder like 'UNKNOWN'
                records.append(record)
        else: # No tickers
            record = base_record.copy()
            record['ticker'] = None
            record['exchange'] = None
            records.append(record)

    except Exception as e:
        print(f"Error parsing company master for CIK (from filename) {cik_from_filename}: {e}")
        # Optionally, create a placeholder record for error cases
        records.append({
            'cik': sanitize_cik(cik_from_filename),
            'company_name': 'ERROR_PARSING',
            'sic': None,
            'sic_description': None,
            'ticker': None,
            'exchange': None,
        })
    return records


# --- Generic Processing Function for a ZIP Archive (Simplified) ---
def process_zip_for_company_master(zip_filepath, parsing_function, output_file_prefix, batch_size):
    """
    Processes JSON files from a ZIP archive in batches and saves to Parquet.
    """
    batch_data = []
    file_counter = 0
    batch_counter = 0

    print(f"Starting company master processing for: {zip_filepath}")
    with zipfile.ZipFile(zip_filepath, 'r') as zf:
        member_list = [m for m in zf.namelist() if m.lower().endswith('.json') and not m.startswith('__MACOSX')]
        total_files = len(member_list)
        print(f"Found {total_files} JSON files in {zip_filepath}.")

        for member_name in member_list:
            try:
                # Extract CIK from filename (e.g., CIK0000320193.json -> 0000320193)
                # Handle cases where filename might just be '0000320193.json'
                cik_match = re.search(r'(?:CIK)?(\d{1,10})\.json$', member_name, re.IGNORECASE)
                cik_from_filename = sanitize_cik(cik_match.group(1)) if cik_match else member_name # Fallback to full member_name if no match

                with zf.open(member_name) as json_file:
                    json_content = json.load(json_file)
                    parsed_records = parsing_function(json_content, cik_from_filename)
                    if parsed_records:
                        batch_data.extend(parsed_records)
                file_counter += 1

                if file_counter % batch_size == 0 or file_counter == total_files:
                    if batch_data:
                        df = pd.DataFrame(batch_data)
                        # Define schema for consistency, especially for potentially None columns
                        # This helps pyarrow infer types correctly even if first batch has all Nones for a column
                        expected_columns = ['cik', 'company_name', 'sic', 'sic_description', 'ticker', 'exchange']
                        for col in expected_columns:
                            if col not in df.columns:
                                df[col] = None # Add missing columns as None
                        df = df[expected_columns] # Reorder and ensure all columns are present

                        output_path = os.path.join(OUTPUT_DIR, f"{output_file_prefix}_batch_{batch_counter}.parquet")
                        df.to_parquet(output_path, engine='pyarrow', index=False)
                        print(f"Processed {file_counter}/{total_files} CIKs. Saved: {output_path} with {len(df)} records.")
                        batch_data = []
                        batch_counter += 1
                    else:
                        print(f"Processed {file_counter}/{total_files} CIKs. No data in current batch to write.")

            except json.JSONDecodeError as e:
                print(f"Skipping malformed JSON file: {member_name} - {e}")
            except Exception as e:
                print(f"An error occurred processing file {member_name}: {e}")
                # Log CIK or filename for problematic files
                if 'cik_from_filename' in locals():
                    print(f"Problematic CIK (from filename): {cik_from_filename}")


    print(f"Finished processing {zip_filepath}. Total batches written: {batch_counter}")

# --- Main Execution ---
if __name__ == "__main__":
    print("Processing submissions.zip for Company Master Data...")
    process_zip_for_company_master(
        SUBMISSIONS_ZIP_PATH,
        parse_company_master_from_submission,
        OUTPUT_FILE_PREFIX,
        50000
    )
    print("\nCompany Master ETL process completed.")