import zipfile
import json
import pandas as pd
import numpy as np
import os
import re

# --- Global Configuration Flags ---
CREATE_FILING_LEVEL_OUTPUT = True
CREATE_TIME_SERIES_OUTPUT = True

# --- Configuration ---
# Info: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
# Download: http://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip
COMPANYFACTS_ZIP_PATH = '/Users/akhil/Documents/Work/PI/RAW_DATA/companyfacts.zip'

OUTPUT_DIR_FILING_LEVEL = '/Users/akhil/Documents/Work/PI/PROCESSED_DATA/output/company_form_data_filing_level'
OUTPUT_DIR_TIME_SERIES = '/Users/akhil/Documents/Work/PI/PROCESSED_DATA/output/company_form_data_time_series'

# Batch sizes refer to the number of records to accumulate before writing a Parquet file
FILING_LEVEL_BATCH_SIZE = 10000  # Number of aggregated filing records per batch
TIME_SERIES_BATCH_SIZE = 5000000 # Number of individual time-series fact records per batch

OUTPUT_FILE_PREFIX_FILING_LEVEL = 'company_form_data_filing_level'
OUTPUT_FILE_PREFIX_TIME_SERIES = 'company_form_data_time_series'

if CREATE_FILING_LEVEL_OUTPUT:
    os.makedirs(OUTPUT_DIR_FILING_LEVEL, exist_ok=True)
if CREATE_TIME_SERIES_OUTPUT:
    os.makedirs(OUTPUT_DIR_TIME_SERIES, exist_ok=True)

def sanitize_cik(cik_string):
    if cik_string is None:
        return None
    return str(cik_string).zfill(10)

def parse_companyfacts_for_aggregation(json_data, cik_from_filename):
    filing_level_records_for_cik = []
    time_series_records_for_cik = []

    try:
        raw_cik_json = json_data.get('cik')
        processed_cik = sanitize_cik(str(raw_cik_json) if raw_cik_json is not None else cik_from_filename)
        entity_name = json_data.get('entityName')
        facts_by_taxonomy = json_data.get('facts', {})
        current_cik_filings_aggregator = {}

        for taxonomy, concepts in facts_by_taxonomy.items():
            for concept_name, concept_data in concepts.items():
                label = concept_data.get('label')
                units = concept_data.get('units', {})

                for unit_type, fact_instances_list in units.items():
                    for fact_instance in fact_instances_list:
                        form_type = fact_instance.get('form')

                        if form_type in ['10-K', '10-Q']:
                            if CREATE_TIME_SERIES_OUTPUT:
                                raw_value = fact_instance.get('val')
                                processed_value = np.nan # Default to NaN
                                if raw_value is not None:
                                    try:
                                        processed_value = float(raw_value)
                                    except (ValueError, TypeError):
                                        processed_value = np.nan

                                raw_fiscal_year = fact_instance.get('fy')
                                processed_fiscal_year = pd.NA # Use pandas NA for nullable integers
                                if raw_fiscal_year is not None:
                                    try:
                                        processed_fiscal_year = int(raw_fiscal_year)
                                    except (ValueError, TypeError):
                                        # If 'fy' is not a valid int, keep as pd.NA or log
                                        # print(f"CIK {processed_cik}, Concept {concept_name}: Could not convert fiscal_year '{raw_fiscal_year}' to int.")
                                        pass # Keep as pd.NA

                                time_series_record = {
                                    'cik': processed_cik,
                                    'entity_name': entity_name,
                                    'taxonomy': taxonomy,
                                    'concept': concept_name,
                                    'label': label,
                                    'unit': unit_type,
                                    'period_end_date': fact_instance.get('end'), # Will be converted to date before to_parquet
                                    'value': processed_value,
                                    'accession_number': fact_instance.get('accn'),
                                    'fiscal_year': processed_fiscal_year, # Use the processed int or pd.NA
                                    'fiscal_period': fact_instance.get('fp'),
                                    'form_type': form_type,
                                    'filed_date': fact_instance.get('filed'), # Will be converted to date before to_parquet
                                    'frame': fact_instance.get('frame')
                                }
                                time_series_records_for_cik.append(time_series_record)

                            if CREATE_FILING_LEVEL_OUTPUT:
                                # ... (filing level logic remains the same) ...
                                accn = fact_instance.get('accn')
                                # filed_date for filing_record will be converted before to_parquet if present as a column
                                filed_date_val = fact_instance.get('filed')
                                fy_val = fact_instance.get('fy') # Keep original for this JSON
                                fp = fact_instance.get('fp')
                                filing_key = (accn, form_type, filed_date_val, fy_val, fp)
                                if filing_key not in current_cik_filings_aggregator:
                                    current_cik_filings_aggregator[filing_key] = []
                                current_cik_filings_aggregator[filing_key].append(fact_instance)


        if CREATE_FILING_LEVEL_OUTPUT:
            for key_tuple, list_of_facts in current_cik_filings_aggregator.items():
                accn_k, form_k, filed_k, fy_k, fp_k = key_tuple
                filing_record = {
                    'cik': processed_cik,
                    'entity_name': entity_name,
                    'accession_number': accn_k,
                    'form_type': form_k,
                    'filed_date': filed_k, # This will be converted to date before to_parquet
                    'fiscal_year': fy_k, # Store original fiscal year for this output
                    'fiscal_period': fp_k,
                    'all_facts_json_array': json.dumps(list_of_facts)
                }
                filing_level_records_for_cik.append(filing_record)

    except Exception as e:
        error_message = f"Error in parse_companyfacts_for_aggregation for CIK (from filename: {cik_from_filename}): {type(e).__name__} - {e}"
        if 'fact_instance' in locals() and 'val' in fact_instance:
            error_message += f" | Problematic fact_instance['val']: {fact_instance.get('val')}"
        elif 'fact_instance' in locals() and 'fy' in fact_instance:
            error_message += f" | Problematic fact_instance['fy']: {fact_instance.get('fy')}"
        print(error_message)
    return filing_level_records_for_cik, time_series_records_for_cik


def process_companyfacts_zip(zip_filepath, parsing_function):
    all_filing_level_batch_data = []
    all_time_series_batch_data = []
    cik_file_counter = 0
    filing_level_batch_counter = 0
    time_series_batch_counter = 0
    total_filing_records_written = 0
    total_time_series_records_written = 0

    print(f"Starting companyfacts processing from: {zip_filepath}")
    with zipfile.ZipFile(zip_filepath, 'r') as zf:
        member_list = [m for m in zf.namelist() if m.lower().endswith('.json') and not m.startswith('__MACOSX')]
        total_cik_files = len(member_list)
        print(f"Found {total_cik_files} JSON files (CIKs) in {zip_filepath}.")

        for member_name in member_list:
            cik_from_filename = "UNKNOWN_CIK"
            try:
                cik_match = re.search(r'(?:CIK)?(\d{1,10})\.json$', member_name, re.IGNORECASE)
                cik_from_filename = sanitize_cik(cik_match.group(1)) if cik_match else member_name

                with zf.open(member_name) as json_file:
                    json_content = json.load(json_file)
                    cik_filing_level_out, cik_time_series_out = parsing_function(json_content, cik_from_filename)

                    if CREATE_FILING_LEVEL_OUTPUT and cik_filing_level_out:
                        all_filing_level_batch_data.extend(cik_filing_level_out)
                    if CREATE_TIME_SERIES_OUTPUT and cik_time_series_out:
                        all_time_series_batch_data.extend(cik_time_series_out)
                cik_file_counter += 1

                if CREATE_FILING_LEVEL_OUTPUT and (len(all_filing_level_batch_data) >= FILING_LEVEL_BATCH_SIZE or (cik_file_counter == total_cik_files and all_filing_level_batch_data)):
                    df_filing = pd.DataFrame(all_filing_level_batch_data)
                    expected_cols_filing = ['cik', 'entity_name', 'accession_number', 'form_type', 'filed_date', 'fiscal_year', 'fiscal_period', 'all_facts_json_array']
                    for col in expected_cols_filing:
                        if col not in df_filing.columns: df_filing[col] = None
                    df_filing = df_filing[expected_cols_filing]
                    
                    # --- FIX: Convert filed_date to datetime.date objects ---
                    if 'filed_date' in df_filing.columns:
                        df_filing['filed_date'] = pd.to_datetime(df_filing['filed_date'], errors='coerce').dt.date
                    # --- END FIX ---
                    
                    # Apply Int64Dtype for fiscal_year if it's in this DataFrame
                    if 'fiscal_year' in df_filing.columns: # Ensure fiscal_year exists before trying to cast
                         try:
                            df_filing['fiscal_year'] = pd.to_numeric(df_filing['fiscal_year'], errors='coerce').astype(pd.Int64Dtype())
                         except Exception as e_fy_filing:
                            print(f"Warning (Filing Level): Could not convert fiscal_year. Error: {e_fy_filing}")


                    output_path = os.path.join(OUTPUT_DIR_FILING_LEVEL, f"{OUTPUT_FILE_PREFIX_FILING_LEVEL}_batch_{filing_level_batch_counter}.parquet")
                    df_filing.to_parquet(output_path, engine='pyarrow', index=False)
                    print(f"FILING-LEVEL: Processed {cik_file_counter}/{total_cik_files} CIKs. Saved batch {filing_level_batch_counter} ({len(df_filing)} records) to {output_path}")
                    total_filing_records_written += len(df_filing)
                    all_filing_level_batch_data = []
                    filing_level_batch_counter += 1

                if CREATE_TIME_SERIES_OUTPUT and (len(all_time_series_batch_data) >= TIME_SERIES_BATCH_SIZE or (cik_file_counter == total_cik_files and all_time_series_batch_data)):
                    df_ts = pd.DataFrame(all_time_series_batch_data)
                    expected_cols_ts = ['cik', 'entity_name', 'taxonomy', 'concept', 'label', 'unit', 'period_end_date', 'value', 'accession_number', 'fiscal_year', 'fiscal_period', 'form_type', 'filed_date', 'frame']
                    for col in expected_cols_ts:
                         if col not in df_ts.columns: df_ts[col] = None 
                    df_ts = df_ts[expected_cols_ts] 

                    # --- FIX: Convert date columns to datetime.date objects ---
                    if 'filed_date' in df_ts.columns:
                        df_ts['filed_date'] = pd.to_datetime(df_ts['filed_date'], errors='coerce').dt.date
                    if 'period_end_date' in df_ts.columns: 
                        df_ts['period_end_date'] = pd.to_datetime(df_ts['period_end_date'], errors='coerce').dt.date
                    # --- END FIX ---

                    dtype_map_ts = {
                        'value': 'float64',            
                        'fiscal_year': pd.Int64Dtype() 
                    }
                    for col, dt in dtype_map_ts.items():
                        if col in df_ts.columns:
                            try:
                                if col == 'fiscal_year': # Special handling for nullable int
                                    df_ts[col] = pd.to_numeric(df_ts[col], errors='coerce').astype(pd.Int64Dtype())
                                else:
                                    df_ts[col] = df_ts[col].astype(dt)
                            except Exception as e_dtype:
                                print(f"Warning (Time Series): Could not convert column {col} to {dt} for DataFrame. Error: {e_dtype}")
                                if col == 'fiscal_year':
                                    print(f"Attempting pd.to_numeric for {col} as fallback after error.")
                                    try:
                                        df_ts[col] = pd.to_numeric(df_ts[col], errors='coerce').astype(pd.Int64Dtype())
                                    except Exception as e_fallback:
                                        print(f"Fallback for {col} also failed. Error: {e_fallback}")
                                        
                    output_path = os.path.join(OUTPUT_DIR_TIME_SERIES, f"{OUTPUT_FILE_PREFIX_TIME_SERIES}_batch_{time_series_batch_counter}.parquet")
                    df_ts.to_parquet(output_path, engine='pyarrow', index=False, coerce_timestamps='ms', allow_truncated_timestamps=False)
                    print(f"TIME-SERIES: Processed {cik_file_counter}/{total_cik_files} CIKs. Saved batch {time_series_batch_counter} ({len(df_ts)} records) to {output_path}")
                    total_time_series_records_written += len(df_ts)
                    all_time_series_batch_data = []
                    time_series_batch_counter += 1

                if cik_file_counter % 200 == 0 :
                    print(f"Progress: Processed {cik_file_counter}/{total_cik_files} CIK files.")
            except json.JSONDecodeError as e:
                print(f"Skipping malformed JSON in CIK {cik_from_filename} (file: {member_name}): {e}")
            except Exception as e:
                error_message = f"An error occurred processing CIK {cik_from_filename} (file: {member_name}): {type(e).__name__} - {e}"
                print(error_message)

    # Final check for any remaining data in the last batch for FILING_LEVEL
    if CREATE_FILING_LEVEL_OUTPUT and all_filing_level_batch_data:
        df_filing = pd.DataFrame(all_filing_level_batch_data)
        expected_cols_filing = ['cik', 'entity_name', 'accession_number', 'form_type', 'filed_date', 'fiscal_year', 'fiscal_period', 'all_facts_json_array']
        for col in expected_cols_filing:
            if col not in df_filing.columns: df_filing[col] = None
        df_filing = df_filing[expected_cols_filing]
        if 'filed_date' in df_filing.columns:
            df_filing['filed_date'] = pd.to_datetime(df_filing['filed_date'], errors='coerce').dt.date
        if 'fiscal_year' in df_filing.columns:
            try:
                df_filing['fiscal_year'] = pd.to_numeric(df_filing['fiscal_year'], errors='coerce').astype(pd.Int64Dtype())
            except Exception as e_fy_final_filing:
                print(f"Warning (Final Filing Batch): Could not convert fiscal_year. Error: {e_fy_final_filing}")
        output_path = os.path.join(OUTPUT_DIR_FILING_LEVEL, f"{OUTPUT_FILE_PREFIX_FILING_LEVEL}_batch_{filing_level_batch_counter}.parquet")
        df_filing.to_parquet(output_path, engine='pyarrow', index=False)
        print(f"FILING-LEVEL: Saved final batch {filing_level_batch_counter} ({len(df_filing)} records) to {output_path}")
        total_filing_records_written += len(df_filing)

    # Final check for any remaining data in the last batch for TIME_SERIES
    if CREATE_TIME_SERIES_OUTPUT and all_time_series_batch_data:
        df_ts = pd.DataFrame(all_time_series_batch_data)
        expected_cols_ts = ['cik', 'entity_name', 'taxonomy', 'concept', 'label', 'unit', 'period_end_date', 'value', 'accession_number', 'fiscal_year', 'fiscal_period', 'form_type', 'filed_date', 'frame']
        for col in expected_cols_ts:
            if col not in df_ts.columns: df_ts[col] = None
        df_ts = df_ts[expected_cols_ts]
        if 'filed_date' in df_ts.columns:
            df_ts['filed_date'] = pd.to_datetime(df_ts['filed_date'], errors='coerce').dt.date
        if 'period_end_date' in df_ts.columns:
            df_ts['period_end_date'] = pd.to_datetime(df_ts['period_end_date'], errors='coerce').dt.date
        dtype_map_ts = {
            'value': 'float64',
            'fiscal_year': pd.Int64Dtype()
        }
        for col, dt in dtype_map_ts.items():
            if col in df_ts.columns:
                try:
                    if col == 'fiscal_year':
                        df_ts[col] = pd.to_numeric(df_ts[col], errors='coerce').astype(pd.Int64Dtype())
                    else:
                        df_ts[col] = df_ts[col].astype(dt)
                except Exception as e_dtype_final:
                    print(f"Warning (Final Time Series Batch): Could not convert column {col} to {dt}. Error: {e_dtype_final}")
        output_path = os.path.join(OUTPUT_DIR_TIME_SERIES, f"{OUTPUT_FILE_PREFIX_TIME_SERIES}_batch_{time_series_batch_counter}.parquet")
        df_ts.to_parquet(output_path, engine='pyarrow', index=False, coerce_timestamps='ms', allow_truncated_timestamps=False)
        print(f"TIME-SERIES: Saved final batch {time_series_batch_counter} ({len(df_ts)} records) to {output_path}")
        total_time_series_records_written += len(df_ts)

    print(f"\nFinished processing {zip_filepath}.")
    if CREATE_FILING_LEVEL_OUTPUT:
        print(f"Total filing-level aggregated records written: {total_filing_records_written} in {filing_level_batch_counter} batches.")
    if CREATE_TIME_SERIES_OUTPUT:
        print(f"Total time-series metric records written: {total_time_series_records_written} in {time_series_batch_counter} batches.")

if __name__ == "__main__":
    if not CREATE_FILING_LEVEL_OUTPUT and not CREATE_TIME_SERIES_OUTPUT:
        print("Both output flags are set to False. No processing will occur.")
    else:
        print("Starting CompanyFacts ETL Process...")
        process_companyfacts_zip(
            COMPANYFACTS_ZIP_PATH,
            parse_companyfacts_for_aggregation
        )
        print("\nCompanyFacts ETL process completed.")
