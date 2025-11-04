"""
KMFK Data Mapping Script
Reads downloaded Excel files and maps them to the standard KMFK format.
Uses hardcoded column template to ensure consistency.
"""

import os
import pandas as pd
import json
import logging
from datetime import datetime

# Create directories if they don't exist
os.makedirs('downloads', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Setup logging with timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/mapper_{timestamp}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DOWNLOAD_DIR = "downloads"
OUTPUT_FILE = f"KMFK_DATA_{datetime.now().strftime('%Y%m%d')}.xlsx"
TEMPLATE_FILE = "column_template.json"

# Column name mappings from Korean to English (matching reference format)
KOREAN_TO_ENGLISH_CATEGORY = {
    '자산총액': 'TotalAsset',  # Matches reference: TOTALASSET (singular, no spaces)
    '주식': 'Stock',
    '채권': 'Bonds',
    'CP': 'CP',
    '어음': 'Notes',
    '집합투자증권': 'CollectiveInvestmentSecurities',
    '파생상품': 'Derivatives',
    '부동산': 'RealEstate',
    '특별자산': 'SpecialAssets',
    '단기대출및예금': 'Deposit',
    '예금': 'Deposit',  # Short form (actually seen in files)
    '콜론': 'CallLoan',  # Call loan
    '기타': 'Other'  # Note: singular "Other" to match template (template uses "Others" in description)
}

KOREAN_TO_ENGLISH_METRIC = {
    '금액': 'Amount',
    '비중': 'Weight'
}

# Dataset name to code prefix mapping (ordered) - must match template exactly
DATASET_CODE_PREFIX = {
    'Equity': 'EQUITY',
    'DomesticEquity': 'DOMESTICEQUITY',
    'HybridEquity': 'HYBRIDEQUITY',
    'HybridDomesticEquity': 'HYBRIDDOMEQUITY',  # Template uses shortened form
    'HybridBond': 'HYBRIDBOND',
    'HybridDomesticBond': 'HYBRIDDOMBOND',  # Template uses shortened form
    'Bond': 'BOND',
    'DomesticBond': 'DOMBOND',  # Template uses shortened form
    'MoneyMarket': 'MONEYMARKET',
    'HybridAsset': 'HYBRIDASSET',
    'DomesticHybridAsset': 'DOMHYBRIDASSET'  # Template uses shortened form
}

# Processing order - must match reference file
PROCESSING_ORDER = [
    'Equity',
    'DomesticEquity',
    'HybridEquity',
    'HybridDomesticEquity',
    'HybridBond',
    'HybridDomesticBond',
    'Bond',
    'DomesticBond',
    'MoneyMarket',
    'HybridAsset',
    'DomesticHybridAsset'
]


def read_and_process_file(file_path):
    """
    Read a downloaded Excel file and intelligently map columns to template codes.
    This function is universal - it reads Korean headers and maps them regardless of position/order.

    Args:
        file_path: Path to the Excel file

    Returns:
        DataFrame with processed data (columns are template codes)
    """
    # Get dataset name from filename (without extension)
    dataset_name = os.path.splitext(os.path.basename(file_path))[0]
    code_prefix = DATASET_CODE_PREFIX.get(dataset_name, dataset_name.upper())

    logger.info(f"Processing {dataset_name}...")

    # Read Excel file with multi-level headers (rows 2 and 3 are Korean headers)
    df = pd.read_excel(file_path, header=[2, 3], engine='openpyxl')

    # Map columns by reading Korean headers
    column_mapping = {}  # Maps current column index to template code
    code_description_map = {}

    for i, (col_level_0, col_level_1) in enumerate(df.columns):
        # Check if this is the date column
        is_date = ('Unnamed' in str(col_level_0) and 'Unnamed' in str(col_level_1)) or \
                  ('기준일자' in str(col_level_0)) or ('기준일' in str(col_level_0))

        if is_date:
            column_mapping[i] = 'Date'
        else:
            # Read Korean category name
            korean_category = col_level_0.strip()
            korean_metric = col_level_1.strip()

            # Map to English
            english_category = KOREAN_TO_ENGLISH_CATEGORY.get(korean_category, korean_category)

            # Check if this is a single-value column (Unnamed metric means no Amount/Weight split)
            if 'Unnamed' in str(col_level_1):
                # Single value column (e.g., Total Asset)
                code = f"KMFK.{code_prefix}.{english_category.upper()}.M"
                description = f"{dataset_name}: {english_category}"
            else:
                # Has metric (Amount or Weight)
                english_metric = KOREAN_TO_ENGLISH_METRIC.get(korean_metric, korean_metric)
                code = f"KMFK.{code_prefix}.{english_category.upper()}.{english_metric.upper()}.M"
                description = f"{dataset_name}: {english_category}: {english_metric}"

            column_mapping[i] = code
            code_description_map[code] = description

    # Create new dataframe with mapped column names
    new_columns = [column_mapping[i] for i in range(len(df.columns))]
    df.columns = new_columns

    # Store metadata
    df.attrs['code_description_map'] = code_description_map

    # Process date column
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Date'] = df['Date'].dt.strftime('%Y-%m')

    # Clean numeric columns
    for col in df.columns:
        if col != 'Date':
            if len(df) > 0 and isinstance(df[col].iloc[0], str):
                df[col] = df[col].str.replace(',', '')
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def merge_all_datasets():
    """
    Read all downloaded files and merge them into a single DataFrame in the correct order.

    Returns:
        Tuple of (DataFrame with all datasets merged, combined code-description map)
    """
    logger.info(f"Processing {len(PROCESSING_ORDER)} datasets in order")
    logger.info("-" * 60)

    all_dataframes = []
    combined_code_desc_map = {}

    # Process files in the specified order
    for dataset_name in PROCESSING_ORDER:
        file_path = os.path.join(DOWNLOAD_DIR, f"{dataset_name}.xls")

        if not os.path.exists(file_path):
            logger.warning(f"{dataset_name}.xls not found, skipping...")
            continue

        df = read_and_process_file(file_path)

        # Collect code-description mappings
        combined_code_desc_map.update(df.attrs.get('code_description_map', {}))

        all_dataframes.append(df)

    if not all_dataframes:
        raise FileNotFoundError(f"No dataset files found in {DOWNLOAD_DIR}")

    # Merge all dataframes on Date column
    merged_df = all_dataframes[0]
    for df in all_dataframes[1:]:
        merged_df = pd.merge(merged_df, df, on='Date', how='outer')

    # Sort by date
    merged_df = merged_df.sort_values('Date').reset_index(drop=True)

    return merged_df, combined_code_desc_map


def create_output_file(df):
    """
    Create the final output Excel file with CODE and DESCRIPTION rows using the hardcoded template.
    The template ensures consistent column order regardless of input data order.

    Args:
        df: DataFrame with processed data (columns may be in any order)
    """
    # Load the column template (defines output order and structure)
    with open(TEMPLATE_FILE, 'r', encoding='utf-8') as f:
        template = json.load(f)

    # Create ordered list of expected columns from template
    expected_codes = ['Date'] + [item['code'] for item in template]
    expected_descriptions = [''] + [item['description'] for item in template]

    # Reorder dataframe columns to match template order
    # Add missing columns with NaN, skip extra columns not in template
    final_columns = []
    for code in expected_codes:
        if code in df.columns:
            final_columns.append(code)
        else:
            # Column missing from data - add it with NaN values
            df[code] = None
            final_columns.append(code)
            logger.warning(f"Column '{code}' not found in data, filled with N.A.")

    # Reorder dataframe to match template
    df_ordered = df[final_columns].copy()

    # Replace NaN/None with "N.A." string for all data columns (except Date)
    for col in df_ordered.columns:
        if col != 'Date':
            df_ordered[col] = df_ordered[col].fillna('N.A.')

    # Create final DataFrame with CODE and DESCRIPTION rows
    code_row = pd.DataFrame([expected_codes], columns=final_columns)
    desc_row = pd.DataFrame([expected_descriptions], columns=final_columns)

    # Combine: CODE, DESCRIPTION, then data
    final_df = pd.concat([code_row, desc_row, df_ordered], ignore_index=True)

    # Write to Excel
    final_df.to_excel(OUTPUT_FILE, index=False, header=False, engine='openpyxl')
    logger.info("-" * 60)
    logger.info(f"Output file created: {OUTPUT_FILE}")
    logger.info(f"  - Total columns: {len(final_df.columns)}")
    logger.info(f"  - Total rows (including headers): {len(final_df)}")
    logger.info(f"  - Data rows: {len(final_df) - 2}")


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("KMFK Data Mapping Script")
    logger.info("=" * 60)

    try:
        # Merge all datasets
        merged_df, _ = merge_all_datasets()

        # Create output file using hardcoded template
        create_output_file(merged_df)

        logger.info("=" * 60)
        logger.info("Mapping completed successfully!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        logger.error("=" * 60)
        raise


if __name__ == "__main__":
    main()
