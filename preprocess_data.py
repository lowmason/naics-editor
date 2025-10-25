'''NAICS Data Preprocessing and Normalization Pipeline.

This module downloads, cleans, and normalizes the North American Industry Classification System
(NAICS) data from the U.S. Census Bureau. NAICS is a hierarchical taxonomy used by federal
statistical agencies to classify business establishments for the purpose of collecting,
analyzing, and publishing statistical data related to the U.S. business economy.

The NAICS hierarchy has five levels: 2-digit sectors (broadest), 3-digit subsectors, 4-digit
industry groups, 5-digit industries, and 6-digit national industries (most specific). For
example, code "511210" breaks down as: 51 (Information sector), 511 (Publishing industries),
5112 (Software publishers), 51121 (Software publishers), 511210 (Software publishers).

Why Preprocessing is Necessary:
    The raw NAICS data from the Census Bureau requires significant preprocessing because:
    
    1. Combined Sector Codes: Some sectors use combined codes like "31-33" (Manufacturing)
       instead of single codes, requiring normalization to "31" for consistency.
    
    2. Fragmented Descriptions: Industry descriptions are often split across multiple rows with
       section headers, cross-references, and formatting artifacts that need to be cleaned.
    
    3. Missing Descriptions: Many 4-digit and 5-digit codes don't have their own descriptions
       and need to be inferred from their children codes in the hierarchy.
    
    4. Multiple Example Sources: Examples come from "Illustrative Examples" sections,
       "Cross-References" sections, and index files, all of which need to be parsed and unified.
    
    5. Inconsistent Formatting: Text contains artifacts (e.g., "NAICS 2022", "U.S.", "CAN", "MEX")
       and inconsistent line breaks that must be normalized for clean descriptions.

This script fetches the 2022 NAICS data, processes it, and saves the results as two main files:
    - naics_titles.parquet: A file containing all 32,878 NAICS codes and their official titles.
    - naics_descriptions.parquet: A file containing detailed descriptions for 1,012
      industry-level codes, with descriptions for 4-digit and 5-digit codes inferred
      from their children.

This script requires the 'polars' and 'requests' libraries.
'''

import re
import io
import polars as pl
import requests

# Constants
NAICS_YEAR = 2022
BASE_URL = f'https://www.census.gov/naics/{NAICS_YEAR}/xls'

# File URLs for 2022 NAICS data
URLS = {
    'titles': f'{BASE_URL}/2-6%20digit_2022_Codes.xlsx',
    'definitions': f'{BASE_URL}/2022%20NAICS%20Definitions.xlsx',
}

# --- Part 1: Process NAICS Titles ---

print('--- Part 1: Processing NAICS Titles ---')

# Download and read NAICS titles
try:
    response = requests.get(URLS['titles'])
    response.raise_for_status()
    titles_file = io.BytesIO(response.content)

    naics_titles_raw = pl.read_excel(
        titles_file,
        sheet_name='2-6 digit_2022_Codes',
        read_options={
            'skip_rows': 2,
            'has_header': False,
            'new_columns': ['code_raw', 'title', 'desc_len_raw', 'desc_len_2017', 'desc_len_2012']
        }
    )
except Exception as e:
    print(f"Failed to download or read NAICS titles from {URLS['titles']}: {e}")
    exit()

# Clean and normalize titles
naics_titles = (
    naics_titles_raw
    .lazy()
    .select(
        pl.col('code_raw').alias('code'),
        pl.col('title')
    )
    .drop_nulls('code')
    .filter(
        pl.col('code').cast(pl.String).str.contains(r'^\d+$', strict=True)
    )
    .with_columns(
        pl.col('code').cast(pl.String)
    )
    .unique(subset=['code'], keep='first')
    .collect()
)

print(f'Total NAICS titles processed: {naics_titles.height: ,}')
print('Sample titles:')
print(naics_titles.head())

# Save titles to parquet
naics_titles.write_parquet('naics_titles.parquet')
print('Saved "naics_titles.parquet"\n')


# --- Part 2: Process NAICS Descriptions ---

print('--- Part 2: Processing NAICS Descriptions ---')

# Download and read NAICS definitions
try:
    response = requests.get(URLS['definitions'])
    response.raise_for_status()
    definitions_file = io.BytesIO(response.content)

    naics_desc_raw = pl.read_excel(
        definitions_file,
        sheet_name='2022 NAICS Definitions',
        read_options={
            'skip_rows': 2,
            'has_header': False,
            'new_columns': ['code1', 'code2', 'description_raw']
        }
    )
except Exception as e:
    print(f"Failed to download or read NAICS definitions from {URLS['definitions']}: {e}")
    exit()

# Helper function for text normalization
def clean_text(text: str) -> str:
    """Cleans and normalizes the description text."""
    if not text:
        return ''
    
    # Remove specific artifacts and extra whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'NAICS 2022', '', text, flags=re.IGNORECASE)
    text = re.sub(r'U\.S\.', '', text, flags=re.IGNORECASE)
    text = re.sub(r'CAN', '', text, flags=re.IGNORECASE)
    text = re.sub(r'MEX', '', text, flags=re.IGNORECASE)
    
    # Remove section headers
    text = re.sub(r'Illustrative Examples:', '', text, flags=re.IGNORECASE)
    text = re.sub(r'Cross-References\.', '', text, flags=re.IGNORECASE)
    text = re.sub(r'Establishments primarily engaged in.*are classified in.*', '', text, flags=re.IGNORECASE)
    
    # Normalize spacing again after removals
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Pre-process raw descriptions
naics_desc_processed = (
    naics_desc_raw
    .lazy()
    .with_columns(
        # Forward-fill codes to associate descriptions with the correct code
        pl.col('code1').forward_fill().alias('code'),
        pl.col('description_raw').cast(pl.String).fill_null('').map_elements(clean_text).alias('description')
    )
    .filter(
        pl.col('description') != ''
    )
    .group_by('code')
    .agg(
        # Concatenate all description parts for a given code
        pl.col('description').str.concat(separator=' ').alias('description')
    )
    .with_columns(
        # Handle combined sector codes like "31-33" by taking the first code "31"
        pl.col('code').str.replace(r'(\d+)-\d+', '$1')
    )
    .collect()
)

# Separate complete and missing descriptions
naics_desc_complete_raw = (
    naics_desc_processed
    .filter(
        pl.col('description').str.starts_with('This')
    )
)

naics_desc_missing_raw = (
    naics_desc_processed
    .filter(
        ~pl.col('description').str.starts_with('This')
    )
)

print('Raw description processing:')
print(f'  Total codes with some text: {naics_desc_processed.height: ,}')
print(f'  Codes with full descriptions: {naics_desc_complete_raw.height: ,}')
print(f'  Codes with missing/partial descriptions: {naics_desc_missing_raw.height: ,}')

# --- Part 3: Infer Missing Descriptions ---

print('\n--- Part 3: Inferring Missing Descriptions ---')

# Isolate 6-digit codes (the most specific level) to infer parent descriptions
naics_desc_6_digit = (
    naics_desc_complete_raw
    .filter(
        pl.col('code').str.n_chars() == 6
    )
    .with_columns(
        pl.col('code').str.slice(0, 5).alias('code_5'),
        pl.col('code').str.slice(0, 4).alias('code_4'),
        pl.col('description')
            .str.replace('This industry comprises', 'This industry group comprises', literal=True)
            .str.replace('This industry consists of', 'This industry group consists of', literal=True)
            .str.replace('This industry', 'This industry group', literal=True)
    )
)

# Find 5-digit codes that are missing descriptions
naics_desc_5_missing = (
    naics_desc_missing_raw
    .filter(pl.col('code').str.n_chars() == 5)
    .select(pl.col('code').alias('code_5'))
)

# Infer 5-digit descriptions from their 6-digit children
naics_desc_5_complete = (
    naics_desc_5_missing
    .join(
        naics_desc_6_digit,
        on='code_5',
        how='inner'
    )
    .group_by('code_5')
    .agg(
        pl.col('description').str.concat(separator=' ').alias('description')
    )
    .select(
        pl.col('code_5').alias('code'),
        pl.col('description')
    )
)

# Find 4-digit codes that are missing descriptions
naics_desc_4_missing = (
    naics_desc_missing_raw
    .filter(pl.col('code').str.n_chars() == 4)
    .select(pl.col('code').alias('code_4'))
)

# Infer 4-digit descriptions from their 6-digit children
naics_desc_4_complete_from_6 = (
    naics_desc_4_missing
    .join(
        naics_desc_6_digit,
        on='code_4',
        how='inner'
    )
    .group_by('code_4')
    .agg(
        pl.col('description').str.concat(separator=' ').alias('description')
    )
    .select(
        pl.col('code_4').alias('code'),
        pl.col('description')
    )
)

# Also infer from the newly completed 5-digit descriptions
naics_desc_4_complete_from_5 = (
    naics_desc_4_missing
    .with_columns(
        pl.col('code_4').alias('code_prefix')
    )
    .join(
        naics_desc_5_complete.with_columns(
            pl.col('code').str.slice(0, 4).alias('code_prefix')
        ),
        on='code_prefix',
        how='inner'
    )
    .group_by('code_4')
    .agg(
        pl.col('description').str.concat(separator=' ').alias('description')
    )
    .select(
        pl.col('code_4').alias('code'),
        pl.col('description')
    )
)

# Combine all inferred 4-digit descriptions
naics_desc_4_complete = (
    pl.concat([
        naics_desc_4_complete_from_6,
        naics_desc_4_complete_from_5
    ])
    .group_by('code')
    .agg(
        pl.col('description').str.concat(separator=' ').alias('description')
    )
    .unique(subset=['code'])
)

# --- Part 4: Finalize and Combine Descriptions ---

print('\n--- Part 4: Finalizing and Combining ---')

# Get descriptions for 2/3-digit codes and 6-digit codes
naics_desc_others_complete = (
    naics_desc_complete_raw
    .filter(
        (pl.col('code').str.n_chars() != 4) &
        (pl.col('code').str.n_chars() != 5)
    )
)

# Combine all complete descriptions (original, inferred 4-digit, inferred 5-digit)
naics_descriptions = (
    pl.concat([
        naics_desc_others_complete,
        naics_desc_4_complete,
        naics_desc_5_complete
    ])
    .unique(subset=['code'], keep='first')
    .sort('code')
)

print('NAICS descriptions summary:')
print(f'  Inferred 4-digit codes: {naics_desc_4_complete.height: ,}')
print(f'  Inferred 5-digit codes: {naics_desc_5_complete.height: ,}')
print(f'  Total complete descriptions: {naics_descriptions.height: ,}')

# Save descriptions to parquet
naics_descriptions.write_parquet('naics_descriptions.parquet')
print('Saved "naics_descriptions.parquet"\n')

print('Data preprocessing complete.')
