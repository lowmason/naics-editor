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
    - db_input.parquet: A file containing detailed descriptions for 1,012
      industry-level codes, with descriptions for 4-digit and 5-digit codes inferred
      from their children.
'''


# -------------------------------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------------------------------

import io
from dataclasses import dataclass, field
from typing import Dict

import httpx
import polars as pl

# -------------------------------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------------------------------

@dataclass
class Config:

    '''Configuration for NAICS data sources and preprocessing pipeline.
    
    This configuration specifies the URLs, sheet names, schemas, and column mappings for all
    four NAICS data files from the U.S. Census Bureau. The configuration is carefully designed
    to handle the specific structure and quirks of the official NAICS 2022 Excel files.
    
    Attributes:
        url_codes: URL to Excel file containing NAICS codes and titles.
        url_index: URL to Excel file containing index items (examples).
        url_descriptions: URL to Excel file containing detailed industry descriptions.
        url_exclusions: URL to Excel file containing cross-references (exclusions).

        sheet_codes: Sheet name in codes file to read.
        sheet_index: Sheet name in index file to read.
        sheet_descriptions: Sheet name in descriptions file to read.
        sheet_exclusions: Sheet name in exclusions file to read.
        
        schema_codes: Column schema for codes file (ensures proper data types).
        schema_index: Column schema for index file (ensures proper data types).
        schema_descriptions: Column schema for descriptions file (ensures proper data types).
        schema_exclusions: Column schema for exclusions file (ensures proper data types).
        
        rename_codes: Column rename mapping for codes file (standardizes column names).
        rename_index: Column rename mapping for index file (standardizes column names).
        rename_descriptions: Column rename mapping for descriptions file (standardizes names).
        rename_exclusions: Column rename mapping for exclusions file (standardizes names).
    '''

    url_codes: str = 'https://www.census.gov/naics/2022NAICS/2-6%20digit_2022_Codes.xlsx'
    url_index: str = 'https://www.census.gov/naics/2022NAICS/2022_NAICS_Index_File.xlsx'
    url_descriptions: str = 'https://www.census.gov/naics/2022NAICS/2022_NAICS_Descriptions.xlsx'
    url_exclusions: str = 'https://www.census.gov/naics/2022NAICS/2022_NAICS_Cross_References.xlsx'

    sheet_codes: str = 'tbl_2022_title_description_coun'
    sheet_index: str = '2022NAICS'
    sheet_descriptions: str = '2022_NAICS_Descriptions'
    sheet_exclusions: str = '2022_NAICS_Cross_References'

    schema_codes: Dict[str, pl.DataType] = field(default_factory=lambda: {
        'Seq. No.': pl.UInt32,
        '2022 NAICS US   Code': pl.Utf8, 
        '2022 NAICS US Title': pl.Utf8
    })
    schema_index: Dict[str, pl.DataType] = field(default_factory=lambda: {
        'NAICS22': pl.Utf8,
        'INDEX ITEM DESCRIPTION': pl.Utf8
    })
    schema_descriptions: Dict[str, pl.DataType] = field(default_factory=lambda: {
        'Code': pl.Utf8,
        'Description': pl.Utf8
    })
    schema_exclusions: Dict[str, pl.DataType] = field(default_factory=lambda: {
        'Code': pl.Utf8,
        'Cross-Reference': pl.Utf8
    })
	
    rename_codes: Dict[str, str] = field(default_factory=lambda: {
        'Seq. No.': 'index',
        '2022 NAICS US   Code': 'code', 
        '2022 NAICS US Title': 'title'
    })        
    rename_index: Dict[str, str] = field(default_factory=lambda: {
        'NAICS22': 'code',
        'INDEX ITEM DESCRIPTION': 'examples_1'
    })
    rename_descriptions: Dict[str, str] = field(default_factory=lambda: {
        'Code': 'code',
        'Description': 'description'
    })
    rename_exclusions: Dict[str, str] = field(default_factory=lambda: {
        'Code': 'code',
        'Cross-Reference': 'excluded'
    })


# -------------------------------------------------------------------------------------------------
# Import utility
# -------------------------------------------------------------------------------------------------

def _read_naics_xlsx(
    url: str, 
    sheet: str, 
    schema: Dict[str, pl.DataType],
    cols: Dict[str, str]
) -> pl.DataFrame:
    
    '''Download and read a NAICS Excel file from the U.S. Census Bureau.
    
    This utility function handles the complete workflow of downloading an Excel file via HTTP,
    reading a specific sheet with controlled schema, and renaming columns to standardized names.
    The function is designed to be called multiple times for different NAICS data files, each
    with its own URL, sheet name, schema, and column mappings.
    
    Args:
        url: Full URL to the Excel file on the Census Bureau website.
        sheet: Name of the specific sheet to read from the Excel workbook.
        schema: Dictionary mapping original column names to Polars data types. This ensures
            proper type handling, especially for NAICS codes which must be strings.
        cols: Dictionary mapping original column names to standardized names used throughout
            the pipeline. This creates consistency across different source files.
    
    Returns:
        Polars DataFrame with the specified columns read from the Excel sheet, renamed to
        standardized column names, and with proper data types enforced.
    '''
    
    resp = httpx.get(url)
    resp.raise_for_status()
    data = resp.content

    f = io.BytesIO(data)

    return (
        pl
        .read_excel(
            f,
            sheet_name=sheet,
            columns=schema.keys(),
            schema_overrides=schema
        )
        .rename(mapping=cols)
    )  


# -------------------------------------------------------------------------------------------------
# Input and preprocess NAICS data
# -------------------------------------------------------------------------------------------------

def preprocess_naics_data(config: Config) -> None:

    '''Preprocess and normalize NAICS data from U.S. Census Bureau Excel files.
    
    This function orchestrates the complete preprocessing pipeline for NAICS data, handling
    downloading, cleaning, normalizing, and merging data from multiple source files. The
    final output is a consolidated Parquet file containing NAICS codes, titles, descriptions,
    exclusions, and examples.

    Args:
        config: Configuration object containing URLs, sheet names, schemas, and column mappings
            for all NAICS data files.
    '''

    # Load NAICS titles and normalize combined sector codes (31-33, 44-45, 48-49)
    naics_titles = (
        _read_naics_xlsx(
            url=config.url_codes,
            sheet=config.sheet_codes,
            schema=config.schema_codes,
            cols=config.rename_codes
        )
        .with_columns(
            code=pl.when(pl.col('code').eq('31-33')).then(pl.lit('31', pl.Utf8))
                .when(pl.col('code').eq('44-45')).then(pl.lit('44', pl.Utf8))
                .when(pl.col('code').eq('48-49')).then(pl.lit('48', pl.Utf8))
                .otherwise(pl.col('code'))
        )
        .select(
            index=pl.col('index')
                    .sub(1),
            level=pl.col('code')
                    .str.len_chars()
                    .cast(pl.UInt8),
            code=pl.col('code'),
            title=pl.col('title')

        )
    )

    naics_codes = set(
        naics_titles
        .get_column('code')
        .unique()
        .sort()
        .to_list()
    )


    print(f'Number of 2-6-digit NAICS codes: {len(naics_codes): ,}')
    print(f'Number of 2-6-digit NAICS titles: {naics_titles.height: ,}')


    # Aggregate examples from index file by code
    naics_examples_1 = (
        _read_naics_xlsx(
            url=config.url_index,
            sheet=config.sheet_index,
            schema=config.schema_index,
            cols=config.rename_index
        )
        .group_by('code', maintain_order=True)
        .agg(
            examples_1=pl.col('examples_1')
        )
    )

    print(f'Number of 6-digit NAICS examples: {naics_examples_1.height: ,}')


    # Load descriptions and normalize combined sector codes
    naics_descriptions_1 = (
        _read_naics_xlsx(
            url=config.url_descriptions,
            sheet=config.sheet_descriptions,
            schema=config.schema_descriptions, 
            cols=config.rename_descriptions    
        )
        .with_columns(
            code=pl.when(pl.col('code').eq('31-33')).then(pl.lit('31', pl.Utf8))
                .when(pl.col('code').eq('44-45')).then(pl.lit('44', pl.Utf8))
                .when(pl.col('code').eq('48-49')).then(pl.lit('48', pl.Utf8))
                .otherwise(pl.col('code'))
        )
        .select('code', 'description')
    )

    print(f'Number of 2-6-digit NAICS descriptions: {naics_descriptions_1.height: ,}')

    # Load descriptions and normalize combined sector codes
    naics_exclusions_1 = (
        _read_naics_xlsx(
            url=config.url_exclusions,
            sheet=config.sheet_exclusions,
            schema=config.schema_exclusions, 
            cols=config.rename_exclusions    
        )
        .with_columns(
            code=pl.when(pl.col('code').eq('31-33')).then(pl.lit('31', pl.Utf8))
                .when(pl.col('code').eq('44-45')).then(pl.lit('44', pl.Utf8))
                .when(pl.col('code').eq('48-49')).then(pl.lit('48', pl.Utf8))
                .otherwise(pl.col('code'))
        )
        .group_by('code', maintain_order=True)
        .agg(
            excluded=pl.col('excluded')
        )
        .select(
            code=pl.col('code'), 
            excluded=pl.col('excluded')
                    .list.join(' ')
        )
    )

    print(f'Number of 2-6-digit NAICS exclusions: {naics_exclusions_1.height: ,}')

    # Split multiline descriptions and filter out section headers and cross-references
    naics_descriptions_2 = (
        naics_descriptions_1
        .with_columns(
            description=pl.col('description')
                        .str.split('\r\n')
                        .list.eval(
                            pl.element()
                                .filter(pl.element().str.len_chars() > 0)
                        )
        )
        .explode('description')
        .with_columns(
            description_id=pl.col('description')
                            .cum_count()
                            .over('code')
        )
        .select('code', 'description_id', 'description')
        .filter(
            pl.col('description').ne('The Sector as a Whole'),
            ~pl.col('description').str.contains('Cross-References.'),
            pl.col('description').str.len_chars().gt(0)
        )
    )

    # Clean and normalize description text
    naics_descriptions_3 = (
        naics_descriptions_2
        .select(
            code=pl.col('code')
                    .str.strip_chars(),
            description_id=pl.col('description_id'),
            description=pl.col('description')
                        .str.strip_prefix(' ')
                        .str.strip_suffix(' ')
                        .str.replace_all(r'NULL', '')
                        .str.replace_all(r'See industry description for \d{6}\.', '')
                        .str.replace_all(r'<.*?>', '')
                        .str.replace_all(r'\xa0', ' ')
                        .str.replace_all('.', '. ', literal=True)
                        .str.replace_all('U. S. ', 'U.S.', literal=True)
                        .str.replace_all('e. g. ,', 'e.g.,', literal=True)
                        .str.replace_all('i. e. ,', 'i.e.,', literal=True)
                        .str.replace_all(';', '; ', literal=True)
                        .str.replace_all('31-33', '31', literal=True)
                        .str.replace_all('44-45', '44', literal=True)
                        .str.replace_all('48-49', '48', literal=True)
                        .str.replace_all(r'\s{2,}', ' ')
        )
        .with_columns(
            description=pl.when(pl.col('description').eq('')).then(None)
                        .otherwise(pl.col('description'))
        )
    )

    print(f'Number of 2-6-digit NAICS: {naics_descriptions_3.height: ,}')


    # Extract excluded activities (typically last description block for a code)
    naics_exclusions_2 = (
        naics_descriptions_3
        .filter(
            pl.col('description_id').max().over('code').eq(pl.col('description_id')),
            pl.col('description').str.contains_any(['Excluded', 'excluded'])
        )
        .select(
            code=pl.col('code')
                    .str.strip_chars(),
            description_id=pl.col('description_id'),
            description=pl.col('description')
        )
    )

    print(f'Number of 2-3-digit NAICS excluded: {naics_exclusions_2.height: ,}')
    naics_exclusions_3 = (
        naics_descriptions_3
        .join(
            naics_exclusions_2,
            how='anti',
            on='code'
        )
        .filter(
            pl.col('code').str.len_chars().gt(2),
            pl.col('description').is_not_null(),
            pl.col('description').str.contains(r' \d{2, 6}')
        )
        .with_columns(
            digit=pl.col('description')
                    .str.extract_all(r' \d{2, 6}')
                    .list.eval(pl.element().str.strip_prefix(' '))
                    .list.set_intersection(naics_codes)
                    .list.drop_nulls()
        )
        .filter(
            pl.col('digit').list.len().gt(0)
        )
    )

    naics_exclusions_3_1 = (
        naics_exclusions_3
        .filter(
            pl.col('digit').list.len().eq(1)
        )
        .explode('digit')
        .filter(
            ~pl.col('digit').str.contains(pl.col('code'))
        )
        .drop('digit')
    )

    naics_exclusions_3_2 = (
        naics_exclusions_3
        .filter(
            pl.col('digit').list.len().ne(1)
        )
        .drop('digit')
    )

    naics_exclusions_4 = (
        pl
        .concat([
            naics_exclusions_3_1,
            naics_exclusions_3_2
        ])
        .filter(
            ~pl.col('code').is_in(['525', '3152', '7132'])
        )
    )

    naics_exclusions_5 = (
        pl
        .concat([
            naics_exclusions_2,
            naics_exclusions_4
        ])
    )

    naics_exclusions_6 = (
        naics_exclusions_5
        .group_by('code', maintain_order=True)
        .agg(
            excluded=pl.col('description')
        )
        .with_columns(
            excluded=pl.col('excluded')
                    .list.join(' ')
        )
    )

    print(f'Number of 2-3-digit NAICS excluded: {naics_exclusions_5.height: ,}')

    naics_exclusions = (
        pl
        .concat([
            naics_exclusions_1,
            naics_exclusions_6
        ])
    )

    print(f'Number of all excluded: {naics_exclusions.height: ,}')

    # Identify where 'Illustrative Examples:' section begins
    naics_examples_2 = (
        naics_descriptions_2
        .filter(
            pl.col('description')
            .str.contains('Illustrative Examples:')
        )
        .select(
            code=pl.col('code'),
            example_id=pl.col('description_id')
        )
    )

    # Extract examples that appear after 'Illustrative Examples:' marker
    naics_examples_3 = (
        naics_descriptions_3
        .join(
            naics_examples_2,
            how='inner',
            on='code'
        )
        .filter(
            pl.col('example_id').lt(pl.col('description_id'))
        )
        .group_by('code', maintain_order=True)
        .agg(
            examples_2=pl.col('description')
        )
    )

    # Merge index examples with illustrative examples, preferring illustrative
    naics_examples = (
        naics_examples_1
        .join(
            naics_examples_3,
            how='full',
            on='code',
            coalesce=True
        )
        .select(
            code=pl.col('code'),
            examples=pl.coalesce(
                'examples_2',
                'examples_1'
            )
        )
    )

    print(f'Number of 2-6-digit NAICS examples: {naics_examples.height: ,}')

    # Extract main descriptions, excluding examples and exclusions
    naics_descriptions_4 = (
        naics_descriptions_3
        .join(
            naics_examples_2,
            how='left',
            on='code'
        )
        .with_columns(
            example_id=pl.col('example_id')
                        .fill_null(9999)
        )
        .filter(
            pl.col('example_id').gt(pl.col('description_id'))
        )
        .join(
            naics_exclusions_5,
            how='anti',
            on=['code', 'description_id']
        )
        .group_by('code', maintain_order=True)
        .agg(
            pl.col('description')
        )
        .with_columns(
            description=pl.col('description')
                        .list.join(' ')
        )
    )

    print(f'Number of 2-6-digit NAICS descriptions: {naics_descriptions_4.height: ,}')

    # Separate complete descriptions from missing ones
    naics_desc_complete_1 = (
        naics_descriptions_4
        .filter(
            pl.col('description').ne('')
        )    
    )

    # Find 4-digit codes missing descriptions
    naics_desc_4_missing = (
        naics_descriptions_4
        .filter(
            pl.col('code').str.len_chars().eq(4),
            pl.col('description').eq('')
        )
        .select(
            code1=pl.col('code')
                    .str.pad_end(5, '1'),
            code2=pl.col('code')
                    .str.pad_end(5, '2'),
            code3=pl.col('code')
                    .str.pad_end(5, '3'),
            code4=pl.col('code')
                    .str.pad_end(5, '4'),
            code9=pl.col('code')
                    .str.pad_end(5, '9')
        )
    )

    # Find 5-digit codes missing descriptions
    naics_desc_5_missing = (
        naics_descriptions_4
        .filter(
            pl.col('code').str.len_chars().eq(5),
            pl.col('description').eq('')
        )
        .select(
            code=pl.col('code')
                    .str.pad_end(6, '0')
        )
    )

    print('NAICS descriptions:')
    print(f'  Total: {naics_descriptions_4.height: ,}')
    print(f'  Complete: {naics_desc_complete_1.height: ,}')
    print(f'  Missing (level 4): {naics_desc_4_missing.height: ,}')
    print(f'  Missing (level 5): {naics_desc_5_missing.height: ,}')

    # Fill missing 5-digit descriptions from 6-digit children
    naics_desc_5_complete = (
        naics_desc_5_missing
        .join(
            naics_desc_complete_1,
            how='inner',
            on='code'
        )
        .with_columns(
            code=pl.col('code')
                    .str.slice(0, 5)
        )
        .select(
            code=pl.col('code'), 
            description=pl.col('description')
                        .str.replace('This industry', 'This NAICS industry', literal=True)
        )
    )

    naics_desc_complete_2 = (
        pl
        .concat([
            naics_desc_complete_1,
            naics_desc_5_complete
        ])
    )

    # Fill missing 4-digit descriptions from 5-digit children (try multiple suffixes)
    naics_desc_4_complete_1 = (
        naics_desc_4_missing
        .join(
            naics_desc_complete_2,
            how='inner',
            right_on='code',
            left_on='code1'
        )
    )

    naics_desc_4_complete_2 = (
        naics_desc_4_missing
        .join(
            naics_desc_complete_2,
            how='inner',
            right_on='code',
            left_on='code2'
        )
    )

    naics_desc_4_complete_3 = (
        naics_desc_4_missing
        .join(
            naics_desc_complete_2,
            how='inner',
            right_on='code',
            left_on='code3'
        )
    )

    naics_desc_4_complete_4 = (
        naics_desc_4_missing
        .join(
            naics_desc_complete_2,
            how='inner',
            right_on='code',
            left_on='code4'
        )
    )

    naics_desc_4_complete_9 = (
        naics_desc_4_missing
        .join(
            naics_desc_complete_2,
            how='inner',
            right_on='code',
            left_on='code9'
        )
    )

    naics_desc_4_complete = (
        pl
        .concat([
            naics_desc_4_complete_1,
            naics_desc_4_complete_2,
            naics_desc_4_complete_3,
            naics_desc_4_complete_4,
            naics_desc_4_complete_9
        ])
        .select(
            code=pl.col('code1')
                .str.slice(0, 4), 
            description=pl.col('description')
                        .str.replace('This industry', 'This industry group', literal=True)
                        .str.replace('This NAICS industry', 'This industry group', literal=True)
        )
        .unique(subset=['code'])
    )

    # Combine all descriptions
    naics_descriptions = (
        pl
        .concat([
            naics_desc_complete_2,
            naics_desc_4_complete
        ])
    )

    print('NAICS descriptions:')
    print(f'  Missing (level 4): {naics_desc_4_missing.height: ,}')
    print(f'  Filled missing (level 4): {naics_desc_4_complete.height: ,}')
    print(f'  Missing (level 5): {naics_desc_5_missing.height: ,}')
    print(f'  Filled missing (level 5): {naics_desc_5_complete.height: ,}')
    print(f'  Complete: {naics_descriptions.height: ,}\n')

    # Join all components and write final output
    naics_final = (
        naics_titles
        .join(
            naics_descriptions,
            how='left',
            on='code'
        )
        .join(
            naics_exclusions,
            how='left',
            on='code'
        )
        .join(
            naics_examples,
            how='left',
            on='code'
        )
    )

    (
        naics_final
        .write_parquet(
            './db_input.parquet'
        )
    )

    print(f'{naics_final.height: ,} NAICS descriptions written to:')
    print('  ./db_input.parquet') 


# -------------------------------------------------------------------------------------------------
# Main Entry Point
# -------------------------------------------------------------------------------------------------

if __name__ == '__main__':

    cfg = Config()

    preprocess_naics_data(cfg)