import polars as pl
import duckdb

# Define file paths
SOURCE_PARQUET = 'naics_descriptions.parquet'
TITLES_PARQUET = 'naics_titles.parquet'
FINAL_PARQUET = 'naics_descriptions_final.parquet'
DB_PATH = 'naics_data.db'

print('--- Starting Data to DB Pipeline ---')

# --- Part 1: Final Data Preparation (unchanged) ---
try:
    print(f'Reading {SOURCE_PARQUET} and {TITLES_PARQUET}...')
    # Load descriptions and titles
    df_desc = pl.read_parquet(SOURCE_PARQUET)
    df_titles = pl.read_parquet(TITLES_PARQUET)

    # Join descriptions with titles
    df_final = df_titles.join(df_desc, on='code', how='left')

    # Add parent code and title information for hierarchy
    df_final = df_final.with_columns(
        pl.when(pl.col('code').str.n_chars() > 2)
        .then(pl.col('code').str.slice(0, -1))
        .otherwise(None)
        .alias('parent_code')
    ).join(
        df_titles.rename({'title': 'parent_title', 'code': 'parent_code'}),
        on='parent_code',
        how='left'
    )

    # Fill null descriptions with a placeholder
    df_final = df_final.with_columns(
        pl.col('description').fill_null('No description available.')
    )

    # Save final combined parquet
    df_final.write_parquet(FINAL_PARQUET)
    print(f'Successfully created {FINAL_PARQUET} with {df_final.height} rows.')

except Exception as e:
    print(f'Error during data preparation: {e}')
    exit()

# --- Part 2: Load to DuckDB ---
try:
    print(f'Connecting to DuckDB at {DB_PATH}...')
    con = duckdb.connect(DB_PATH)

    print('Creating main `naics` table...')
    # Create the main table from the final parquet file
    con.execute(f"""
        DROP TABLE IF EXISTS naics;
        CREATE TABLE naics AS
        SELECT * FROM read_parquet('{FINAL_PARQUET}');
    """)

    print('Creating Full-Text Search (FTS) index...')
    # Create FTS index
    con.execute("PRAGMA create_fts_index('naics', 'code', 'title', 'description', overwrite=1);")

    # --- NEW: Create (or reset) the naics_edits table ---
    print('Creating `naics_edits` table...')
    con.execute("""
        DROP TABLE IF EXISTS naics_edits;
        CREATE TABLE naics_edits (
            code TEXT PRIMARY KEY,
            description TEXT
        );
    """)
    print('All tables created successfully.')

    # Verify by showing tables
    print('\nDatabase tables:')
    con.execute("SHOW TABLES;").print()

    print('\n`naics` table schema:')
    con.execute("DESCRIBE naics;").print()

    print('\n`naics_edits` table schema:')
    con.execute("DESCRIBE naics_edits;").print()

    con.close()
    print('--- Database Pipeline Complete ---')

except Exception as e:
    print(f'Error during database loading: {e}')
    exit()
