# -------------------------------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------------------------------

from pathlib import Path
from dataclasses import dataclass

import duckdb


# -------------------------------------------------------------------------------------------------
# Dataclass for Script Arguments
# -------------------------------------------------------------------------------------------------

@dataclass
class Config:

    '''Holds the configuration for the export script.'''

    db_file: str = './naics_data.db'

    output_file: str = 'naics_descriptions_final.parquet'


# -------------------------------------------------------------------------------------------------
# Main Export Function
# -------------------------------------------------------------------------------------------------

def export_to_parquet(db_file: str, output_file: str):

    '''
    Connects to the DuckDB database, reads the 'naics' table,
    and exports it to a specified Parquet file.
    '''

    if not Path(db_file).exists():
        print(f"Error: Database file '{db_file}' not found.")
    else:
        print(f"Connecting to database '{db_file}'...")

    try:

        with duckdb.connect(db_file, read_only=True) as con:

            print(f"Exporting data to '{output_file}'...")
            
            con.execute(f"COPY (SELECT * FROM naics ORDER BY index) TO '{output_file}' (FORMAT PARQUET);")
            
            print('Export completed successfully.')
            
    except Exception as e:
        print(f'An error occurred during export: {e}')


# -------------------------------------------------------------------------------------------------
# Main Execution Block
# -------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    
    cfg = Config()
    
    # Simple manual parsing of sys.argv to override the default
    
    export_to_parquet(cfg.db_file, cfg.output_file)

