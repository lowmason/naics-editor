'''NAICS Data Editor - FastAPI Web Application.

This module provides a FastAPI-based web application for searching, viewing, and editing
North American Industry Classification System (NAICS) data. The application serves as an
interactive editor with a modern web interface for managing NAICS classifications, descriptions,
examples, and exclusions.

Architecture:
    The application consists of three main components:
    
    1. FastAPI Backend: RESTful API endpoints for data retrieval and updates.
    2. DuckDB Database: High-performance embedded database for NAICS data storage.
    3. Static Web Interface: Single-page application (index.html) for user interaction.

Database Schema:
    The DuckDB database stores NAICS data with the following structure:
    - index (UBIGINT, PRIMARY KEY): Sequential identifier for each NAICS entry
    - level (UTINYINT): Hierarchy level 2-6 (2=sector, 6=national industry)
    - code (VARCHAR): NAICS classification code (e.g., "511210")
    - title (VARCHAR): Official industry title
    - description (VARCHAR): Detailed industry description
    - excluded (VARCHAR): Activities excluded from this classification
    - examples (VARCHAR): Illustrative examples of businesses

Automatic Data Loading:
    On first startup, if the required data files don't exist, the application automatically:
    1. Runs the preprocessing pipeline (preprocess_data.py)
    2. Downloads raw NAICS data from the U.S. Census Bureau
    3. Cleans, normalizes, and consolidates the data
    4. Creates naics_descriptions.parquet with processed data
    5. Loads the data into a DuckDB database (naics_data.db)
    
    Subsequent startups use the existing database for instant loading.

API Endpoints:
    GET /api/data: Query and filter NAICS data
        - Supports filtering by level, code prefix, and regex search
        - Returns up to 100 matching entries
        - Query parameters: level (int), code (str), search (str)
    
    PUT /api/data/{item_index}: Update a NAICS entry
        - Partial updates supported (only provided fields are modified)
        - Returns 204 No Content on success
    
    POST /api/export_and_shutdown: Export all data to naics_descriptions.parquet and shutdown
        - Exports complete database to parquet file
        - Triggers controlled shutdown sequence
    
    POST /api/shutdown: Gracefully shut down the server
        - Triggers controlled shutdown sequence
        - Returns confirmation message

Web Interface:
    The static web interface (static/index.html) provides:
    - Real-time search and filtering with debounced input
    - Inline editing of NAICS fields (click any field to edit)
    - Visual feedback for save operations (success/error states)
    - Responsive design with Tailwind CSS
    - "Edits Complete" button to export final data
    - Graceful server shutdown button

Thread Safety:
    Database operations are protected by a threading lock (db_lock) to ensure
    thread-safe concurrent access in the Uvicorn async environment.

Usage:
    Start the server:
        $ python main.py
    
    Or with uvicorn directly:
        $ uvicorn main:app --reload --host 127.0.0.1 --port 8000
    
    Access the web interface:
        http://127.0.0.1:8000
    
    Query the API:
        http://127.0.0.1:8000/api/data?level=2
        http://127.0.0.1:8000/api/data?code=51&search=software

Example Workflow:
    1. Start application (preprocessing runs automatically if needed)
    2. Open web browser to http://127.0.0.1:8000
    3. Filter by level (e.g., "2" for sectors) or code (e.g., "51" for Information)
    4. Click any field in a NAICS card to edit inline
    5. Changes save automatically when you click away (blur event)
    6. Visual feedback shows save success (blue flash) or error (red flash)
    7. Click "Edits Complete" when done to export data and shutdown
    8. Or click "Shutdown Server" to shutdown without exporting

Dependencies:
    - FastAPI: Web framework and API routing
    - Uvicorn: ASGI server for running FastAPI
    - DuckDB: Embedded analytical database
    - Pandas: Data manipulation for loading parquet files
    - Polars: Data processing in preprocessing pipeline (via preprocess_data.py)
'''


# -------------------------------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------------------------------

import os
import signal
import threading
from contextlib import closing

import duckdb
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from preprocess_data import Config, preprocess_naics_data

# -------------------------------------------------------------------------------------------------
# Globals
# -------------------------------------------------------------------------------------------------

db_lock = threading.Lock()
DB_FILE = 'naics_data.db'
PARQUET_FILE = 'db_input.parquet'
SHUTDOWN_EVENT = threading.Event()


# -------------------------------------------------------------------------------------------------
# Pydantic Models for Data Validation
# -------------------------------------------------------------------------------------------------

class NaicsEntry(BaseModel):

    '''Represents a single NAICS entry for API responses.
    
    Attributes:
        index: Unique sequential identifier for the NAICS entry.
        level: Hierarchical level (2-6 digits) of the NAICS code.
        code: The NAICS classification code.
        title: Official title of the NAICS classification.
        description: Detailed description of the industry classification.
        excluded: Activities explicitly excluded from this classification.
        examples: Illustrative examples of businesses in this classification.

    '''
    
    index: int
    level: int | None = None
    code: str | None = None
    title: str | None = None
    description: str | None = None
    excluded: str | None = None
    examples: str | None = None


class NaicsUpdate(BaseModel):

    '''Represents the data structure for updating a NAICS entry.
    
    All fields are optional to allow partial updates of NAICS entries.
    
    Attributes:
        level: Hierarchical level (2-6 digits) of the NAICS code.
        code: The NAICS classification code.
        title: Official title of the NAICS classification.
        description: Detailed description of the industry classification.
        excluded: Activities explicitly excluded from this classification.
        examples: Illustrative examples of businesses in this classification.
    '''

    level: int | None = Field(None)
    code: str | None = Field(None)
    title: str | None = Field(None)
    description: str | None = Field(None)
    excluded: str | None = Field(None)
    examples: str | None = Field(None)


# -------------------------------------------------------------------------------------------------
# FastAPI App Initialization
# -------------------------------------------------------------------------------------------------

app = FastAPI()


# -------------------------------------------------------------------------------------------------
# Database Setup
# -------------------------------------------------------------------------------------------------

@app.on_event('startup')
def startup_event():

    '''Initialize the database on application startup.
    
    This function performs the following initialization steps:
    
    1. Checks if the parquet data file exists; if not, runs preprocessing
       to download and prepare NAICS data from the U.S. Census Bureau.
    2. Checks if the database file exists; if not, creates the table structure.
    3. Loads the initial data from the parquet file into the database.
    4. Deletes the parquet file after successful database creation to save disk space.
    
    The database uses DuckDB with the following schema:
    - index: Primary key (UBIGINT)
    - level: NAICS hierarchy level 2-6 (UTINYINT)
    - code: NAICS classification code (VARCHAR)
    - title: Official title (VARCHAR)
    - description: Detailed description (VARCHAR)
    - excluded: Excluded activities (VARCHAR)
    - examples: Illustrative examples (VARCHAR)
    
    Raises:
        Exception: If database creation or data loading fails.
    '''

    # Check if parquet file exists; if not, run preprocessing
    if not os.path.exists(PARQUET_FILE):
        print(f"Parquet file '{PARQUET_FILE}' not found.")
        print("Running preprocessing to download and prepare NAICS data...")
        try:
            config = Config()
            preprocess_naics_data(config)
            print(f"Successfully created '{PARQUET_FILE}'")

        except Exception as e:
            print(f"Error during preprocessing: {e}")
            raise
    
    db_file_exists = os.path.exists(DB_FILE)
    with db_lock:
        with closing(duckdb.connect(DB_FILE)) as con:
            print(f'Connected to database: {DB_FILE}')

            if not db_file_exists:
                print('Database file not found. Creating and populating table...')
                try:
                    con.execute('''
                        CREATE TABLE naics (
                            index UBIGINT PRIMARY KEY,
                            level UTINYINT,
                            code VARCHAR,
                            title VARCHAR,
                            description VARCHAR,
                            excluded VARCHAR,
                            examples VARCHAR
                        );
                    ''')
                    if os.path.exists(PARQUET_FILE):
                        df = pd.read_parquet(PARQUET_FILE)
                        con.register('df_view', df)
                        con.execute('INSERT INTO naics SELECT * FROM df_view')
                        print(f'Successfully loaded {len(df)} rows from {PARQUET_FILE}.')
                        # Delete the parquet file after successful database creation
                        os.remove(PARQUET_FILE)
                        print(f'Deleted temporary file: {PARQUET_FILE}')
                    else:
                        print(f"Warning: '{PARQUET_FILE}' not found. Database will be empty.")
                except Exception as e:
                    print(f'Error during initial database setup: {e}')
                    if os.path.exists(DB_FILE):
                        os.remove(DB_FILE)
                    raise
            else:
                print('Existing database file found. Skipping data load.')


@app.on_event('shutdown')
def shutdown_event():
    
    '''Perform cleanup operations on application shutdown.
    
    Currently logs shutdown status. Can be extended for resource cleanup
    such as closing database connections or flushing caches.
    '''

    print('FastAPI application is shutting down.')

# -------------------------------------------------------------------------------------------------
# API Endpoints
# -------------------------------------------------------------------------------------------------

@app.get('/api/data', response_model=list[NaicsEntry])
def get_data(level: int = None, code: str = None, search: str = None):

    '''Fetch and filter NAICS data from the database.
    
    Supports filtering by hierarchical level, code prefix matching, and
    full-text regex search across title, description, examples, and exclusions.
    Results are limited to 100 entries for performance.
    
    Args:
        level: Filter by NAICS hierarchy level (2-6). Optional.
        code: Filter by code prefix (e.g., "31" matches "31", "311", "3112"). Optional.
        search: Regex pattern to search across all text fields. Optional.
    
    Returns:
        List of NaicsEntry objects matching the filter criteria.
        Returns empty list if no filters are provided.
    
    Raises:
        HTTPException: 500 error if database query fails or unexpected error occurs.
    
    Examples:
        - `/api/data?level=2` - Get all 2-digit sector codes
        - `/api/data?code=51` - Get all codes starting with "51"
        - `/api/data?search=software` - Search for "software" in all fields
        - `/api/data?level=6&code=51&search=publisher` - Combined filters
    '''

    query_parts = []
    params = []

    if level is not None:
        query_parts.append('level = ?')
        params.append(level)
    if code:
        query_parts.append("code LIKE ?")
        params.append(f'{code}%')
    if search:
        search_condition = '''
            (regexp_matches(title, ?) OR
             regexp_matches(description, ?) OR
             regexp_matches(examples, ?) OR
             regexp_matches(excluded, ?))
        '''
        query_parts.append(search_condition)
        params.extend([search] * 4)

    if not query_parts:
        return []

    base_query = 'SELECT * FROM naics WHERE ' + ' AND '.join(query_parts) + ' LIMIT 100'

    try:
        with db_lock:
            with closing(duckdb.connect(DB_FILE, read_only=True)) as con:
                results = con.execute(base_query, params).fetchdf()
        return results.to_dict('records')
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=f'Database query failed: {e}')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'An unexpected error occurred: {e}')


@app.put('/api/data/{item_index}', status_code=204)
def update_data(item_index: int, item: NaicsUpdate):
 
    '''Update an existing NAICS entry in the database.
    
    Performs a partial update of the NAICS entry identified by its index.
    Only fields provided in the request body will be updated; unset fields
    are left unchanged.
    
    Args:
        item_index: The unique index of the NAICS entry to update.
        item: NaicsUpdate object containing fields to update.
    
    Returns:
        HTTP 204 No Content on successful update.
    
    Raises:
        HTTPException: 
            - 400 if no fields are provided for update.
            - 404 if the specified item_index does not exist.
            - 500 if the database update operation fails.
    
    Examples:
        PUT /api/data/123 with body:
        ```json
        {
            "description": "Updated description text",
            "examples": "New example 1, New example 2"
        }
        ```
    '''
 
    update_fields = item.dict(exclude_unset=True)
    if not update_fields:
        raise HTTPException(status_code=400, detail='No fields to update.')

    set_clauses = [f'{key} = ?' for key in update_fields.keys()]
    params = list(update_fields.values())
    params.append(item_index)
    query = f"UPDATE naics SET {', '.join(set_clauses)} WHERE index = ?"

    try:
        with db_lock:
            with closing(duckdb.connect(DB_FILE)) as con:
                result = con.execute(query, params)
                if result.fetchone()[0] == 0:
                    raise HTTPException(status_code=404, detail=f'Item with index {item_index} not found.')
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=f'Database update failed: {e}')


@app.post('/api/export_and_shutdown')
def export_and_shutdown():

    '''Export all NAICS data to parquet file and gracefully shut down the server.
    
    This endpoint performs the following actions:
    1. Queries all data from the DuckDB database
    2. Exports the data to naics_descriptions.parquet with the same schema as db_input.parquet
    3. Triggers server shutdown sequence
    
    Returns:
        Dict with export and shutdown confirmation message.
    
    Raises:
        HTTPException: 500 error if export fails.
    
    Note:
        This endpoint is called when the user clicks "Edits Complete" button,
        signaling that all manual edits are finished and the final data should be saved.
    '''

    try:
        print('Export and shutdown request received.')
        
        # Export all data from database to parquet
        with db_lock:
            with closing(duckdb.connect(DB_FILE, read_only=True)) as con:
                df = con.execute('SELECT * FROM naics ORDER BY index').fetchdf()
        
        # Save to parquet file
        df.to_parquet('naics_descriptions.parquet', index=False)
        print(f'Successfully exported {len(df)} rows to naics_descriptions.parquet')
        
        # Trigger shutdown
        print('Terminating server.')
        SHUTDOWN_EVENT.set()
        threading.Timer(1, _perform_shutdown).start()
        
        return {'message': 'Data exported successfully. Server is shutting down.'}
        
    except Exception as e:
        print(f'Error during export: {e}')
        raise HTTPException(status_code=500, detail=f'Failed to export data: {e}')


@app.post('/api/shutdown')
def shutdown_server():

    '''Gracefully shut down the Uvicorn server process.
    
    Triggers a controlled shutdown sequence:
    1. Sets the SHUTDOWN_EVENT flag to signal shutdown
    2. Schedules shutdown execution after 1 second delay
    3. Sends SIGINT to parent process to stop uvicorn --reload
    
    Returns:
        Dict with shutdown confirmation message.
    
    Note:
        This endpoint is designed to work with uvicorn's --reload mode.
        The parent process (reload watcher) will be terminated, stopping
        the entire application gracefully.
    '''

    print('Shutdown request received. Terminating server.')
    SHUTDOWN_EVENT.set()
    threading.Timer(1, _perform_shutdown).start()
    return {'message': 'Server is shutting down.'}


# Send SIGINT to parent process to stop uvicorn
def _perform_shutdown():
    
    parent_pid = os.getppid()
    os.kill(parent_pid, signal.SIGINT)


# -------------------------------------------------------------------------------------------------
# Static Files Mount
# -------------------------------------------------------------------------------------------------

app.mount('/', StaticFiles(directory='static', html=True), name='static')


# -------------------------------------------------------------------------------------------------
# Main Execution Block
# -------------------------------------------------------------------------------------------------

if __name__ == '__main__':

    '''Run the FastAPI server with Uvicorn in reload mode.
    
    This block handles direct execution via 'python main.py' and implements
    graceful shutdown:
    
    1. Configures Uvicorn server on 127.0.0.1:8000 with reload enabled
    2. Starts server in a separate thread
    3. Waits for shutdown signal from /api/shutdown endpoint
    4. Gracefully stops the server when shutdown is triggered
    
    The server is accessible at http://127.0.0.1:8000 after startup.
    '''

    config = uvicorn.Config('main:app', host='127.0.0.1', port=8000, reload=True)
    server = uvicorn.Server(config)

    server_thread = threading.Thread(target=server.run)
    server_thread.start()

    # Wait for the shutdown event to be set by the /api/shutdown endpoint
    SHUTDOWN_EVENT.wait()
    
    # Gracefully stop the server
    server.should_exit = True
    server_thread.join()
    print('Uvicorn server has stopped.')