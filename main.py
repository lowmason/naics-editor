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

# -------------------------------------------------------------------------------------------------
# Globals
# -------------------------------------------------------------------------------------------------

# Use a lock for database operations to ensure thread safety
db_lock = threading.Lock()
DB_FILE = 'naics_data.db'
# Use a threading Event to signal shutdown across the application
SHUTDOWN_EVENT = threading.Event()

# -------------------------------------------------------------------------------------------------
# Pydantic Models for Data Validation
# -------------------------------------------------------------------------------------------------

class NaicsEntry(BaseModel):
    '''Represents a single NAICS entry for API responses.'''
    index: int
    level: int | None = None
    code: str | None = None
    title: str | None = None
    description: str | None = None
    excluded: str | None = None
    examples: str | None = None

class NaicsUpdate(BaseModel):
    '''Represents the data structure for updating a NAICS entry.'''
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

    '''
    Initialize the database on application startup.
    Checks if the database file exists. If not, it creates the table and
    loads the initial data from the 'naics_descriptions.parquet' file.
    '''
    
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
                    parquet_file = 'naics_descriptions.parquet'
                    
                    if os.path.exists(parquet_file):
                        df = pd.read_parquet(parquet_file)
                        con.register('df_view', df)
                        con.execute('INSERT INTO naics SELECT * FROM df_view')
                        print(f'Successfully loaded {len(df)} rows from {parquet_file}.')
                    
                    else:
                        print(f"Warning: '{parquet_file}' not found. Database will be empty.")

                except Exception as e:
                    print(f'Error during initial database setup: {e}')
                    os.remove(DB_FILE)

            else:
                print('Existing database file found. Skipping data load.')

@app.on_event('shutdown')
def shutdown_event():
    '''Placeholder for any cleanup on shutdown.'''
    print('FastAPI application is shutting down.')


# -------------------------------------------------------------------------------------------------
# API Endpoints
# -------------------------------------------------------------------------------------------------

@app.get('/api/data', response_model=list[NaicsEntry])
def get_data(level: int = None, code: str = None, search: str = None):

    '''
    Fetches and filters NAICS data from the database.
    Supports filtering by level, code prefix, and a full-text search term.
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

    '''Updates an existing NAICS entry in the database based on its index.'''

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


@app.post('/api/shutdown')
def shutdown_server():

    '''Gracefully shuts down the Uvicorn server process.'''

    print('Shutdown request received. Terminating server.')
    SHUTDOWN_EVENT.set()
    threading.Timer(1, _perform_shutdown).start()
    return {'message': 'Server is shutting down.'}


def _perform_shutdown():

    '''Sends an interrupt signal to the parent process to stop uvicorn --reload.'''


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

    '''
    This block allows running the server directly with 'python main.py'
    and handles the graceful shutdown mechanism.
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

