# -------------------------------------------------------------------------------------------------
# Imports 
# -------------------------------------------------------------------------------------------------

import duckdb
import os
import signal
from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Optional


# -------------------------------------------------------------------------------------------------
# Pydantic model for data validation
# -------------------------------------------------------------------------------------------------

class Item(BaseModel):
    index: int
    level: Optional[int] = None
    code: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    excluded: Optional[str] = Field(None, alias='excluded')
    examples: Optional[str] = Field(None, alias='examples')

    class Config:
        populate_by_name = True # Allows using alias
        
app = FastAPI()


# -------------------------------------------------------------------------------------------------
# Connect to a file-based DuckDB database for persistence.
# -------------------------------------------------------------------------------------------------

con = duckdb.connect(database='naics.duckdb', read_only=False)

@app.on_event('startup')
def startup_event():

    '''
    On startup, checks if the 'naics' table exists. If not, it loads the
    initial data from the 'naics_descriptions.parquet' file.
    '''

    tables = con.execute('SHOW TABLES').fetchall()

    if ('naics',) not in tables:
        print("Table 'naics' not found. Loading initial data from parquet file...")
        try:
            con.execute("CREATE TABLE naics AS SELECT * FROM read_parquet('naics_descriptions.parquet');")
            print("Successfully created 'naics' table and loaded data.")
        except Exception as e:
            print(f'Error loading Parquet file: {e}')
            # Handle case where file might not be found
    else:
        print("Table 'naics' already exists. Skipping initial data load.")


# -------------------------------------------------------------------------------------------------
# API Endpoints
# -------------------------------------------------------------------------------------------------

@app.get('/api/data')
def get_data(level: Optional[int] = None, code: Optional[str] = None, search: Optional[str] = None):
    
    '''
    Fetches and filters data from the NAICS table.
    Supports filtering by level, code, and a regex search term.
    '''
    
    query = 'SELECT * FROM naics'
    filters = []
    params = []

    if level is not None:
        filters.append('level = ?')
        params.append(level)
    if code:
        filters.append('code LIKE ?')
        params.append(f'%{code}%')
    if search:
        # Regex search across multiple text fields
        search_clause = 'regexp_matches(title, ?) OR regexp_matches(description, ?) OR regexp_matches(examples, ?) OR regexp_matches(excluded, ?)'
        filters.append(f'({search_clause})')
        # Add the search parameter four times, once for each field
        params.extend([search] * 4)

    if filters:
        query += ' WHERE ' + ' AND '.join(filters)

    query += ' ORDER BY index'
    
    results = con.execute(query, params).fetchdf()
    
    return results.to_dict('records')

@app.put('/api/data/{index}')
async def update_item(item: Item, index: int = Path(..., title='The index of the item to update')):
    '''
    Updates an existing item in the database based on its index.
    '''
    if item.index != index:
        raise HTTPException(status_code=400, detail='Path index and item index do not match.')
    
    try:
        # Prepare the UPDATE statement
        con.execute(
            '''
            UPDATE naics 
            SET title = ?, description = ?, examples = ?, excluded = ?
            WHERE index = ?
            ''',
            [item.title, item.description, item.examples, item.excluded, index]
        )
        return {'status': 'success', 'message': f'Item {index} updated.'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Database error: {str(e)}')


@app.post('/api/shutdown')
async def shutdown_server():
    '''
    Gracefully shuts down the application by closing the database
    connection and then terminating the server process.
    '''
    print('Shutdown signal received. Closing database connection.')
    con.close()
    
    # Get the process ID of the current process and send a terminate signal
    pid = os.getpid()
    print(f'Server process (PID: {pid}) is shutting down.')
    os.kill(pid, signal.SIGTERM)
    
    return {'message': 'Server is shutting down.'}


# -------------------------------------------------------------------------------------------------
# Mount the 'static' directory to serve index.html
# -------------------------------------------------------------------------------------------------

app.mount('/static', StaticFiles(directory='static'), name='static')

@app.get('/', response_class=HTMLResponse)
async def read_root():
    '''
    Serves the main index.html file from the 'static' directory.
    '''
    with open('static/index.html') as f:
        return HTMLResponse(content=f.read(), status_code=200)

