import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from contextlib import asynccontextmanager

DB_PATH = 'naics_data.db'

@asynccontextmanager
async def lifespan(app: FastAPI):
    # On startup, connect to the database and create tables if they don't exist
    print("Connecting to database...")
    con = duckdb.connect(DB_PATH)
    
    # Create main NAICS table if it doesn't exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS naics (
            code TEXT,
            title TEXT,
            description TEXT,
            parent_code TEXT,
            parent_title TEXT
        );
    """)
    
    # Create FTS index on the main table
    try:
        con.execute("PRAGMA create_fts_index('naics', 'code', 'title', 'description', overwrite=1);")
    except duckdb.Exception as e:
        print(f"Could not create FTS index (might already exist): {e}")

    # NEW: Create the edits table to store user changes
    con.execute("""
        CREATE TABLE IF NOT EXISTS naics_edits (
            code TEXT PRIMARY KEY,
            description TEXT
        );
    """)
    
    con.close()
    print("Database connection closed.")
    yield
    # On shutdown (nothing needed for this app)
    print("Application shutting down.")

app = FastAPI(lifespan=lifespan)

class DescriptionUpdate(BaseModel):
    code: str
    description: str

def get_db_connection():
    """Helper function to get a database connection."""
    return duckdb.connect(DB_PATH, read_only=False)

@app.post("/update_description")
async def update_description(update: DescriptionUpdate):
    """
    Updates the description for a given NAICS code.
    This now saves the change to a separate 'naics_edits' table.
    """
    if not update.code or update.description is None:
        raise HTTPException(status_code=400, detail="Invalid request payload")

    try:
        with get_db_connection() as con:
            # Use INSERT OR REPLACE (UPSERT) to add or update the edit
            con.execute(
                "INSERT OR REPLACE INTO naics_edits (code, description) VALUES (?, ?)",
                [update.code, update.description]
            )
        return {"status": "success", "code": update.code}
    except Exception as e:
        print(f"Error updating description: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

@app.get("/search")
async def search_naics(q: str = Query(..., min_length=1)):
    """
    Performs a full-text search.
    Results now reflect user edits by joining 'naics' and 'naics_edits'.
    """
    query = """
        SELECT
            n.code,
            n.title,
            -- Use the edited description if it exists, otherwise use the original
            COALESCE(e.description, n.description) AS description,
            fts_main_naics.score
        FROM fts_main_naics
        JOIN naics AS n ON fts_main_naics.rowid = n.rowid
        LEFT JOIN naics_edits AS e ON n.code = e.code
        WHERE fts_main_naics.score IS NOT NULL
        ORDER BY fts_main_naics.score DESC
        LIMIT 50;
    """
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            # Pass the search query 'q' as a parameter to the FTS function
            results = con.execute(query, [q]).fetchall()
        
        # Convert tuples to dictionaries
        return [
            {"code": r[0], "title": r[1], "description": r[2], "score": r[3]}
            for r in results
        ]
    except Exception as e:
        print(f"Error during search: {e}")
        raise HTTPException(status_code=500, detail=f"Search error: {e}")

@app.get("/browse")
async def browse_naics(code: str = Query("", alias="code")):
    """
    Browses the NAICS hierarchy.
    Results now reflect user edits by joining 'naics' and 'naics_edits'.
    """
    # ... existing code ...
    if not code:
        # Root level (2-digit sectors)
        query_code = "LENGTH(n.code) = 2"
    elif len(code) < 6:
        # Children of the current code
        query_code = f"n.parent_code = '{code}'"
    else:
        # At the lowest level (6-digit), just show the item itself
        query_code = f"n.code = '{code}'"

    query = f"""
        SELECT
            n.code,
            n.title,
            -- Use the edited description if it exists, otherwise use the original
            COALESCE(e.description, n.description) AS description,
            n.parent_code,
            n.parent_title,
            -- Check if children exist (for UI drill-down)
            EXISTS (SELECT 1 FROM naics child WHERE child.parent_code = n.code) AS has_children
        FROM naics AS n
        LEFT JOIN naics_edits AS e ON n.code = e.code
        WHERE {query_code}
        ORDER BY n.code;
    """
    
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            results = con.execute(query).fetchall()
        
        # ... existing code ...
        return [
            {
                "code": r[0], 
                "title": r[1], 
                "description": r[2], 
                "parent_code": r[3],
                "parent_title": r[4],
                "has_children": r[5]
            }
            for r in results
        ]
    except Exception as e:
        print(f"Error during browse: {e}")
        raise HTTPException(status_code=500, detail=f"Browse error: {e}")

# Mount the static directory to serve index.html
app.mount("/", StaticFiles(directory="static", html=True), name="static")
