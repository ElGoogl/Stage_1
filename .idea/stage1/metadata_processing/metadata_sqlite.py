#!/usr/bin/env python3
"""Simplified SQLite metadata handler for Project Gutenberg books."""

import sqlite3, json, os, re

def get_db_path(db_filename=None):
    """Get database path in data_repository directory."""
    db_filename = db_filename or "metadata_sqlite.db"
    if os.path.isabs(db_filename): return db_filename
    
    # Get the project root (go up from metadata_processing to stage1, then to data_repository)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)  # Go up to stage1
    data_dir = os.path.join(project_root, "data_repository")
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, db_filename)

def parse_gutenberg_metadata(text):
    """Parse Project Gutenberg metadata from text."""
    result = {}
    try:
        lines = text.split('\n')
        title_found = False
        empty_count = 0
        key, value = None, []
        
        for line in lines:
            line = line.strip()
            
            # Find first "Title:" to start parsing
            if not title_found:
                if line.startswith("Title:"):
                    title_found = True
                    key, value = "Title", [line[line.find(':') + 1:].strip()]
                continue
            
            # Stop at two empty lines
            if not line:
                empty_count += 1
                if empty_count >= 2:
                    if key and value: result[key] = ' '.join(value).strip()
                    break
            else:
                empty_count = 0
            
            # New key-value pair or continuation
            if ':' in line:
                if key and value: result[key] = ' '.join(value).strip()
                colon_pos = line.find(':')
                key, value = line[:colon_pos].strip(), [line[colon_pos + 1:].strip()]
            elif key and line:
                value.append(line)
        
        # Save last pair
        if key and value: result[key] = ' '.join(value).strip()
        
    except Exception as e:
        print(f"Parse error: {e}")
        return {}
    
    return result

def sanitize_column(name):
    """Convert field name to valid SQLite column name."""
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')
    return sanitized or 'unknown'

def create_metadata_table(db_path=None):
    """Create metadata table if it doesn't exist."""
    db_path = db_path or get_db_path()
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS book_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                book_id TEXT UNIQUE,
                title TEXT, author TEXT, release_date TEXT, language TEXT,
                credits TEXT, subject TEXT, loc_class TEXT, category TEXT,
                ebook_no TEXT, raw_metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )''')
        print(f"✅ Metadata table created/verified in {db_path}")
    except Exception as e:
        print(f"❌ Table creation error: {e}")

def store_metadata_in_db(metadata, db_path=None, book_id=None):
    """Store metadata in database with dynamic column creation."""
    db_path = db_path or get_db_path()
    try:
        create_metadata_table(db_path)
        
        with sqlite3.connect(db_path) as conn:
            # Get existing columns
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(book_metadata)")
            existing = {row[1].lower() for row in cursor.fetchall()}
            
            # Add new columns as needed
            new_cols = []
            for field in metadata.keys():
                col = sanitize_column(field)
                if col not in existing:
                    try:
                        cursor.execute(f'ALTER TABLE book_metadata ADD COLUMN {col} TEXT')
                        existing.add(col)
                        new_cols.append(field)
                        print(f"🆕 Added new column: {col} (from field: {field})")
                    except sqlite3.OperationalError as e:
                        print(f"⚠️ Could not add column {col}: {e}")
            
            # Prepare data
            data = {'book_id': book_id or metadata.get('Title') or f"unknown_{hash(str(metadata))}"}
            for field, value in metadata.items():
                data[sanitize_column(field)] = value
            data['raw_metadata'] = json.dumps(metadata)
            
            # Insert data
            cols = list(data.keys())
            placeholders = ', '.join(['?' for _ in cols])
            sql = f"INSERT OR REPLACE INTO book_metadata ({', '.join(cols)}) VALUES ({placeholders})"
            cursor.execute(sql, [data[col] for col in cols])
            
            title = data.get('title') or data['book_id']
            msg = f"✅ Stored metadata for: {title}"
            if new_cols: msg += f" (added {len(new_cols)} new columns)"
            print(msg)
            return True
            
    except Exception as e:
        print(f"❌ Storage error: {e}")
        return False

def get_metadata_from_db(book_id, db_path=None):
    """Retrieve metadata for a book from database."""
    db_path = db_path or get_db_path()
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT raw_metadata FROM book_metadata WHERE book_id = ?', (book_id,))
            result = cursor.fetchone()
            return json.loads(result[0]) if result else None
    except Exception as e:
        print(f"❌ Retrieval error: {e}")
        return None

def search_books(keyword, value, db_path=None):
    """Search for book IDs by keyword and value."""
    db_path = db_path or get_db_path()
    book_ids = []
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT book_id, raw_metadata FROM book_metadata WHERE raw_metadata IS NOT NULL')
            
            for row in cursor.fetchall():
                try:
                    metadata = json.loads(row[1])
                    if keyword in metadata and value.lower() in str(metadata[keyword]).lower():
                        book_ids.append(row[0])
                except:
                    continue
                    
    except Exception as e:
        print(f"❌ Search error: {e}")
    
    return book_ids