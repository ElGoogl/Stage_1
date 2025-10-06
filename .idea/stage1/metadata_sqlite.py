import sqlite3
import json
import os

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def get_db_path(db_filename=None):
    """
    Get the full path to the database file in the data_repository directory.
    
    Args:
        db_filename (str, optional): Database filename. Defaults to 'gutenberg_metadata.db'
        
    Returns:
        str: Full path to database file in data_repository directory
    """
    if db_filename is None:
        db_filename = "gutenberg_metadata.db"
    
    # If it's already an absolute path, use it as-is
    if os.path.isabs(db_filename):
        return db_filename
    
    # Place it in the data_repository folder
    data_repo_dir = os.path.join(SCRIPT_DIR, "data_repository")
    os.makedirs(data_repo_dir, exist_ok=True)  # Create directory if it doesn't exist
    return os.path.join(data_repo_dir, db_filename)

def parse_gutenberg_metadata(text_content):
    """
    Parse Project Gutenberg eBook metadata starting from the first "Title:" keyword.
    
    Args:
        text_content (str): The text content of a Project Gutenberg eBook
        
    Returns:
        dict: Dictionary with metadata keys and values
        
    Example:
        Starting from "Title:" and parsing each line with ":" as key-value pairs
        until two consecutive empty lines are found.
    """
    result = {}
    
    try:
        lines = text_content.split('\n')
        
        # Find the first occurrence of "Title:"
        title_found = False
        empty_line_count = 0
        current_key = None
        current_value = []
        
        for line in lines:
            stripped_line = line.strip()
            
            # Look for the first "Title:" to start parsing
            if not title_found:
                if stripped_line.startswith("Title:"):
                    title_found = True
                    # Process this Title line
                    colon_pos = stripped_line.find(':')
                    current_key = "Title"
                    current_value = [stripped_line[colon_pos + 1:].strip()]
                continue
            
            # Once we found Title, start parsing metadata
            if title_found:
                # Check for two consecutive empty lines (end of metadata)
                if not stripped_line:
                    empty_line_count += 1
                    if empty_line_count >= 2:
                        # Save the last key-value pair and stop
                        if current_key and current_value:
                            result[current_key] = ' '.join(current_value).strip()
                        break
                else:
                    empty_line_count = 0  # Reset counter when we find non-empty line
                
                # Check if this line contains a colon (new key-value pair)
                if ':' in stripped_line:
                    # Save previous key-value pair if exists
                    if current_key and current_value:
                        result[current_key] = ' '.join(current_value).strip()
                    
                    # Start new key-value pair
                    colon_pos = stripped_line.find(':')
                    current_key = stripped_line[:colon_pos].strip()
                    current_value = [stripped_line[colon_pos + 1:].strip()]
                else:
                    # Continuation line for current key
                    if current_key and stripped_line:
                        current_value.append(stripped_line)
        
        # Don't forget the last key-value pair if we didn't hit two empty lines
        if current_key and current_value:
            result[current_key] = ' '.join(current_value).strip()
            
    except Exception as e:
        print(f"Error parsing text content: {e}")
        return {}
    
    return result


def create_metadata_table(db_path=None):
    """
    Create the metadata table in SQLite database if it doesn't exist.
    
    Args:
        db_path (str): Path to the SQLite database file
    """
    if db_path is None:
        db_path = get_db_path()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table with flexible schema to handle various metadata fields
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS book_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                book_id TEXT UNIQUE,
                title TEXT,
                author TEXT,
                release_date TEXT,
                language TEXT,
                credits TEXT,
                subject TEXT,
                loc_class TEXT,
                category TEXT,
                ebook_no TEXT,
                raw_metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        print(f"✅ Metadata table created/verified in {db_path}")
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")


def store_metadata_in_db(metadata_dict, db_path=None, book_id=None):
    """
    Store a metadata dictionary in SQLite database.
    Automatically adds new columns for any fields not already in the schema.
    
    Args:
        metadata_dict (dict): Dictionary with metadata key-value pairs
        db_path (str): Path to the SQLite database file
        book_id (str, optional): Unique book identifier. If not provided, uses Title as ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    if db_path is None:
        db_path = get_db_path()
    try:
        # Ensure table exists
        create_metadata_table(db_path)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get existing column names
        cursor.execute("PRAGMA table_info(book_metadata)")
        existing_columns = {row[1].lower() for row in cursor.fetchall()}
        
        # Helper function to create valid column names
        def sanitize_column_name(field_name):
            """Convert field names to valid SQLite column names."""
            # Replace spaces and special chars with underscores, convert to lowercase
            import re
            sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', field_name.lower())
            # Remove multiple consecutive underscores
            sanitized = re.sub(r'_+', '_', sanitized)
            # Remove leading/trailing underscores
            sanitized = sanitized.strip('_')
            return sanitized or 'unknown_field'
        
        # Check for new fields and add columns as needed
        new_columns_added = []
        for field_name in metadata_dict.keys():
            column_name = sanitize_column_name(field_name)
            
            if column_name not in existing_columns:
                try:
                    cursor.execute(f'ALTER TABLE book_metadata ADD COLUMN {column_name} TEXT')
                    existing_columns.add(column_name)
                    new_columns_added.append(field_name)
                    print(f"🆕 Added new column: {column_name} (from field: {field_name})")
                except sqlite3.OperationalError as e:
                    print(f"⚠️ Could not add column {column_name}: {e}")
        
        # Prepare data for insertion - now including all fields
        data = {'book_id': book_id or metadata_dict.get('Title') or f"unknown_{hash(str(metadata_dict))}"}
        
        # Add all metadata fields to data dict with sanitized column names
        for field_name, field_value in metadata_dict.items():
            column_name = sanitize_column_name(field_name)
            data[column_name] = field_value
        
        # Always store complete raw metadata as backup
        data['raw_metadata'] = json.dumps(metadata_dict)
        
        # Build dynamic INSERT statement
        columns = list(data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        column_names = ', '.join(columns)
        
        insert_sql = f'''
            INSERT OR REPLACE INTO book_metadata 
            ({column_names})
            VALUES ({placeholders})
        '''
        
        values = [data[col] for col in columns]
        cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
        
        success_msg = f"✅ Stored metadata for: {data.get('title') or data['book_id']}"
        if new_columns_added:
            success_msg += f" (added {len(new_columns_added)} new columns)"
        print(success_msg)
        return True
        
    except Exception as e:
        print(f"❌ Error storing metadata: {e}")
        return False


def get_metadata_from_db(book_id, db_path=None):
    """
    Retrieve metadata for a specific book from the database.
    
    Args:
        book_id (str): Book identifier
        db_path (str): Path to the SQLite database file
        
    Returns:
        dict: Metadata dictionary or None if not found
    """
    if db_path is None:
        db_path = get_db_path()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT raw_metadata FROM book_metadata WHERE book_id = ?', (book_id,))
        result = cursor.fetchone()
        
        conn.close()
        
        if result:
            return json.loads(result[0])
        return None
        
    except Exception as e:
        print(f"❌ Error retrieving metadata: {e}")
        return None





