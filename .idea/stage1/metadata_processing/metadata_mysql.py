import mysql.connector
import re

DEBUG = False

DB_CONFIG = {
    "host": "localhost",
    "user": "pythonuser",     
    "password": "twoje_haslo",
    "database": "gutenberg"
}

def get_connection():
    conn = mysql.connector.connect(**DB_CONFIG)
    return conn


def sanitize_column_name(field_name):
    """Convert field name to valid MySQL column name."""
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', field_name.lower())
    sanitized = re.sub(r'_+', '_', sanitized)
    return sanitized.strip('_') or 'unknown_field'


def parse_gutenberg_metadata(text_content):

    result = {}
    try:
        lines = text_content.split('\n')
        title_found = False
        empty_line_count = 0
        current_key = None
        current_value = []
        
        for line in lines:
            stripped_line = line.strip()
            if not title_found:
                if stripped_line.startswith("Title:"):
                    title_found = True
                    colon_pos = stripped_line.find(':')
                    current_key = "Title"
                    current_value = [stripped_line[colon_pos + 1:].strip()]
                continue
            
            if title_found:
                if not stripped_line:
                    empty_line_count += 1
                    if empty_line_count >= 2:
                        if current_key and current_value:
                            result[current_key] = ' '.join(current_value).strip()
                        break
                else:
                    empty_line_count = 0
                
                if ':' in stripped_line:
                    if current_key and current_value:
                        result[current_key] = ' '.join(current_value).strip()
                    colon_pos = stripped_line.find(':')
                    current_key = stripped_line[:colon_pos].strip()
                    current_value = [stripped_line[colon_pos + 1:].strip()]
                else:
                    if current_key and stripped_line:
                        current_value.append(stripped_line)
        
        if current_key and current_value:
            result[current_key] = ' '.join(current_value).strip()
    except Exception as e:
        print(f"Error parsing text content: {e}")
        return {}
    
    return result


def create_metadata_table():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS book_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                book_id VARCHAR(255) UNIQUE,
                title TEXT,
                author TEXT,
                release_date TEXT,
                language TEXT,
                credits TEXT,
                subject TEXT,
                loc_class TEXT,
                category TEXT,
                ebook_no TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        print("✅ Metadata table created/verified")

    except Exception as e:
        print(f"❌ Error creating table: {e}")


def store_metadata_in_db(metadata_dict, book_id=None):
    try:
        create_metadata_table()
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SHOW COLUMNS FROM book_metadata")
        existing_columns = {row[0].lower() for row in cursor.fetchall()}

        new_columns = []
        for field_name in metadata_dict.keys():
            column_name = sanitize_column_name(field_name)
            if column_name not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE book_metadata ADD COLUMN {column_name} TEXT")
                    existing_columns.add(column_name)
                    new_columns.append(field_name)
                    if DEBUG:
                        print(f"🆕 Added new column: {column_name} (from field: {field_name})")
                except Exception as e:
                    print(f"Could not add column {column_name}: {e}")

        data = {'book_id': book_id or metadata_dict.get('Title') or f"unknown_{hash(str(metadata_dict))}"}
        for field_name, field_value in metadata_dict.items():
            data[sanitize_column_name(field_name)] = field_value

        columns = list(data.keys())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)
        update_clause = ', '.join([f"{col} = VALUES({col})" for col in columns])

        insert_sql = f"""
            INSERT INTO book_metadata ({column_names})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        values = [data[col] for col in columns]

        cursor.execute(insert_sql, values)
        conn.commit()
        conn.close()

        title = data.get('title') or data['book_id']
        msg = f"✅ Stored metadata for: {title}"
        if new_columns:
            msg += f" (added {len(new_columns)} new columns)"
        print(msg)
        return True

    except Exception as e:
        print(f"Error storing metadata: {e}")
        return False


def get_metadata_from_db(book_id):
    """Retrieve metadata for a book from database using structured columns."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get all columns except system columns
        cursor.execute("SHOW COLUMNS FROM book_metadata")
        columns = [row[0] for row in cursor.fetchall() 
                  if row[0] not in ['id', 'book_id', 'raw_metadata', 'created_at']]
        
        if not columns:
            conn.close()
            return None
            
        # Build dynamic query for all metadata columns
        cols_str = ', '.join(columns)
        cursor.execute(f'SELECT {cols_str} FROM book_metadata WHERE book_id = %s', (book_id,))
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return None
            
        # Reconstruct metadata dictionary
        metadata = {}
        for i, col in enumerate(columns):
            if result[i] is not None:
                # Convert column name back to original field name
                original_field = col.replace('_', ' ').title()
                metadata[original_field] = result[i]
                
        return metadata if metadata else None
        
    except Exception as e:
        print(f"❌ Retrieval error: {e}")
        return None

def search_books(keyword, value):
    """Search for book IDs by keyword and value using structured columns."""
    book_ids = []
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Convert keyword to column name
        column_name = sanitize_column_name(keyword)
        
        # Check if column exists
        cursor.execute("SHOW COLUMNS FROM book_metadata")
        existing_columns = {row[0].lower() for row in cursor.fetchall()}
        
        if column_name in existing_columns:
            # Direct SQL search on the specific column
            sql = f"SELECT book_id FROM book_metadata WHERE {column_name} LIKE %s AND {column_name} IS NOT NULL"
            cursor.execute(sql, (f'%{value}%',))
            book_ids = [row[0] for row in cursor.fetchall()]
        else:
            print(f"⚠️ Column '{column_name}' (from keyword '{keyword}') not found in database")
        
        conn.close()
                
    except Exception as e:
        print(f"❌ Search error: {e}")
    
    return book_ids


def list_all_books():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT book_id, title, author FROM book_metadata ORDER BY title")
        results = cursor.fetchall()
        conn.close()
        return results
    except Exception as e:
        print(f"Error listing books: {e}")
        return []
