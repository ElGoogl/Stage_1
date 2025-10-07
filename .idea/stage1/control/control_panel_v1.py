from pathlib import Path
import random
import json
from crawler_v1 import download_book_v1
from JSON_Indexer.indexer import build_inverted_index, save_index
from metadata_sqlite import parse_gutenberg_metadata, store_metadata_in_db, create_metadata_table, get_metadata_from_db

# Paths for tracking state - V1 uses crawler_v1 and indexer_v1
CONTROL_PATH = Path("control")
DOWNLOADS = CONTROL_PATH / "v1_crawler_books.txt"      # V1 crawler progress
INDEXINGS = CONTROL_PATH / "v1_indexer_books.txt"      # V1 indexer progress
METADATA_STORED = CONTROL_PATH / "v1_metadata_books.txt"  # V1 metadata file

# Total range of available book IDs (Project Gutenberg goes up to ~70,000+)
TOTAL_BOOKS = 80000

# Initialize metadata database
create_metadata_table()


def load_state(file_path: Path):
    """Helper to load a state file (downloaded or indexed IDs)."""
    if file_path.exists():
        return set(file_path.read_text(encoding="utf-8").splitlines())
    return set()


def save_state(file_path: Path, book_id: str):
    """Helper to append a single ID to a state file."""
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(f"{book_id}\n")


def control_pipeline_step():
    """
    Controls the data pipeline:
    1. Checks which books are downloaded but not yet indexed.
    2. If found → index them AND store their metadata.
    3. If none → download a new one.
    """

    CONTROL_PATH.mkdir(parents=True, exist_ok=True)

    # Load current state
    downloaded = load_state(DOWNLOADS)
    indexed = load_state(INDEXINGS)
    metadata_stored = load_state(METADATA_STORED)

    ready_to_index = downloaded - indexed
    ready_for_metadata = downloaded - metadata_stored

    # If there are books ready to index
    if ready_to_index:
        book_id = ready_to_index.pop()
        print(f"[CONTROL] Scheduling book {book_id} for indexing...")

        # Run the indexer (this builds the index for all downloaded files)
        index = build_inverted_index()
        save_index(index)

        save_state(INDEXINGS, book_id)
        print(f"[CONTROL] Book {book_id} indexed successfully.")
        
        # Also extract and store metadata if not already done
        if book_id in ready_for_metadata:
            print(f"[CONTROL] Extracting metadata for book {book_id}...")
            try:
                # Load the JSON file to get header content
                json_path = Path("data_repository/datalake_v1") / f"{book_id}.json"
                if json_path.exists():
                    with open(json_path, "r", encoding="utf-8") as f:
                        book_data = json.load(f)
                    
                    # Extract header and parse metadata
                    header = book_data.get("header", "")
                    if header:
                        metadata_dict = parse_gutenberg_metadata(header)
                        if metadata_dict:
                            success = store_metadata_in_db(metadata_dict, book_id=book_id)
                            if success:
                                save_state(METADATA_STORED, book_id)
                                print(f"[CONTROL] Metadata stored for book {book_id}")
                                
                                # === DEBUG: Print stored metadata (comment out this section to disable) ===
                                try:
                                    retrieved_metadata = get_metadata_from_db(book_id)
                                    if retrieved_metadata:
                                        print(f"[DEBUG] 📖 Retrieved metadata for book {book_id}:")
                                        for key, value in retrieved_metadata.items():
                                            # Truncate long values for readability
                                            display_value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                                            print(f"[DEBUG]   {key}: {display_value}")
                                        print(f"[DEBUG] 📖 End metadata for book {book_id}")
                                    else:
                                        print(f"[DEBUG] ⚠️ Could not retrieve metadata for book {book_id}")
                                except Exception as debug_e:
                                    print(f"[DEBUG] ✗ Error retrieving metadata for display: {debug_e}")
                                # === END DEBUG SECTION ===
                                
                            else:
                                print(f"[CONTROL] Failed to store metadata for book {book_id}")
                        else:
                            print(f"[CONTROL] No metadata found in header for book {book_id}")
                    else:
                        print(f"[CONTROL] No header found for book {book_id}")
                else:
                    print(f"[CONTROL] JSON file not found for book {book_id}")
            except Exception as e:
                print(f"[CONTROL] Error extracting metadata for book {book_id}: {e}")
        
        return

    # Otherwise, download a new book
    print("[CONTROL] No books left to index — downloading a new one...")
    # Try 10 random ids
    for _ in range(10):
        candidate_id = str(random.randint(1, TOTAL_BOOKS))
        if candidate_id not in downloaded:
            print(f"[CONTROL] ↓ Downloading book {candidate_id} ...")
            success = download_book_v1(int(candidate_id))

            if success:
                save_state(DOWNLOADS, candidate_id)
                print(f"[CONTROL] Book {candidate_id} downloaded successfully.")
            else:
                print(f"[CONTROL] Book {candidate_id} could not be downloaded.")
            break
    else:
        print("[CONTROL] Could not find a new book ID after 10 attempts.")