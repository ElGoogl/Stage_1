from crawler_v1 import download_book
from crawler_v2 import download_book_v2
from indexer import build_inverted_index, save_index
from metadata_sqlite import store_metadata_in_db, parse_gutenberg_metadata, get_metadata_from_db
import os
import glob

if __name__ == "__main__":
    # example book id list
    book_ids = [76921, 76922, 76947, 20345]

    # Crawler v1 thats creates the Datalake v1 with Metadata + content JSON files
    for book_id in book_ids:
        download_book(book_id)

    index = build_inverted_index()
    save_index(index)

    #Crawler v2 thats creates the datalake v2 with categorized subfolders and txt files for metadata and content
    for book_id in book_ids:
        download_book_v2(book_id)

    # extract and save metadata in an sqlite database
    raw_v2_path = os.path.join("data_repository", "raw_v2")
    
    # Find all *_header.txt files in all subdirectories of raw_v2
    header_files = glob.glob(os.path.join(raw_v2_path, "**", "*_header.txt"), recursive=True)
    
    print(f"Found {len(header_files)} header files to process")
    
    for header_file_path in header_files:
        try:
            # Extract book ID from filename (e.g., "76921_header.txt" -> "76921")
            filename = os.path.basename(header_file_path)
            book_id = filename.split("_")[0]
            
            print(f"Processing {filename}...")
            
            # Read the header file content
            with open(header_file_path, 'r', encoding='utf-8') as file:
                header_content = file.read()
            
            # Parse metadata from the header content
            metadata = parse_gutenberg_metadata(header_content)
            
            if metadata:
                # Store in SQLite database using book_id as identifier
                success = store_metadata_in_db(metadata, book_id=f"{book_id}")
                if success:
                    print(f"✅ Successfully stored metadata for book {book_id}")
                    #print(get_metadata_from_db(f"{book_id}"))
                else:
                    print(f"❌ Failed to store metadata for book {book_id}")
            else:
                print(f"⚠️ No metadata extracted from {filename}")
                
        except Exception as e:
            print(f"❌ Error processing {header_file_path}: {e}")
    
    print(f"\n🎉 Finished processing all header files!")

