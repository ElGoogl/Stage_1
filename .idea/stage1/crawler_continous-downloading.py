# by chat gpt

import requests
import os
import time
import json
import sys

# Use script directory as working directory - all paths relative to where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)  # Change working directory to script location

# Import inverted_index from the same directory
from inverted_index import InvertedIndex

# === CONFIGURATION ===
# All paths relative to script directory
OUTPUT_DIR = "gutenberg_books"
STATE_FILE = os.path.join(OUTPUT_DIR, "last_id.txt")  # moved inside gutenberg_books folder
CATALOG_FILE = "metadata_catalog.json"  # stores all metadata (inside gutenberg_books)
INDEX_FILE = "gutenberg_inverted_index.json"  # inverted index file (inside gutenberg_books)
BOOKS_PER_RUN = 5                   # how many books to fetch each run
SLEEP_TIME = 2                      # polite delay between requests (seconds)


def get_book_metadata(book_id):
    """Fetch metadata from Gutendex by book ID."""
    url = f"https://gutendex.com/books/{book_id}"
    try:
        resp = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed for ID {book_id}: {e}")
        return None

    if resp.status_code != 200:
        return None

    try:
        data = resp.json()
    except json.JSONDecodeError:
        return None

    if "detail" in data:  # Not found
        return None

    return data


def download_book(book, output_dir, inverted_index):
    """Update inverted index with book content and return metadata dict (no text file storage)."""
    book_id = book.get("id")
    title = book.get("title", f"book_{book_id}").replace("/", "-")
    authors = ", ".join(a["name"] for a in book.get("authors", [])) or "Unknown"
    language = ", ".join(book.get("languages", []))

    # Try to find a plain text format
    formats = book.get("formats", {})
    text_url = (
            formats.get("text/plain; charset=utf-8")
            or formats.get("text/plain; charset=us-ascii")
            or formats.get("text/plain")
    )

    # Build clean metadata dict
    metadata = {
        "id": book_id,
        "title": title,
        "authors": authors,
        "language": language,
        "download_url": text_url,
    }

    os.makedirs(output_dir, exist_ok=True)

    # Download text and update inverted index (without saving text file)
    if text_url:
        try:
            resp = requests.get(text_url, timeout=20)
            if resp.status_code == 200:
                print(f"📖 Downloaded text for {title}")
                
                # Update inverted index with the new document
                doc_id = str(book_id)  # Use book ID as document ID
                inverted_index.add_document(doc_id, resp.text)
                print(f"🔍 Updated inverted index for document {doc_id}")
                
        except Exception as e:
            print(f"⚠️ Failed to download text for {title}: {e}")
    else:
        print(f"⚠️ No plain text available for {title}")

    return metadata


def get_last_id():
    """Read last downloaded ID from file, default 0 if missing."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0


def save_last_id(book_id):
    """Write last downloaded ID to file."""
    with open(STATE_FILE, "w") as f:
        f.write(str(book_id))


def load_catalog():
    """Load existing metadata catalog, return list."""
    catalog_path = os.path.join(OUTPUT_DIR, CATALOG_FILE)
    if os.path.exists(catalog_path):
        with open(catalog_path, "r", encoding="utf-8") as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []


def save_catalog(metadata_list):
    """Save metadata catalog (append mode)."""
    catalog_path = os.path.join(OUTPUT_DIR, CATALOG_FILE)
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(metadata_list, f, indent=2, ensure_ascii=False)


def main():
    last_id = get_last_id()
    print(f"📌 Starting from book ID {last_id + 1}")
    print(f"📂 Working directory: {SCRIPT_DIR}")
    print(f"📚 Books directory: {os.path.join(SCRIPT_DIR, OUTPUT_DIR)}")

    # Initialize inverted index
    index_path = os.path.join(OUTPUT_DIR, INDEX_FILE)
    inverted_index = InvertedIndex(index_path)
    print(f"🔍 Loaded inverted index from {index_path}")

    catalog = load_catalog()
    downloaded = 0
    current_id = last_id + 1

    while downloaded < BOOKS_PER_RUN:
        book = get_book_metadata(current_id)
        if book:
            metadata = download_book(book, OUTPUT_DIR, inverted_index)
            catalog.append(metadata)  # append new book
            downloaded += 1
            save_last_id(current_id)
        else:
            print(f"❌ No valid book for ID {current_id}, skipping...")

        current_id += 1
        time.sleep(SLEEP_TIME)

    # Save updated catalog and inverted index
    save_catalog(catalog)
    inverted_index.save_index()

    # Show final statistics
    stats = inverted_index.get_stats()
    print(f"\n✅ Finished downloading {downloaded} books.")
    print(f"📂 Metadata catalog now has {len(catalog)} entries.")
    print(f"🔍 Inverted index statistics:")
    print(f"   - Total unique words: {stats['total_words']:,}")
    print(f"   - Total documents: {stats['total_documents']:,}")
    print(f"   - Total word occurrences: {stats['total_word_occurrences']:,}")
    print(f"   - Average words per document: {stats['average_words_per_document']:.1f}")
    print(f"➡️ Next run will start from ID {current_id}")


if __name__ == "__main__":
    main()