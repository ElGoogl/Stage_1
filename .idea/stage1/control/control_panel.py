from pathlib import Path
import random
from crawler_v1 import download_book_v1
from indexer import build_inverted_index, save_index

# Paths for tracking state
CONTROL_PATH = Path("control")
DOWNLOADS = CONTROL_PATH / "downloaded_books.txt"
INDEXINGS = CONTROL_PATH / "indexed_books.txt"

# Total range of available book IDs (Project Gutenberg goes up to ~70,000+)
TOTAL_BOOKS = 80000


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
    2. If found → index them.
    3. If none → download a new one.
    """

    CONTROL_PATH.mkdir(parents=True, exist_ok=True)

    # 1️Load current state
    downloaded = load_state(DOWNLOADS)
    indexed = load_state(INDEXINGS)

    ready_to_index = downloaded - indexed

    # 2If there are books ready to index
    if ready_to_index:
        book_id = ready_to_index.pop()
        print(f"[CONTROL] Scheduling book {book_id} for indexing...")

        # Run the indexer (this builds the index for all downloaded files)
        index = build_inverted_index()
        save_index(index)

        save_state(INDEXINGS, book_id)
        print(f"[CONTROL] Book {book_id} indexed successfully.")
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
