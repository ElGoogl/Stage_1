import os
import requests
from pathlib import Path

BASE_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"
RAW_V2_DIR = Path("data_repository/raw_v2")

START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"


def get_subfolder(book_id: int) -> Path:
    """
    Calculate the subfolder for a book ID.
    Example: book_id=76343 -> subfolder '76001-77000'
    """
    lower = ((book_id - 1) // 1000) * 1000 + 1
    upper = lower + 999
    folder_name = f"{lower}-{upper}"
    return RAW_V2_DIR / folder_name


def download_book_v2(book_id: int):
    """Download a Gutenberg book and save header + content as separate TXT files."""
    url = BASE_URL.format(id=book_id)
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to download book {book_id}: HTTP {response.status_code}")
        return False

    text = response.text

    if START_MARKER not in text or END_MARKER not in text:
        print(f"Book {book_id} does not contain expected START/END markers")
        return False

    # Split into Metadata and content
    header, body_and_footer = text.split(START_MARKER, 1)
    body, _ = body_and_footer.split(END_MARKER, 1)

    subfolder = get_subfolder(book_id)
    subfolder.mkdir(parents=True, exist_ok=True)

    # Save header
    header_path = subfolder / f"{book_id}_header.txt"
    with open(header_path, "w", encoding="utf-8") as f:
        f.write(header.strip())

    # Save content
    content_path = subfolder / f"{book_id}_content.txt"
    with open(content_path, "w", encoding="utf-8") as f:
        f.write(body.strip())

    print(f"Book {book_id} saved in {subfolder}")
    return True

