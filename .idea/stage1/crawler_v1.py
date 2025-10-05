import os
import requests
import json
from pathlib import Path

BASE_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"
RAW_DIR = Path("data_repository/datalake_v1")

START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"


def parse_gutenberg_text(text: str, book_id: int) -> dict:
    """
    Split a Gutenberg text into header, content, and footer parts.
    """
    if START_MARKER not in text or END_MARKER not in text:
        print(f"Book {book_id} missing expected START/END markers")
        return {"id": book_id, "header": text.strip(), "content": "", "footer": ""}

    header, body_and_footer = text.split(START_MARKER, 1)
    content, footer = body_and_footer.split(END_MARKER, 1)

    return {
        "id": book_id,
        "header": header.strip(),
        "content": content.strip(),
        "footer": footer.strip(),
    }


def download_book_v1(book_id: int):
    """Download a Gutenberg book and save header, content, and footer as JSON."""
    url = BASE_URL.format(id=book_id)
    response = requests.get(url)

    if response.status_code != 200:
        print(f"Failed to download book {book_id}: HTTP {response.status_code}")
        return False

    data = parse_gutenberg_text(response.text, book_id)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    filepath = RAW_DIR / f"{book_id}.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Book {book_id} saved as JSON with header/content/footer at {filepath}")
    return True

