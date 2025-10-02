import os
import requests
import json

BASE_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"
DATA_DIR = "data_repository/raw"


def parse_gutenberg_text(text: str, book_id: int) -> dict:
    """Extract metadata and content from Gutenberg.org .txt file and return as dictionary"""
    lines = text.splitlines()

    metadata = {
        "id": book_id,
        "title": None,
        "author": None,
        "release_date": None,
        "language": None,
        "original_publication": None,
        "credits": None,
        "content": None,
    }

    content_lines = []
    in_content = False
    for line in lines:
        if line.startswith("*** START OF THE PROJECT GUTENBERG EBOOK"):
            in_content = True
            continue
        if line.startswith("*** END OF THE PROJECT GUTENBERG EBOOK"):
            break
        if not in_content:
            if line.startswith("Title:"):
                metadata["title"] = line.replace("Title:", "").strip()
            elif line.startswith("Author:"):
                metadata["author"] = line.replace("Author:", "").strip()
            elif line.startswith("Release date:"):
                metadata["release_date"] = line.replace("Release date:", "").strip()
            elif line.startswith("Language:"):
                metadata["language"] = line.replace("Language:", "").strip()
            elif line.startswith("Original publication:"):
                metadata["original_publication"] = line.replace("Original publication:", "").strip()
            elif line.startswith("Credits:"):
                metadata["credits"] = line.replace("Credits:", "").strip()
        else:
            content_lines.append(line)

    metadata["content"] = "\n".join(content_lines).strip()
    return metadata


def download_book(book_id: int):
    """Download book, parse metadata and save as JSON files"""
    url = BASE_URL.format(id=book_id)
    response = requests.get(url)

    if response.status_code == 200:
        os.makedirs(DATA_DIR, exist_ok=True)
        book_data = parse_gutenberg_text(response.text, book_id)

        filepath = os.path.join(DATA_DIR, f"{book_id}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(book_data, f, ensure_ascii=False, indent=2)

        print(f"Book {book_id} saved as {filepath}")
    else:
        print(f"Failed to download book {book_id}: HTTP {response.status_code}")
