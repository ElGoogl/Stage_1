import os
import requests
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"
RAW_V2_DIR = Path("data_repository/datalake_v2")

START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"

def get_with_retries(url, retries=3, backoff=1):
    """Helper to fetch URLs with retry and timeout logic."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Download failed for {url}: {e}")
        return None

def get_subfolder(book_id: int) -> Path:
    """
    Calculate the subfolder for a book ID.
    Example: book_id=76343 -> subfolder '76001-77000'
    """
    lower = ((book_id - 1) // 1000) * 1000 + 1
    upper = lower + 999
    return RAW_V2_DIR / f"{lower}-{upper}"


def split_gutenberg_text(text: str, book_id: int):
    """
    Split a Gutenberg text into header, content, footer sections.
    Returns tuple: (header, content, footer)
    """
    if START_MARKER not in text or END_MARKER not in text:
        print(f"Book {book_id} missing expected START/END markers.")
        return text.strip(), "", ""

    header, body_and_footer = text.split(START_MARKER, 1)
    content, footer = body_and_footer.split(END_MARKER, 1)
    return header.strip(), content.strip(), footer.strip()


def download_book_v2(book_id: int):
    """Download a Gutenberg book and save header, content, and footer as separate TXT files."""
    url = BASE_URL.format(id=book_id)
    response = get_with_retries(url)
    if response is None:
        return False


    if response.status_code != 200:
        print(f"Failed to download book {book_id}: HTTP {response.status_code}")
        return False

    header, content, footer = split_gutenberg_text(response.text, book_id)

    # Prepare subfolder based on ID range
    subfolder = get_subfolder(book_id)
    subfolder.mkdir(parents=True, exist_ok=True)

    # Save header
    header_path = subfolder / f"{book_id}_header.txt"
    with open(header_path, "w", encoding="utf-8") as f:
        f.write(header)

    # Save content
    content_path = subfolder / f"{book_id}_content.txt"
    with open(content_path, "w", encoding="utf-8") as f:
        f.write(content)

    # Save footer
    footer_path = subfolder / f"{book_id}_footer.txt"
    with open(footer_path, "w", encoding="utf-8") as f:
        f.write(footer)

    print(f"Book {book_id} saved as 3 files in {subfolder}")
    return True