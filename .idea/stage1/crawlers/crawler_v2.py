import os
import requests
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"

# --- Pfade robust relativ zum Repo (/stage1) auflösen ---
BASE_DIR = Path(__file__).resolve().parents[1]  # .../stage1
RAW_V2_DIR = BASE_DIR / "data_repository" / "datalake_v2"

START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER = "*** END OF THE PROJECT GUTENBERG EBOOK"

def get_with_retries(url, retries=3, backoff=1):
    """Helper to fetch URLs with Retry und Timeout."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],  # bei älterem urllib3 ggf. method_whitelist verwenden
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
    Berechnet die ID-Range-Unterordner, z.B. 76343 -> '76001-77000'.
    """
    lower = ((book_id - 1) // 1000) * 1000 + 1
    upper = lower + 999
    return RAW_V2_DIR / f"{lower}-{upper}"

def split_gutenberg_text(text: str, book_id: int):
    """
    Teilt den Gutenberg-Text in (header, content, footer).
    """
    if START_MARKER not in text or END_MARKER not in text:
        print(f"Book {book_id} missing expected START/END markers.")
        return text.strip(), "", ""
    header, body_and_footer = text.split(START_MARKER, 1)
    content, footer = body_and_footer.split(END_MARKER, 1)
    return header.strip(), content.strip(), footer.strip()

def download_book_v2(book_id: int) -> bool:
    """Lädt ein Buch und speichert header/content/footer als separate .txt unter datalake_v2."""
    url = BASE_URL.format(id=book_id)
    response = get_with_retries(url)
    if response is None:
        return False

    if response.status_code != 200:
        print(f"Failed to download book {book_id}: HTTP {response.status_code}")
        return False

    header, content, footer = split_gutenberg_text(response.text, book_id)

    subfolder = get_subfolder(book_id)
    subfolder.mkdir(parents=True, exist_ok=True)

    (subfolder / f"{book_id}_header.txt").write_text(header, encoding="utf-8")
    (subfolder / f"{book_id}_content.txt").write_text(content, encoding="utf-8")
    (subfolder / f"{book_id}_footer.txt").write_text(footer, encoding="utf-8")

    print(f"Book {book_id} saved as 3 files in {subfolder}")
    return True
