
# by chat gpt

import requests
import os
import time
import json

# === CONFIGURATION ===
OUTPUT_DIR = "gutenberg_books"
STATE_FILE = "last_id.txt"   # keeps track of last downloaded ID
BOOKS_PER_RUN = 5            # how many books to fetch each run
SLEEP_TIME = 2               # polite delay between requests (seconds)


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


def download_book(book, output_dir):
    """Save minimal metadata + download text if available."""
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

    # Save metadata
    meta_path = os.path.join(output_dir, f"{book_id}_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"✅ Saved metadata for {title} (ID {book_id})")

    # Save text if available
    if text_url:
        try:
            resp = requests.get(text_url, timeout=20)
            if resp.status_code == 200:
                text_path = os.path.join(output_dir, f"{book_id}.txt")
                with open(text_path, "w", encoding="utf-8", errors="ignore") as f:
                    f.write(resp.text)
                print(f"📖 Downloaded text for {title}")
        except Exception as e:
            print(f"⚠️ Failed to download text for {title}: {e}")
    else:
        print(f"⚠️ No plain text available for {title}")


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


def main():
    last_id = get_last_id()
    print(f"📌 Starting from book ID {last_id + 1}")

    downloaded = 0
    current_id = last_id + 1

    while downloaded < BOOKS_PER_RUN:
        book = get_book_metadata(current_id)
        if book:
            download_book(book, OUTPUT_DIR)
            downloaded += 1
            save_last_id(current_id)
        else:
            print(f"❌ No valid book for ID {current_id}, skipping...")

        current_id += 1
        time.sleep(SLEEP_TIME)

    print(f"\n✅ Finished downloading {downloaded} books.")
    print(f"➡️ Next run will start from ID {current_id}")


if __name__ == "__main__":
    main()