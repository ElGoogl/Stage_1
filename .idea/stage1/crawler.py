import os
import requests

BASE_URL = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"

DATA_DIR = "data_repository/raw"


def download_book(book_id: int):
    url = BASE_URL.format(id=book_id)
    response = requests.get(url)

    if response.status_code == 200:
        os.makedirs(DATA_DIR, exist_ok=True)
        filepath = os.path.join(DATA_DIR, f"{book_id}.txt")
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Book {book_id} downloaded")
    else:
        print(f"Failed to download book {book_id}: HTTP {response.status_code}")


if __name__ == "__main__":
    book_ids = [76921, 76922]

    for book_id in book_ids:
        download_book(book_id)