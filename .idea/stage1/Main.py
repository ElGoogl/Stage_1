from crawler import download_book
from indexer import build_inverted_index, save_index

if __name__ == "__main__":
    # example book id list
    book_ids = [76921, 76922, 76947]

    for book_id in book_ids:
        download_book(book_id)

    index = build_inverted_index()
    save_index(index)