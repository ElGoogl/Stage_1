from crawler_v1 import download_book
from crawler_v2 import download_book_v2
from indexer import build_inverted_index, save_index

if __name__ == "__main__":
    # example book id list
    book_ids = [76921, 76922, 76947, 20345]

    # Crawler v1 thats creates the Datalake v1 with Metadata + content JSON files
    for book_id in book_ids:
        download_book(book_id)

    index = build_inverted_index()
    save_index(index)

    #Crawler v2 thats creates the datalake v2 with categorized subfolders and txt files for metadata and content
    for book_id in book_ids:
        download_book_v2(book_id)