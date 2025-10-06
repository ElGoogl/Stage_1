from control.control_panel import control_pipeline_step
from crawler_v2 import download_book_v2
import time


if __name__ == "__main__":

    # Crawler v1 + Indexer v1 launched through the control panel
    print("Starting Control Panel test (5 iterations)...\n")

    # 5 test runs of the control panel (each run is either one indexing or one download)
    for i in range(5):
        print(f"--- Control panel iteration {i+1}/5 ---")
        control_pipeline_step()

        # Pause so the accesses to Gutenberg aren't too fast and our IP gets blocked lol
        time.sleep(2)

    print("\nControl Panel test finished.")

    # example book id list for crawler v2
    book_ids = [76921, 76922, 76947, 20345]

    #Crawler v2 thats creates the datalake v2 with categorized subfolders and txt files for metadata and content
    for book_id in book_ids:
        download_book_v2(book_id)