import shutil
from pathlib import Path

# Base paths (adjust if needed)
DATA_REPO = Path("../data_repository")
CONTROL_PATH = Path("../control")

# directory names
DATALAKE_V1 = DATA_REPO / "datalake_v1"
DATALAKE_V2 = DATA_REPO / "datalake_v2"
DATAMART_INDEXER_V1 = DATA_REPO / "datamart_indexer_v1"

# Control panel tracking files
DOWNLOADED = CONTROL_PATH / "downloaded_books.txt"
INDEXED_LIST = CONTROL_PATH / "indexed_books.txt"


def cleanup_project(confirm: bool = True):
    """
    Deletes both datalakes, the datamart (indexed data), and control panel files.
    Use with care – this permanently removes all data!
    """
    if confirm:
        print("This will permanently delete all crawled and indexed data!")
        answer = input("Type 'YES' to confirm: ")
        if answer.strip().upper() != "YES":
            print("Aborted cleanup.")
            return

    # Helper: safely delete directory if it exists
    def safe_rmdir(path: Path):
        if path.exists():
            shutil.rmtree(path)
            print(f"Deleted folder: {path}")

    # Helper: safely delete file if it exists
    def safe_rmfile(path: Path):
        if path.exists():
            path.unlink()
            print(f"Deleted file: {path}")

    # Delete datalakes and datamart
    safe_rmdir(DATALAKE_V1)
    safe_rmdir(DATALAKE_V2)
    safe_rmdir(DATAMART_INDEXER_V1)

    # Delete control panel tracking files
    safe_rmfile(DOWNLOADED)
    safe_rmfile(INDEXED_LIST)

    print("Cleanup complete — project state has been reset.")

if __name__ == "__main__":
    cleanup_project()