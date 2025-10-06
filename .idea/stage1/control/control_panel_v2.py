from __future__ import annotations

import random
import threading
import time
from queue import Queue, Empty
from pathlib import Path
from typing import Iterable, Optional, Set, Sequence

from crawler_v2 import download_book_v2, get_subfolder
from Hierarchical_Indext import index_book_incremental
from metadata_sqlite import parse_gutenberg_metadata, store_metadata_in_db, create_metadata_table, get_metadata_from_db

# Ordner + State-Dateien
CONTROL_PATH = Path("control")
CONTROL_PATH.mkdir(parents=True, exist_ok=True)
DOWNLOADED_FILE = CONTROL_PATH / "downloaded_books.txt"
INDEXED_FILE    = CONTROL_PATH / "indexed_books.txt"
METADATA_FILE   = CONTROL_PATH / "metadata_stored_books.txt"

TOTAL_BOOKS = 80000  # obere Schranke für Random-IDs

# ------------------------ State helpers ------------------------

def _load_ids(path: Path) -> Set[str]:
    if path.exists():
        return set(path.read_text(encoding="utf-8").splitlines())
    return set()

def _append_id(path: Path, book_id: int | str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{int(book_id)}\n")

# ------------------------ Control Panel V2 ----------------------

class ControlPanelV2:
    """
    Pipeline:
      Downloader-Thread -> Queue[book_id] -> Indexer-Thread (+ Metadata Storage)

    - Lädt pro Run 'batch_size' Bücher (zufällige IDs ODER 'fixed_book_ids').
    - Nach erfolgreichem Download: sofort in DOWNLOADED_FILE + Queue.
    - Indexer verarbeitet Queue inkrementell, pflegt INDEXED_FILE und speichert Metadata.
    """

    def __init__(self,
                 batch_size: int = 5,
                 queue_size: int = 8,
                 throttle_seconds: float = 0.5,
                 max_random_tries: int = 100,
                 fixed_book_ids: Optional[Sequence[int]] = None):
        self.batch_size = int(batch_size)
        self.q: Queue[int] = Queue(maxsize=queue_size)
        self.throttle_seconds = throttle_seconds
        self.max_random_tries = max_random_tries
        self.fixed_book_ids = list(fixed_book_ids) if fixed_book_ids else None

        # State laden
        self.downloaded_ids: Set[str] = _load_ids(DOWNLOADED_FILE)
        self.indexed_ids: Set[str]    = _load_ids(INDEXED_FILE)
        self.metadata_ids: Set[str]   = _load_ids(METADATA_FILE)

        # Thread-Steuerung
        self._stop = threading.Event()
        self._producer = threading.Thread(target=self._run_producer, name="Downloader", daemon=True)
        self._consumer = threading.Thread(target=self._run_consumer, name="Indexer", daemon=True)

        # Initialize metadata database
        create_metadata_table()

    # ---------- Lifecycle ----------

    def start(self):
        self._producer.start()
        self._consumer.start()

    def join(self):
        self._producer.join()
        # Producer ist fertig → End-Signal für Consumer
        self.q.put(None)  # type: ignore
        self._consumer.join()

    def stop(self):
        self._stop.set()

    # ---------- Auswahl der nächsten IDs ----------

    def _select_next_ids(self) -> Sequence[int]:
        """
        Liefert bis zu 'batch_size' neue Kandidaten.
        Wenn fixed_book_ids gesetzt ist, werden daraus die ersten genommen,
        die noch nicht heruntergeladen wurden.
        """
        if self.fixed_book_ids is not None:
            picked, seen = [], set()
            for cand in self.fixed_book_ids:
                s = str(int(cand))
                if s in self.downloaded_ids or cand in seen:
                    continue
                picked.append(int(cand))
                seen.add(cand)
                if len(picked) >= self.batch_size:
                    break
            return picked

        picked: Set[int] = set()
        tries = 0
        while len(picked) < self.batch_size and tries < self.max_random_tries:
            tries += 1
            cand = random.randint(1, TOTAL_BOOKS)
            s = str(cand)
            if s in self.downloaded_ids or cand in picked:
                continue
            picked.add(cand)
        return list(picked)

    # ---------- Producer ----------

    def _run_producer(self):
        targets = list(self._select_next_ids())
        if not targets:
            print("[Downloader] Keine neuen Kandidaten gefunden (evtl. alles schon geladen?).")
        else:
            print(f"[Downloader] Zielanzahl für diesen Run: {len(targets)} Bücher")

        for book_id in targets:
            if self._stop.is_set():
                break

            print(f"[Downloader] ↓ Downloading {book_id} ...")
            ok = download_book_v2(book_id)
            if ok:
                print(f"[Downloader] ✓ Downloaded {book_id}")
                _append_id(DOWNLOADED_FILE, book_id)
                self.downloaded_ids.add(str(book_id))
                # In Queue legen (blockiert, wenn voll → Backpressure)
                self.q.put(book_id)
            else:
                print(f"[Downloader] ✗ Failed {book_id}")

            # leichtes Drosseln, um Gutenberg nicht zu stressen
            time.sleep(self.throttle_seconds)

        print("[Downloader] Done")

    # ---------- Consumer ----------

    def _run_consumer(self):
        while not self._stop.is_set():
            try:
                item = self.q.get(timeout=1.0)
            except Empty:
                continue

            if item is None:
                break

            book_id = int(item)
            book_id_str = str(book_id)
            
            # Check if already processed
            already_indexed = book_id_str in self.indexed_ids
            already_metadata = book_id_str in self.metadata_ids
            
            if already_indexed and already_metadata:
                print(f"[Indexer] → {book_id} ist bereits vollständig verarbeitet, überspringe.")
                self.q.task_done()
                continue

            # Indexing
            if not already_indexed:
                print(f"[Indexer] → Indexing {book_id} (incremental)")
                try:
                    index_book_incremental(book_id)
                    _append_id(INDEXED_FILE, book_id)
                    self.indexed_ids.add(book_id_str)
                    print(f"[Indexer] ✓ Indexed {book_id}")
                except Exception as e:
                    print(f"[Indexer] ✗ Indexing error for {book_id}: {e}")

            # Metadata extraction and storage
            if not already_metadata:
                print(f"[Indexer] → Extracting metadata for {book_id}")
                try:
                    # Get the header file path
                    subfolder = get_subfolder(book_id)
                    header_path = subfolder / f"{book_id}_header.txt"
                    
                    if header_path.exists():
                        # Read and parse header
                        header_content = header_path.read_text(encoding="utf-8")
                        metadata_dict = parse_gutenberg_metadata(header_content)
                        
                        if metadata_dict:
                            # Store in database
                            success = store_metadata_in_db(metadata_dict, book_id=book_id_str)
                            if success:
                                _append_id(METADATA_FILE, book_id)
                                self.metadata_ids.add(book_id_str)
                                print(f"[Indexer] ✓ Metadata stored for {book_id}")
                                
                                # === DEBUG: Print stored metadata (comment out this section to disable) ===
                                try:
                                    retrieved_metadata = get_metadata_from_db(book_id_str)
                                    if retrieved_metadata:
                                        print(f"[DEBUG] 📖 Retrieved metadata for book {book_id}:")
                                        for key, value in retrieved_metadata.items():
                                            # Truncate long values for readability
                                            display_value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                                            print(f"[DEBUG]   {key}: {display_value}")
                                        print(f"[DEBUG] 📖 End metadata for book {book_id}")
                                    else:
                                        print(f"[DEBUG] ⚠️ Could not retrieve metadata for book {book_id}")
                                except Exception as debug_e:
                                    print(f"[DEBUG] ✗ Error retrieving metadata for display: {debug_e}")
                                # === END DEBUG SECTION ===
                                
                            else:
                                print(f"[Indexer] ✗ Failed to store metadata for {book_id}")
                        else:
                            print(f"[Indexer] ⚠️ No metadata found in header for {book_id}")
                    else:
                        print(f"[Indexer] ⚠️ Header file not found for {book_id}: {header_path}")
                        
                except Exception as e:
                    print(f"[Indexer] ✗ Metadata extraction error for {book_id}: {e}")

            self.q.task_done()

        print("[Indexer] Done")