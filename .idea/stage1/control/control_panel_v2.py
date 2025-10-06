from __future__ import annotations

import random
import threading
import time
from queue import Queue, Empty
from pathlib import Path
from typing import Iterable, Optional, Set, Sequence

from crawler_v2 import download_book_v2
from Hierarchical_Indext import index_book_incremental

# Ordner + State-Dateien
CONTROL_PATH = Path("control")
CONTROL_PATH.mkdir(parents=True, exist_ok=True)
DOWNLOADED_FILE = CONTROL_PATH / "downloaded_books.txt"
INDEXED_FILE    = CONTROL_PATH / "indexed_books.txt"

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
      Downloader-Thread -> Queue[book_id] -> Indexer-Thread

    - Lädt pro Run 'batch_size' Bücher (zufällige IDs ODER 'fixed_book_ids').
    - Nach erfolgreichem Download: sofort in DOWNLOADED_FILE + Queue.
    - Indexer verarbeitet Queue inkrementell und pflegt INDEXED_FILE.
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

        # Thread-Steuerung
        self._stop = threading.Event()
        self._producer = threading.Thread(target=self._run_producer, name="Downloader", daemon=True)
        self._consumer = threading.Thread(target=self._run_consumer, name="Indexer", daemon=True)

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
            if str(book_id) in self.indexed_ids:
                print(f"[Indexer] → {book_id} ist bereits indexiert, überspringe.")
                self.q.task_done()
                continue

            print(f"[Indexer] → Indexing {book_id} (incremental)")
            try:
                index_book_incremental(book_id)
                _append_id(INDEXED_FILE, book_id)
                self.indexed_ids.add(str(book_id))
                print(f"[Indexer] ✓ Indexed {book_id}")
            except Exception as e:
                print(f"[Indexer] ✗ Indexing error for {book_id}: {e}")
            finally:
                self.q.task_done()

        print("[Indexer] Done")