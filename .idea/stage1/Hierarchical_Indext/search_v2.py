# stage1/Hierarchical_Indext/search_v2.py
from __future__ import annotations
from pathlib import Path
from typing import List
from functools import lru_cache

# interne Konstanten/Helper aus dem Indexer
from .indexer_v2 import INDEX_ROOT_V2, bucket_for_term, STOPWORDS, WORD_RE


# ---------- Normalisierung wie im Indexer ----------
def _normalize_term(term: str) -> str:
    term = (term or "").replace("_", " ").lower()
    toks = WORD_RE.findall(term)
    toks = [t for t in toks if t not in STOPWORDS]
    return toks[0] if toks else ""


def _postings_path_for_term(term_norm: str) -> Path:
    return INDEX_ROOT_V2 / bucket_for_term(term_norm) / f"{term_norm}.txt"


# ---------- FAST PATH: Datei-Reads cachen ----------
@lru_cache(maxsize=10000)
def _read_postings_file_cached(path: Path) -> List[str]:
    """
    Liest eine Postingsliste (Datei) effizient mit LRU-Cache.
    Gibt eine Liste von Book-IDs als Strings zurück.
    """
    if not path.exists():
        return []
    try:
        data = path.read_bytes()
        return [
            line.decode("utf-8", errors="ignore").strip()
            for line in data.splitlines()
            if line.strip()
        ]
    except Exception:
        return []


def clear_postings_cache() -> None:
    """LRU-Cache für Postings-Dateien leeren (für 'cold' Benchmarks)."""
    _read_postings_file_cached.cache_clear()


def search_postings(term: str) -> List[str]:
    """
    Postingsliste (Book-IDs) für ein einzelnes Wort.
    Nutzt LRU-Cache pro Datei.
    """
    term_norm = _normalize_term(term)
    if not term_norm:
        return []
    path = _postings_path_for_term(term_norm)
    return _read_postings_file_cached(path)