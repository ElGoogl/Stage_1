# Hierarchical_Indext/indexer_v2.py
import os
import re
from pathlib import Path
from collections import defaultdict

import nltk
from nltk.corpus import stopwords

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_LAKE_V2 = BASE_DIR / "data_repository" / "datalake_v2"
INDEX_ROOT_V2 = BASE_DIR / "data_repository" / "datamart_indexer_v2"

def _load_stopwords():
    try:
        sw = set(stopwords.words("english"))
    except LookupError:
        try:
            nltk.download("stopwords", quiet=True)
            sw = set(stopwords.words("english"))
        except Exception:
            sw = {
                "a","an","and","are","as","at","be","by","for","from","has",
                "he","in","is","it","its","of","on","that","the","to","was",
                "were","will","with","this","these","those","or","not","but",
                "you","your","i","we","they","she","him","her","them","our"
            }
    return sw

STOPWORDS = _load_stopwords()
WORD_RE = re.compile(r"\b\w+\b", flags=re.UNICODE)

def tokenize(text: str):
    # Unterstriche zu Leerzeichen → "_two" -> " two" → "two", "new_york" -> "new york"
    text = text.replace("_", " ")
    tokens = WORD_RE.findall(text.lower())
    return [t for t in tokens if t not in STOPWORDS]

def bucket_for_term(term: str) -> str:
    if not term:
        return "0-9"
    c = term[0].upper()
    if "A" <= c <= "Z":
        return c
    if term[0].isdigit():
        return "0-9"
    return "0-9"

def _safe_write_lines(path: Path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for line in lines:
            f.write(f"{line}\n")

def _iter_source_files(include_header=False, include_footer=False):
    if not DATA_LAKE_V2.exists():
        raise FileNotFoundError(f"datalake_v2 not found: {DATA_LAKE_V2}")
    for p in DATA_LAKE_V2.rglob("*_content.txt"):
        yield p
    if include_header:
        for p in DATA_LAKE_V2.rglob("*_header.txt"):
            yield p
    if include_footer:
        for p in DATA_LAKE_V2.rglob("*_footer.txt"):
            yield p

def build_hierarchical_index(clean: bool = True,
                             include_header: bool = False,
                             include_footer: bool = False) -> None:
    if clean and INDEX_ROOT_V2.exists():
        for root, dirs, files in os.walk(INDEX_ROOT_V2, topdown=False):
            for name in files:
                try: (Path(root) / name).unlink()
                except FileNotFoundError: pass
            for name in dirs:
                try: (Path(root) / name).rmdir()
                except OSError: pass
    INDEX_ROOT_V2.mkdir(parents=True, exist_ok=True)

    postings = defaultdict(set)

    for file_path in _iter_source_files(include_header=include_header, include_footer=include_footer):
        m = re.match(r"(\d+)_(header|content|footer)\.txt$", file_path.name)
        if not m:
            continue
        book_id = m.group(1)
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        unique_terms = set(tokenize(text))
        for term in unique_terms:
            postings[term].add(book_id)

    for term, ids in postings.items():
        bucket = bucket_for_term(term)
        out_dir = INDEX_ROOT_V2 / bucket
        out_file = out_dir / f"{term}.txt"
        sorted_ids = sorted(ids, key=lambda s: int(s))
        _safe_write_lines(out_file, sorted_ids)

    print(f"✅ Hierarchical index written to: {INDEX_ROOT_V2}")

# --------------- INKREMENTELL: nur ein Buch indexieren ---------------

def _content_files_for_book(book_id: int, include_header=False, include_footer=False):
    bid = str(book_id)
    # Dateien liegen in <...>/datalake_v2/<range>/<book_id>_*.txt
    for cand in DATA_LAKE_V2.rglob(f"{bid}_content.txt"):
        yield cand
    if include_header:
        for cand in DATA_LAKE_V2.rglob(f"{bid}_header.txt"):
            yield cand
    if include_footer:
        for cand in DATA_LAKE_V2.rglob(f"{bid}_footer.txt"):
            yield cand

def _append_book_to_term_file(term: str, book_id: int):
    """
    Fügt book_id in <INDEX_ROOT_V2>/<bucket>/<term>.txt ein (Set-Semantik):
    - liest existierende IDs (falls Datei existiert),
    - ergänzt book_id, falls noch nicht vorhanden,
    - schreibt sortiert zurück.
    Da nur ein Indexer-Thread schreibt, ist kein File-Lock notwendig.
    """
    bucket = bucket_for_term(term)
    out_dir = INDEX_ROOT_V2 / bucket
    out_file = out_dir / f"{term}.txt"
    out_dir.mkdir(parents=True, exist_ok=True)

    ids = set()
    if out_file.exists():
        try:
            content = out_file.read_text(encoding="utf-8", errors="ignore").splitlines()
            ids.update(line.strip() for line in content if line.strip().isdigit())
        except Exception:
            pass
    ids.add(str(book_id))
    sorted_ids = sorted(ids, key=lambda s: int(s))
    _safe_write_lines(out_file, sorted_ids)

def index_book_incremental(book_id: int,
                           include_header: bool = False,
                           include_footer: bool = False):
    """
    Indexiert NUR das angegebene Buch inkrementell in den hierarchischen Index.
    Erzeugt INDEX_ROOT_V2 bei Bedarf.
    """
    INDEX_ROOT_V2.mkdir(parents=True, exist_ok=True)

    # Alle Quelle(n) des Buchs laden → unique terms
    all_terms = set()
    any_found = False
    for file_path in _content_files_for_book(book_id, include_header, include_footer):
        any_found = True
        try:
            text = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        all_terms |= set(tokenize(text))

    if not any_found:
        raise FileNotFoundError(f"No TXT parts found for book_id={book_id} in {DATA_LAKE_V2}")

    # Postings-Dateien je Term aktualisieren
    for term in all_terms:
        _append_book_to_term_file(term, book_id)