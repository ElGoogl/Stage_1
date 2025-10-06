import json
from pathlib import Path

INDEX_PATH = Path("data_repository/datamart_indexer_v1/inverted_index.json")


def load_index():
    """Load the inverted index from the JSON file."""
    if not INDEX_PATH.exists():
        raise FileNotFoundError(f"Inverted index not found at {INDEX_PATH}")
    with open(INDEX_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def search_file_v1(term, index=None):
    """
    Search for a single term in the inverted index.
    - term: string, search term
    - index: preloaded index (optional)
    """
    if index is None:
        index = load_index()
    term = term.lower()
    return index.get(term, [])


def search_multiple_files_v1(terms, index=None, mode="and"):
    """
    Search for multiple terms.
    mode='and': documents must contain *all* terms.
    mode='or': documents containing *any* of the terms.
    """
    if index is None:
        index = load_index()

    # Normalize and look up each term
    result_sets = []
    for term in terms:
        docs = set(index.get(term.lower(), []))
        result_sets.append(docs)

    if not result_sets:
        return []

    if mode == "and":
        results = set.intersection(*result_sets)
    else:
        results = set.union(*result_sets)

    return sorted(results)
