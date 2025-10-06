import re
import os
import json
from collections import defaultdict
import nltk
from nltk.corpus import stopwords

RAW_DIR = "data_repository/datalake_v1"
INDEX_DIR = "data_repository/datamart_indexer_v1"

try:
    STOPWORDS = set(stopwords.words("english"))
except LookupError:
    nltk.download("stopwords")
    STOPWORDS = set(stopwords.words("english"))
STOPWORDS = set(stopwords.words("english"))


def tokenize(text: str):
    tokens = re.findall(r"\b\w+\b", text.lower())
    return [t for t in tokens if t not in STOPWORDS]


def build_inverted_index():
    inverted_index = defaultdict(set)

    for filename in os.listdir(RAW_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(RAW_DIR, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                doc = json.load(f)

            doc_id = str(doc["id"])
            content = doc.get("content", "")

            tokens = tokenize(content)

            for token in tokens:
                inverted_index[token].add(doc_id)

    inverted_index = {word: list(doc_ids) for word, doc_ids in inverted_index.items()}
    return inverted_index


def save_index(index):
    """Save the inverted index to disk."""
    os.makedirs(INDEX_DIR, exist_ok=True)
    filepath = os.path.join(INDEX_DIR, "inverted_index.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"Inverted index saved to {filepath}")