import re
import os
import json
from collections import defaultdict
import nltk
from nltk.corpus import stopwords

# -------------------------------------------------------------
# Pfaddefinitionen (robust & dynamisch)
# -------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data_repository", "datalake_v1")
INDEX_DIR = os.path.join(BASE_DIR, "data_repository", "datamart_indexer_v1")

# Überprüfen, ob RAW_DIR existiert
if not os.path.exists(RAW_DIR):
    raise FileNotFoundError(f"❌ RAW_DIR not found: {RAW_DIR}")
else:
    print(f"✅ Using RAW_DIR: {RAW_DIR}")

# -------------------------------------------------------------
# Stopwords laden (robust gegen fehlende Downloads)
# -------------------------------------------------------------
try:
    STOPWORDS = set(stopwords.words("english"))
except LookupError:
    print("⚠️ Stopwords not found — downloading...")
    nltk.download("stopwords")
    STOPWORDS = set(stopwords.words("english"))

# -------------------------------------------------------------
# Tokenizer
# -------------------------------------------------------------
def tokenize(text: str):
    """Zerlegt Text in Tokens und entfernt Stopwords."""
    tokens = re.findall(r"\b\w+\b", text.lower())
    return [t for t in tokens if t not in STOPWORDS]


# -------------------------------------------------------------
# Inverted Index Builder
# -------------------------------------------------------------
def build_inverted_index():
    """Erstellt ein Inverted Index aus allen JSON-Dateien im RAW_DIR."""
    inverted_index = defaultdict(set)

    print("🔍 Building inverted index...")
    for filename in os.listdir(RAW_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(RAW_DIR, filename)

            # Buch lesen
            with open(filepath, "r", encoding="utf-8") as f:
                doc = json.load(f)

            doc_id = str(doc["id"])
            content = doc.get("content", "")

            print(f"   → Indexing book ID {doc_id} ({filename})")

            # Tokenisierung
            tokens = tokenize(content)

            # Wörter zum Index hinzufügen
            for token in tokens:
                inverted_index[token].add(doc_id)

    # Sets zu Listen umwandeln (für JSON-Speicherung)
    inverted_index = {word: sorted(list(doc_ids)) for word, doc_ids in inverted_index.items()}
    print(f"✅ Indexed {len(inverted_index)} unique tokens.")
    return inverted_index


# -------------------------------------------------------------
# Index speichern
# -------------------------------------------------------------
def save_index(index):
    """Speichert das Inverted Index als JSON-Datei im INDEX_DIR."""
    os.makedirs(INDEX_DIR, exist_ok=True)
    filepath = os.path.join(INDEX_DIR, "inverted_index.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"💾 Inverted index saved to {filepath}")