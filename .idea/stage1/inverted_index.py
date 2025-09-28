# by chat gpt

import json
import os
import re
from collections import defaultdict, Counter
from typing import Dict, Set, List, Optional


class InvertedIndex:
    """
    Inverted index data structure that maps words to documents and their frequencies.
    Format: {word: {doc_id: count, doc_id: count, ...}}
    """
    
    def __init__(self, index_file: str = "inverted_index.json"):
        self.index_file = index_file
        self.index: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.document_stats: Dict[str, int] = {}  # doc_id -> total_word_count
        self.load_index()
    
    def preprocess_text(self, text: str) -> List[str]:
        """Clean and tokenize text into words."""
        # Convert to lowercase and remove non-alphabetic characters
        text = re.sub(r'[^a-zA-Z\s]', ' ', text.lower())
        # Split into words and filter out empty strings and short words
        words = [word.strip() for word in text.split() if len(word.strip()) > 2]
        return words
    
    def add_document(self, doc_id: str, text: str) -> None:
        """Add or update a document in the inverted index."""
        words = self.preprocess_text(text)
        word_counts = Counter(words)
        
        # Remove old entries for this document if it exists
        self.remove_document(doc_id)
        
        # Add new word counts
        for word, count in word_counts.items():
            self.index[word][doc_id] = count
        
        # Update document stats
        self.document_stats[doc_id] = len(words)
        
        print(f"✅ Added document {doc_id} with {len(words)} words ({len(set(words))} unique)")
    
    def add_document_from_file(self, doc_id: str, file_path: str) -> bool:
        """Add a document from a text file."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
            self.add_document(doc_id, text)
            return True
        except Exception as e:
            print(f"❌ Failed to process {file_path}: {e}")
            return False
    
    def remove_document(self, doc_id: str) -> None:
        """Remove a document from the inverted index."""
        # Remove doc_id from all word entries
        words_to_remove = []
        for word, doc_dict in self.index.items():
            if doc_id in doc_dict:
                del doc_dict[doc_id]
                # If no documents left for this word, mark for removal
                if not doc_dict:
                    words_to_remove.append(word)
        
        # Remove empty word entries
        for word in words_to_remove:
            del self.index[word]
        
        # Remove from document stats
        if doc_id in self.document_stats:
            del self.document_stats[doc_id]
    
    def search(self, query: str) -> Dict[str, int]:
        """Search for documents containing query words."""
        query_words = self.preprocess_text(query)
        if not query_words:
            return {}
        
        # Get documents that contain any of the query words
        result_docs = defaultdict(int)
        for word in query_words:
            if word in self.index:
                for doc_id, count in self.index[word].items():
                    result_docs[doc_id] += count
        
        # Sort by relevance (total word count)
        return dict(sorted(result_docs.items(), key=lambda x: x[1], reverse=True))
    
    def get_word_documents(self, word: str) -> Dict[str, int]:
        """Get all documents containing a specific word."""
        word = word.lower().strip()
        return dict(self.index.get(word, {}))
    
    def get_document_words(self, doc_id: str) -> Dict[str, int]:
        """Get all words in a specific document with their counts."""
        doc_words = {}
        for word, doc_dict in self.index.items():
            if doc_id in doc_dict:
                doc_words[word] = doc_dict[doc_id]
        return doc_words
    
    def get_stats(self) -> Dict:
        """Get statistics about the index."""
        return {
            "total_words": len(self.index),
            "total_documents": len(self.document_stats),
            "total_word_occurrences": sum(
                sum(doc_dict.values()) for doc_dict in self.index.values()
            ),
            "average_words_per_document": (
                sum(self.document_stats.values()) / len(self.document_stats)
                if self.document_stats else 0
            )
        }
    
    def save_index(self) -> None:
        """Save the inverted index to file."""
        data = {
            "index": dict(self.index),
            "document_stats": self.document_stats
        }
        with open(self.index_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"💾 Saved index to {self.index_file}")
    
    def load_index(self) -> None:
        """Load the inverted index from file."""
        if os.path.exists(self.index_file):
            try:
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Convert back to defaultdict structure
                self.index = defaultdict(lambda: defaultdict(int))
                for word, doc_dict in data.get("index", {}).items():
                    for doc_id, count in doc_dict.items():
                        self.index[word][doc_id] = count
                
                self.document_stats = data.get("document_stats", {})
                print(f"📂 Loaded index from {self.index_file}")
            except Exception as e:
                print(f"⚠️ Failed to load index: {e}")
                self.index = defaultdict(lambda: defaultdict(int))
                self.document_stats = {}


def process_gutenberg_books(books_dir: str = "gutenberg_books", 
                          index_file: str = "gutenberg_inverted_index.json") -> InvertedIndex:
    """Process all Gutenberg books and build inverted index."""
    idx = InvertedIndex(index_file)
    
    if not os.path.exists(books_dir):
        print(f"❌ Directory {books_dir} not found")
        return idx
    
    txt_files = [f for f in os.listdir(books_dir) if f.endswith('.txt')]
    print(f"📚 Found {len(txt_files)} text files to process")
    
    for txt_file in txt_files:
        file_path = os.path.join(books_dir, txt_file)
        doc_id = os.path.splitext(txt_file)[0]  # Use filename without extension as doc_id
        idx.add_document_from_file(doc_id, file_path)
    
    idx.save_index()
    stats = idx.get_stats()
    print(f"\n📊 Index Statistics:")
    print(f"   Total unique words: {stats['total_words']:,}")
    print(f"   Total documents: {stats['total_documents']:,}")
    print(f"   Total word occurrences: {stats['total_word_occurrences']:,}")
    print(f"   Average words per document: {stats['average_words_per_document']:.1f}")
    
    return idx


def update_index_with_new_files(books_dir: str = "gutenberg_books",
                               index_file: str = "gutenberg_inverted_index.json") -> InvertedIndex:
    """Update existing index with any new files found in the directory."""
    idx = InvertedIndex(index_file)
    
    if not os.path.exists(books_dir):
        print(f"❌ Directory {books_dir} not found")
        return idx
    
    txt_files = [f for f in os.listdir(books_dir) if f.endswith('.txt')]
    existing_docs = set(idx.document_stats.keys())
    
    new_files = []
    for txt_file in txt_files:
        doc_id = os.path.splitext(txt_file)[0]
        if doc_id not in existing_docs:
            new_files.append((doc_id, txt_file))
    
    if not new_files:
        print("📋 No new files to process")
        return idx
    
    print(f"📚 Found {len(new_files)} new files to process")
    
    for doc_id, txt_file in new_files:
        file_path = os.path.join(books_dir, txt_file)
        idx.add_document_from_file(doc_id, file_path)
    
    idx.save_index()
    return idx


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "update":
            # Update mode - only process new files
            print("🔄 Updating inverted index with new files...")
            index = update_index_with_new_files()
        elif sys.argv[1] == "rebuild":
            # Rebuild mode - process all files
            print("🔨 Rebuilding inverted index from scratch...")
            index = process_gutenberg_books()
        else:
            print("Usage: python inverted_index.py [update|rebuild]")
            sys.exit(1)
    else:
        # Default: update mode
        print("🔄 Updating inverted index with new files...")
        index = update_index_with_new_files()
    
    # Example searches
    print("\n🔎 Example searches:")
    
    # Search for specific words
    results = index.search("love peace")
    print(f"\nDocuments containing 'love' or 'peace': {len(results)}")
    for doc_id, score in list(results.items())[:5]:
        print(f"  {doc_id}: {score} matches")
    
    # Get stats for a specific word
    love_docs = index.get_word_documents("love")
    print(f"\nWord 'love' appears in {len(love_docs)} documents")
    
    # Show final stats
    stats = index.get_stats()
    print(f"\n📊 Final Index Statistics:")
    print(f"   Total unique words: {stats['total_words']:,}")
    print(f"   Total documents: {stats['total_documents']:,}")
    print(f"   Total word occurrences: {stats['total_word_occurrences']:,}")
    print(f"   Average words per document: {stats['average_words_per_document']:.1f}")