# 🧠 Search Engine Big Data – Stage 1

This project implements the **Stage 1 data pipeline** for a simple search engine to search through Gutenberg.org.  
It includes:
- Two data crawlers (JSON and TXT),
- Two indexers with inverted indexing,
- Two databanks as metadata repositories
- A query engine for term-based search for every indexer,
- A control panel that orchestrates the pipeline,
- Benchmarking for performance analysis.

---

## 📦 Project Overview

```

## 📦 Project Overview


stage1/
│
├── crawlers/
│   ├── crawler_v1.py          # Crawler (JSON format)
│   └── crawler_v2.py          # Crawler (TXT format, 3-part files)
│
├── indexers/
│   └── indexer_v1.py          # Indexer with tokenization and inverted index
│
├── indexer_query/
│   └── query_engine.py        # Single and multi-term search
│
├── control/
│   └── control_panel.py       # Manages downloading + indexing workflow
│
├── utils/
│   ├── cleanup_project.py     # Deletes all datalakes and control files
│   
│
├── benchmarking/
│   ├── benchmark_index_scaling.py # Index scaling + search benchmark
│   ├── benchmark_indexer_v1.py    # Indexer benchmarking
│
├── data_repository/
│   ├── datalake_v1/          # JSON files (crawler v1)
│   ├── datalake_v2/          # TXT files (crawler v2)
│   ├── datamart_indexer_v1/  # Indexed inverted index JSON
│
├── Main.py                   # Optional entry point
└── README.md

'''

 🧰 Requirements

### 🐍 Python
**Python 3.10 or later** is required (tested with Python 3.13).  

### 📦 Install dependencies
Install all required packages using pip:

```bash
pip install requests nltk memory-profiler psutil sortedcontainers
````

If you use NLTK for stopwords, run this once:

```python
import nltk
nltk.download("stopwords")
```

---

## 🚀 Setup Instructions

### 1️⃣ Clone the repository

```bash
git clone https://github.com/<your-username>/SearchEngineBigData.git
cd SearchEngineBigData
```

### 2️⃣ Mark stage1 as a Python package root

Ensure that every folder contains an `__init__.py` file.

### 3️⃣ Run the functions

Run Scripts in the ``Main.py`` file

---

## 🧩 Components and Usage

### 🕸️ Crawler v1 (JSON-based)

Downloads Project Gutenberg texts and stores each as a JSON file
with three sections: header, content, footer.

Run manually:

```python
from stage1.crawlers.crawler_v1 import download_book_v1
download_book_v1(76921)
```

📁 **Output:**

```
data_repository/datalake_v1/76921.json
```

---

### 📂 Crawler v2 (TXT-based)

Downloads and splits books into three text files:
`<id>_header.txt`, `<id>_content.txt`, `<id>_footer.txt`.

Run manually:

```python
from stage1.crawlers.crawler_v2 import download_book_v2
download_book_v2(76921)
```

📁 **Output:**

```
data_repository/datalake_v2/76001-77000/76921_content.txt
```

---

### ⚙️ Indexer v1

Builds an inverted index from the JSON datalake (v1).
Handles tokenization, normalization, and stopword removal.

Run manually:

```python
from stage1.indexers.indexer_v1 import build_inverted_index, save_index
index = build_inverted_index()
save_index(index)
```

📁 **Output:**

```
data_repository/datamart_indexer_v1/inverted_index.json
```

---

### 🔍 Indexer_v1 Query Engine

Allows single-term and multi-term searches over the index.

Example:

```python
from stage1.query.query_engine import search, search_multiple

search("house")  
# → ['70001', '70023']

search_multiple(["white", "house"], mode="and")  
# → ['70023']
```

---

### 🧠 Control Panel

Coordinates the data pipeline:

* Downloads books
* Indexes them
* Tracks progress

Run:

```bash
python -m stage1.control.control_panel
```

Or from code:

```python
from stage1.control.control_panel import control_pipeline_step
control_pipeline_step()
```

✅ Automatically checks which books are downloaded/indexed
and triggers the next step accordingly.

---

## 🧪 Benchmarking

### 1️⃣ Index Scaling + Index search v1 Benchmark

Measures how indexing performance scales with dataset size and how query time behaves.

Run:

```bash
python -m stage1.benchmarking.benchmark_indexer_v1
```

📊 **Output:**

```
data_repository/benchmark_index_scaling.csv
```

**Columns:**
| Number of Books | Indexing Time (s) | Memory (MB) | File Size (MB) | Search (advantage) [µs] | Search (house) [µs] | Search (white) [µs] |

---

### 2️⃣ Other Benchmarks

* `benchmark_indexer_v1.py` → Tests index build performance.
* `benchmark_crawlers.py` → Compares crawler v1 and v2.

All can be run similarly:

```bash
python -m stage1.benchmarking.<filename_without_py>
```

---

## 🧹 Cleanup Utilities

### Delete everything (reset project)

```python
from stage1.utils.cleanup_project import cleanup_project
cleanup_project(confirm=False)
```

Deletes:

* `data_repository/datalake_v1`
* `data_repository/datalake_v2`
* `data_repository/datamart_indexer_v1`
* Control files (`downloaded_books.txt`, `indexed_books.txt`)

---

### Delete only the indexed file

```python
from stage1.utils.project_cleanup import delete_indexed_file
delete_indexed_file()
```

---

## 📈 Typical Workflow

### 1️⃣ Start fresh

```python
from stage1.utils.cleanup_project import cleanup_project
cleanup_project(confirm=False)
```

### 2️⃣ Run Control Panel (downloads + indexes automatically)

```bash
python -m stage1.control.control_panel
```

### 3️⃣ Search your index

```python
from stage1.indexer_query.indexed_query_v1 import search_file_v1
print(search_file_v1("advantage"))
```



---

## 🧑‍💻 Authors

Project developed as part of **Big Data – Stage 1 (2025)**
**Search Engine Big Data Group @ University Project** by Marco Keilwagen, Kacper Clasen, Richard Raatz, Jonas Müller and Milosch Fürsteneberg

---

## 📜 License

This project is for **academic use**.
All downloaded Project Gutenberg texts are **public domain**,
but please respect [Project Gutenberg’s Terms of Use](https://www.gutenberg.org/policy/terms_of_use.html).

```
