# IMDb Analytics Explorer with DuckDB

This project is a Streamlit-based analytics application built on top of **DuckDB** using real IMDb datasets. The goal is to demonstrate not only working analytical queries but also the mapping between **application behavior** and **database internals** using `EXPLAIN ANALYZE`.

## Project Overview

The application supports several analytical queries on IMDb movie data, including:
* Top movies by rating
* Movie counts by decade
* Average rating by year
* Top genres by total votes
* Top Sci-Fi movies after 2010

The app also allows you to compare query execution performance across different source formats:
* DuckDB native tables
* Direct `TSV.GZ` reads
* Parquet files

## Core Internal Focus

This project serves as an educational tool for exploring **DuckDB query execution internals**. It focuses specifically on physical operators such as:
* `TABLE_SCAN`
* `HASH_JOIN`
* `HASH_GROUP_BY`
* `TOP_N`
* `ORDER_BY`

Using `EXPLAIN ANALYZE`, the project connects user-facing query results directly to the physical execution plan chosen by DuckDB.

---

## Repository Contents

* `app_streamlit_real_imdb.py` — The main Streamlit application.
* `checkpoint_real_imdb.py` — The setup/checkpoint script for loading data and verifying the database.
* `README.md` — Setup and run instructions.

## Dataset Setup

This project uses the official IMDb non-commercial datasets. **Note:** Due to size constraints, the raw data files and DuckDB database files are not included in this repository. 

1. Download the following files from the [IMDb datasets page](https://datasets.imdbws.com/):
   * `title.basics.tsv.gz`
   * `title.ratings.tsv.gz`
2. Create a `data/` folder in the root directory.
3. Place the downloaded files into the `data/` folder.

The checkpoint script will automatically generate `title.basics.parquet` and `title.ratings.parquet` for you.

### Expected Folder Structure

```text
551final_project/
├── app_streamlit_real_imdb.py
├── checkpoint_real_imdb.py
├── README.md
└── data/
    ├── title.basics.tsv.gz
    ├── title.ratings.tsv.gz
    ├── title.basics.parquet (generated)
    └── title.ratings.parquet (generated)
```

---

## Getting Started

### 1. Environment Setup

**macOS / Linux**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install duckdb pandas pyarrow streamlit
```

**Windows PowerShell**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install duckdb pandas pyarrow streamlit
```

### 2. Run the Checkpoint Script

Before running the app, initialize the database and generate the necessary files:

```bash
python checkpoint_real_imdb.py
```
**This script will:**
* Create or load the DuckDB database.
* Build the `basics` and `ratings` tables from the IMDb TSV files.
* Generate Parquet files if they do not already exist.
* Run example analytical queries to verify functionality.
* Print schema information and `EXPLAIN ANALYZE` output.

### 3. Run the Streamlit App

Launch the interactive dashboard:

```bash
streamlit run app_streamlit_real_imdb.py
```
*(If the `streamlit` command is not recognized, use: `python -m streamlit run app_streamlit_real_imdb.py`)*

---

## How to Use the App

1. **Launch** the Streamlit app in your browser.
2. **Choose a query template** from the sidebar dropdown.
3. **Select a source mode** to test performance:
   * DuckDB tables
   * Direct TSV.GZ
   * Parquet
4. **Run the query** and inspect the resulting data table.
5. **Toggle "Show execution plan"** to view the `EXPLAIN ANALYZE` output and study the physical operators used.

---

## Query Templates & Benchmarks

### Available Queries
* **Top movies by rating:** Join + filter + Top-N ranking.
* **Movie counts by decade:** Join + filter + grouped aggregation over transformed year values.
* **Average rating by year (movies only):** Join + filter + aggregation.
* **Top genres by total votes:** Join + split/unnest + aggregation.
* **Top Sci-Fi movies after 2010:** A more selective query for studying filter selectivity and Top-N behavior.

### Benchmark Summary
In preliminary benchmarking, the performance ranked as follows:
1. **DuckDB tables:** Fastest
2. **Parquet:** Slightly slower
3. **Direct TSV.GZ:** Much slower

This supports the project’s conclusion that native DuckDB tables and Parquet files are highly optimized for analytical workloads over large datasets.

### Demo Focus
For the final demo, the primary examples used to illustrate concepts are **Top movies by rating** and **Movie counts by decade**. These queries perfectly highlight the mapping between what the application requests, what DuckDB executes internally, and why the physical execution behavior matters.