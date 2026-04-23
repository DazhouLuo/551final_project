# DuckDB Midterm Starter

This starter package gets you to a valid **midterm implementation checkpoint** quickly:

- DuckDB installed and configured
- Initial schema/data model implemented
- At least one working query
- Optional minimal app feature with Streamlit

## Files

- `checkpoint.py` — creates the database, loads CSV data, runs a working query, prints `EXPLAIN ANALYZE`
- `app_streamlit.py` — minimal interactive app with safe query templates
- `data/basics.csv` and `data/ratings.csv` — tiny starter IMDb-like dataset

## 1) Install packages

In PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install duckdb pandas pyarrow streamlit
```

## 2) Run the checkpoint script

```powershell
python checkpoint.py
```

This should create:
- `imdb.duckdb`
- `data/basics.parquet`
- `data/ratings.parquet`

## 3) Run the minimal app

```powershell
streamlit run app_streamlit.py
```

## 4) What to screenshot for the midterm

- output of `SHOW TABLES`
- schema from `DESCRIBE basics` and `DESCRIBE ratings`
- one working query result
- one `EXPLAIN ANALYZE` output
- optional screenshot of the Streamlit page

## 5) Next project step

Replace the tiny starter CSV files with a larger curated IMDb subset and keep the same app structure.
