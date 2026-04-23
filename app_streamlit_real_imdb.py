from pathlib import Path
import time
import duckdb
import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "imdb_real.duckdb"

BASICS_FILE = DATA_DIR / "title.basics.tsv.gz"
RATINGS_FILE = DATA_DIR / "title.ratings.tsv.gz"
BASICS_PARQUET = DATA_DIR / "title.basics.parquet"
RATINGS_PARQUET = DATA_DIR / "title.ratings.parquet"

st.set_page_config(page_title="IMDb Analytics Explorer", layout="wide")
st.title("IMDb Analytics Explorer")
st.caption("DuckDB analytics app using the real IMDb title.basics and title.ratings datasets.")

for required in [BASICS_FILE, RATINGS_FILE]:
    if not required.exists():
        st.error(f"Missing required file: {required}")
        st.stop()


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))

    con.execute(f"""
        CREATE OR REPLACE TABLE basics AS
        SELECT
            tconst,
            titleType,
            primaryTitle,
            TRY_CAST(isAdult AS INTEGER) AS isAdult,
            TRY_CAST(startYear AS INTEGER) AS startYear,
            genres
        FROM read_csv(
            '{BASICS_FILE.as_posix()}',
            delim='\t',
            header=true,
            nullstr='\\N',
            auto_detect=true
        );
    """)

    con.execute(f"""
        CREATE OR REPLACE TABLE ratings AS
        SELECT
            tconst,
            TRY_CAST(averageRating AS DOUBLE) AS averageRating,
            TRY_CAST(numVotes AS BIGINT) AS numVotes
        FROM read_csv(
            '{RATINGS_FILE.as_posix()}',
            delim='\t',
            header=true,
            nullstr='\\N',
            auto_detect=true
        );
    """)

    if not BASICS_PARQUET.exists():
        con.execute(f"COPY basics TO '{BASICS_PARQUET.as_posix()}' (FORMAT PARQUET);")
    if not RATINGS_PARQUET.exists():
        con.execute(f"COPY ratings TO '{RATINGS_PARQUET.as_posix()}' (FORMAT PARQUET);")

    return con


def explain_text(con: duckdb.DuckDBPyConnection, sql: str) -> str:
    explain_df = con.execute("EXPLAIN ANALYZE " + sql).fetchdf()
    if explain_df.shape[1] >= 2:
        plan_text = str(explain_df.iloc[0, 1])
    else:
        plan_text = explain_df.to_string(index=False)

    cleaned_lines = []
    for line in plan_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("EXPLAIN ANALYZE"):
            continue
        cleaned_lines.append(line.rstrip())

    cleaned_text = "\n".join(cleaned_lines).strip()

    while "\n\n\n" in cleaned_text:
        cleaned_text = cleaned_text.replace("\n\n\n", "\n\n")

    return cleaned_text


def get_source_refs(source_mode: str) -> dict:
    if source_mode == "DuckDB tables":
        return {
            "basics": "basics",
            "ratings": "ratings",
            "label": "Persisted DuckDB tables",
        }

    if source_mode == "Direct TSV.GZ":
        return {
            "basics": f"""
                (
                    SELECT
                        tconst,
                        titleType,
                        primaryTitle,
                        TRY_CAST(isAdult AS INTEGER) AS isAdult,
                        TRY_CAST(startYear AS INTEGER) AS startYear,
                        genres
                    FROM read_csv(
                        '{BASICS_FILE.as_posix()}',
                        delim='\\t',
                        header=true,
                        nullstr='\\\\N',
                        auto_detect=true
                    )
                )
            """,
            "ratings": f"""
                (
                    SELECT
                        tconst,
                        TRY_CAST(averageRating AS DOUBLE) AS averageRating,
                        TRY_CAST(numVotes AS BIGINT) AS numVotes
                    FROM read_csv(
                        '{RATINGS_FILE.as_posix()}',
                        delim='\\t',
                        header=true,
                        nullstr='\\\\N',
                        auto_detect=true
                    )
                )
            """,
            "label": "Direct reads from .tsv.gz files",
        }

    return {
        "basics": f"read_parquet('{BASICS_PARQUET.as_posix()}')",
        "ratings": f"read_parquet('{RATINGS_PARQUET.as_posix()}')",
        "label": "Parquet files",
    }


QUERY_TEMPLATES = {
    "Average rating by year (movies only)": {
        "description": "Join + filter + aggregation. Good for comparing grouped analytical workloads.",
        "sql": """
            SELECT
                b.startYear,
                COUNT(*) AS title_count,
                ROUND(AVG(r.averageRating), 2) AS avg_rating,
                SUM(r.numVotes) AS total_votes
            FROM {basics_ref} b
            JOIN {ratings_ref} r
              ON b.tconst = r.tconst
            WHERE b.titleType = 'movie'
              AND b.isAdult = 0
              AND b.startYear IS NOT NULL
              AND r.numVotes >= 5000
            GROUP BY b.startYear
            ORDER BY b.startYear
        """,
    },
    "Top movies by rating": {
        "description": "Join + filter + Top-N ranking. Useful for observing join, projection, and sort/top operators.",
        "sql": """
            SELECT
                b.primaryTitle,
                b.startYear,
                r.averageRating,
                r.numVotes
            FROM {basics_ref} b
            JOIN {ratings_ref} r
              ON b.tconst = r.tconst
            WHERE b.titleType = 'movie'
              AND b.isAdult = 0
              AND r.numVotes >= 50000
            ORDER BY r.averageRating DESC, r.numVotes DESC
            LIMIT 20
        """,
    },
    "Top genres by total votes": {
        "description": "Join + split/unnest + aggregation. Good example of higher-level app logic mapping to multiple operators.",
        "sql": """
            SELECT
                genre,
                COUNT(*) AS title_count,
                ROUND(AVG(r.averageRating), 2) AS avg_rating,
                SUM(r.numVotes) AS total_votes
            FROM {basics_ref} b
            JOIN {ratings_ref} r
              ON b.tconst = r.tconst,
            UNNEST(string_split(b.genres, ',')) AS g(genre)
            WHERE b.titleType = 'movie'
              AND b.isAdult = 0
              AND b.genres IS NOT NULL
              AND r.numVotes >= 5000
            GROUP BY genre
            ORDER BY total_votes DESC
            LIMIT 20
        """,
    },
    "Movie counts by decade": {
        "description": "Join + filter + grouped aggregation over transformed year values.",
        "sql": """
            SELECT
                CAST(FLOOR(b.startYear / 10) * 10 AS INTEGER) AS decade,
                COUNT(*) AS title_count
            FROM {basics_ref} b
            JOIN {ratings_ref} r
              ON b.tconst = r.tconst
            WHERE b.titleType = 'movie'
              AND b.isAdult = 0
              AND b.startYear IS NOT NULL
            GROUP BY decade
            ORDER BY decade
        """,
    },
    "Top Sci-Fi movies after 2010": {
        "description": "A more selective query to study filter selectivity and Top-N behavior.",
        "sql": """
            SELECT
                b.primaryTitle,
                b.startYear,
                b.genres,
                r.averageRating,
                r.numVotes
            FROM {basics_ref} b
            JOIN {ratings_ref} r
              ON b.tconst = r.tconst
            WHERE b.titleType = 'movie'
              AND b.isAdult = 0
              AND b.startYear >= 2010
              AND b.genres IS NOT NULL
              AND b.genres LIKE '%Sci-Fi%'
              AND r.numVotes >= 100000
            ORDER BY r.averageRating DESC, r.numVotes DESC
            LIMIT 20
        """,
    },
}


def render_query(selected_query: str, source_mode: str) -> str:
    refs = get_source_refs(source_mode)
    sql = QUERY_TEMPLATES[selected_query]["sql"].format(
        basics_ref=refs["basics"],
        ratings_ref=refs["ratings"],
    )
    return sql.strip() + ";"


def run_timed_query(con: duckdb.DuckDBPyConnection, sql: str):
    start = time.perf_counter()
    df = con.execute(sql).fetchdf()
    elapsed = time.perf_counter() - start
    return df, elapsed


con = get_connection()

with st.sidebar:
    st.header("Experiment controls")
    source_mode = st.radio(
        "Query source",
        ["DuckDB tables", "Direct TSV.GZ", "Parquet"],
        index=0,
        help="Use the same SQL logic against different storage representations.",
    )
    st.write(f"**Current source:** {get_source_refs(source_mode)['label']}")

selected = st.selectbox("Choose a query template", list(QUERY_TEMPLATES.keys()))
st.caption(QUERY_TEMPLATES[selected]["description"])
sql = render_query(selected, source_mode)

c1, c2 = st.columns([1.05, 0.95])

with c1:
    st.subheader("SQL")
    st.code(sql, language="sql")

    if st.button("Run query", type="primary"):
        result, elapsed = run_timed_query(con, sql)
        st.subheader("Result")
        st.metric("Execution time", f"{elapsed:.4f} sec")
        st.dataframe(result, use_container_width=True)

with c2:
    st.subheader("EXPLAIN ANALYZE")
    if st.button("Show execution plan"):
        plan_text = explain_text(con, sql)
        st.code(plan_text, language="text")

st.markdown("---")
left, right = st.columns(2)

with left:
    st.subheader("Loaded tables")
    st.dataframe(con.execute("SHOW TABLES").fetchdf(), use_container_width=True)

    st.subheader("DuckDB table sizes")
    counts = con.execute("""
        SELECT 'basics' AS table_name, COUNT(*) AS rows FROM basics
        UNION ALL
        SELECT 'ratings' AS table_name, COUNT(*) AS rows FROM ratings
    """).fetchdf()
    st.dataframe(counts, use_container_width=True)

    st.subheader("Available file sources")
    source_info = pd.DataFrame(
        [
            {"source": "TSV.GZ", "path": BASICS_FILE.name, "exists": BASICS_FILE.exists()},
            {"source": "TSV.GZ", "path": RATINGS_FILE.name, "exists": RATINGS_FILE.exists()},
            {"source": "Parquet", "path": BASICS_PARQUET.name, "exists": BASICS_PARQUET.exists()},
            {"source": "Parquet", "path": RATINGS_PARQUET.name, "exists": RATINGS_PARQUET.exists()},
        ]
    )
    st.dataframe(source_info, use_container_width=True)

with right:
    st.subheader("Schema preview")
    schema_choice = st.radio("Choose a DuckDB table", ["basics", "ratings"], horizontal=True)
    st.dataframe(con.execute(f"DESCRIBE {schema_choice}").fetchdf(), use_container_width=True)

    st.subheader("How to use this app")
    st.markdown(
        """
        1. Pick a query template.  
        2. Choose a source format in the sidebar.  
        3. Run the query and record the execution time.  
        4. Use **Show execution plan** to inspect physical operators.  
        """
    )
