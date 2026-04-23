from pathlib import Path
import time
import duckdb

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = BASE_DIR / "imdb_real.duckdb"

BASICS_FILE = DATA_DIR / "title.basics.tsv.gz"
RATINGS_FILE = DATA_DIR / "title.ratings.tsv.gz"


def require_files():
    missing = [p.name for p in [BASICS_FILE, RATINGS_FILE] if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing file(s) in the data folder: "
            + ", ".join(missing)
            + "\nExpected paths:\n"
            + f"- {BASICS_FILE}\n- {RATINGS_FILE}"
        )


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
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

    con.execute(f"""
        COPY basics TO '{(DATA_DIR / "title.basics.parquet").as_posix()}' (FORMAT PARQUET);
    """)
    con.execute(f"""
        COPY ratings TO '{(DATA_DIR / "title.ratings.parquet").as_posix()}' (FORMAT PARQUET);
    """)


def print_section(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def time_query(con: duckdb.DuckDBPyConnection, query: str):
    start = time.perf_counter()
    df = con.execute(query).fetchdf()
    elapsed = time.perf_counter() - start
    return df, elapsed


def main():
    require_files()
    con = duckdb.connect(str(DB_PATH))
    create_tables(con)

    print_section("1) DATABASE INSTALLED / CONFIGURED")
    print(f"DuckDB database path: {DB_PATH}")

    print_section("2) INITIAL SCHEMA / DATA MODEL")
    print("SHOW TABLES")
    print(con.execute("SHOW TABLES").fetchdf().to_string(index=False))

    print("\nDESCRIBE basics")
    print(con.execute("DESCRIBE basics").fetchdf().to_string(index=False))

    print("\nDESCRIBE ratings")
    print(con.execute("DESCRIBE ratings").fetchdf().to_string(index=False))

    print_section("3) BASIC DATA CHECKS")
    print("Row counts")
    print(con.execute("""
        SELECT 'basics' AS table_name, COUNT(*) AS rows FROM basics
        UNION ALL
        SELECT 'ratings' AS table_name, COUNT(*) AS rows FROM ratings
    """).fetchdf().to_string(index=False))

    print("\nSample joined rows")
    print(con.execute("""
        SELECT
            b.primaryTitle,
            b.titleType,
            b.startYear,
            r.averageRating,
            r.numVotes
        FROM basics b
        JOIN ratings r
          ON b.tconst = r.tconst
        WHERE b.titleType = 'movie'
          AND b.startYear IS NOT NULL
        ORDER BY r.numVotes DESC
        LIMIT 10
    """).fetchdf().to_string(index=False))

    query = """
        SELECT
            b.startYear,
            COUNT(*) AS title_count,
            ROUND(AVG(r.averageRating), 2) AS avg_rating,
            SUM(r.numVotes) AS total_votes
        FROM basics b
        JOIN ratings r
          ON b.tconst = r.tconst
        WHERE b.titleType = 'movie'
          AND b.isAdult = 0
          AND b.startYear IS NOT NULL
          AND r.numVotes >= 5000
        GROUP BY b.startYear
        ORDER BY total_votes DESC
        LIMIT 20;
    """

    print_section("4) WORKING QUERY")
    result_df, elapsed = time_query(con, query)
    print(result_df.to_string(index=False))
    print(f"\nExecution time on persisted DuckDB tables: {elapsed:.4f} seconds")

    print_section("5) EXPLAIN ANALYZE")
    explain_df = con.execute("EXPLAIN ANALYZE " + query).fetchdf()
    if explain_df.shape[1] >= 2:
        print(explain_df.iloc[0, 1])
    else:
        print(explain_df.to_string(index=False))

    csv_query = f"""
        SELECT
            b.startYear,
            COUNT(*) AS title_count,
            ROUND(AVG(r.averageRating), 2) AS avg_rating,
            SUM(r.numVotes) AS total_votes
        FROM read_csv('{BASICS_FILE.as_posix()}', delim='\t', header=true, nullstr='\\N', auto_detect=true) b
        JOIN read_csv('{RATINGS_FILE.as_posix()}', delim='\t', header=true, nullstr='\\N', auto_detect=true) r
          ON b.tconst = r.tconst
        WHERE b.titleType = 'movie'
          AND TRY_CAST(b.isAdult AS INTEGER) = 0
          AND TRY_CAST(b.startYear AS INTEGER) IS NOT NULL
          AND TRY_CAST(r.numVotes AS BIGINT) >= 5000
        GROUP BY b.startYear
        ORDER BY total_votes DESC
        LIMIT 20;
    """

    parquet_query = f"""
        SELECT
            b.startYear,
            COUNT(*) AS title_count,
            ROUND(AVG(r.averageRating), 2) AS avg_rating,
            SUM(r.numVotes) AS total_votes
        FROM read_parquet('{(DATA_DIR / "title.basics.parquet").as_posix()}') b
        JOIN read_parquet('{(DATA_DIR / "title.ratings.parquet").as_posix()}') r
          ON b.tconst = r.tconst
        WHERE b.titleType = 'movie'
          AND b.isAdult = 0
          AND b.startYear IS NOT NULL
          AND r.numVotes >= 5000
        GROUP BY b.startYear
        ORDER BY total_votes DESC
        LIMIT 20;
    """

    print_section("6) OPTIONAL CSV VS PARQUET CHECK")
    _, csv_time = time_query(con, csv_query)
    _, parquet_time = time_query(con, parquet_query)
    print(f"Direct .tsv.gz query time: {csv_time:.4f} seconds")
    print(f"Parquet query time:        {parquet_time:.4f} seconds")

    print_section("DONE")
    print("Checkpoint evidence available:")
    print("- DuckDB installed and configured")
    print("- Real IMDb data loaded")
    print("- basics and ratings tables created")
    print("- One working analytical query")
    print("- EXPLAIN ANALYZE output")
    print("- Optional .tsv.gz vs Parquet comparison")


if __name__ == "__main__":
    main()
