from pathlib import Path
import sqlite3
import pandas as pd

CLEAN_DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "cleaned_data.csv"
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "ecommerce.db"
TABLE_NAME = "orders"


def load_clean_data() -> pd.DataFrame:
    return pd.read_csv(CLEAN_DATA_PATH)


def connect_db() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def write_table(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)


def verify_table(conn: sqlite3.Connection) -> None:
    query = f"SELECT COUNT(*) AS row_count FROM {TABLE_NAME}"
    result = pd.read_sql_query(query, conn)
    print("=== TABLE VERIFICATION ===")
    print(result)
    print()



def main() -> None:
    df = load_clean_data()
    conn = connect_db()

    try:
        write_table(df, conn)
        verify_table(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()