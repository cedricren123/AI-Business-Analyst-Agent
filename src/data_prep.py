from io import StringIO
from pathlib import Path

import pandas as pd


DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "ecommerce_customer_behavior_dataset_v2.csv"
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "cleaned_data.csv"


def load_data() -> pd.DataFrame:
    return pd.read_csv(DATA_PATH)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "", regex=True)
    )

    df["date"] = pd.to_datetime(df["date"])

    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["has_discount"] = df["discount_amount"] > 0
    df["gross_amount"] = df["unit_price"] * df["quantity"]
    df["discount_rate"] = (
        df["discount_amount"]
        .div(df["gross_amount"].where(df["gross_amount"] != 0))
        .fillna(0)
    )
    df["customer_type"] = df["is_returning_customer"].map({
        True: "returning",
        False: "new"
    })

    money_columns = ["unit_price", "discount_amount", "total_amount", "gross_amount"]
    df[money_columns] = df[money_columns].round(2)
    df["discount_rate"] = df["discount_rate"].round(4)

    return df



def print_dataset_overview(df: pd.DataFrame) -> None:
    print("=== DATASET OVERVIEW ===")
    print(f"Rows, columns: {df.shape}")
    print()

    print("=== COLUMN NAMES ===")
    for column in df.columns:
        print(f"- {column}")
    print()

    print("=== DATAFRAME INFO ===")
    buffer = StringIO()
    df.info(buf=buffer)
    print(buffer.getvalue())

    print("=== MISSING VALUES PER COLUMN ===")
    print(df.isnull().sum())
    print()

    print("=== FIRST 5 ROWS ===")
    print(df.head())
    print()


def check_for_duplicates(df: pd.DataFrame) -> None:
    duplicate_rows = df.duplicated().sum()
    duplicate_order_ids = df["order_id"].duplicated().sum()

    print("=== DUPLICATE CHECK ===")
    print(f"Duplicate rows: {duplicate_rows}")
    print(f"Duplicate Order_ID values: {duplicate_order_ids}")
    print()


def validate_total_amount(df: pd.DataFrame) -> None:
    expected_total = (df["unit_price"] * df["quantity"] - df["discount_amount"]).round(2)
    actual_total = df["total_amount"].round(2)
    mismatched_rows = (expected_total != actual_total).sum()

    print("=== TOTAL AMOUNT VALIDATION ===")
    print(f"Rows where Total_Amount does not match the formula: {mismatched_rows}")
    print()

def run_validation_checks(df: pd.DataFrame) -> None:
    validate_total_amount(df)
    check_for_duplicates(df)


def save_clean_data(df: pd.DataFrame) -> None:
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Cleaned CSV saved to disk: {OUTPUT_PATH}")


def main() -> None:
    df = load_data()
    df = clean_data(df)
    print_dataset_overview(df)
    run_validation_checks(df)
    save_clean_data(df)
    


if __name__ == "__main__":
    main()
