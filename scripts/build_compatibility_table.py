
import polars as pl
from pathlib import Path

import polars as pl
from typing import Union

def value_to_indicator(v: Union[int, float, str]) -> str:
    """
    Map numeric value to indicator:
    - 100 -> ✅
    - 0   -> ❌
    - anything else -> original value as string
    """
    # Handle missing values if needed
    if v is None:
        return ""
    try:
        num = float(v)
    except (TypeError, ValueError):
        return str(v)

    if num == 100:
        return "✅"
    elif num == 0:
        return "❌"
    else:
        # Keep the original representation (no trailing .0 for ints)
        return str(int(num)) if num.is_integer() else str(num)


def df_to_compat_markdown(
    df: pl.DataFrame,
    reader_col: str = "reader",
    table_title_header: str = "Reader \\ Writer",
) -> str:
    """
    Convert a wide Polars DataFrame into a Markdown compatibility matrix.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame where:
        - One column (`reader_col`) contains the reader names
        - All other columns are writers with numeric compat values
    reader_col : str
        Name of the column containing readers (row labels).
    table_title_header : str
        Text to show in the top-left cell of the Markdown table.

    Returns
    -------
    str
        Markdown table as a string.
    """
    if reader_col not in df.columns:
        raise ValueError(f"Column '{reader_col}' not found in DataFrame")

    # Writers are all columns except the reader label column
    writer_cols = [c for c in df.columns if c != reader_col]

    # Header row
    header_cells = [table_title_header] + writer_cols
    header_row = "| " + " | ".join(header_cells) + " |"

    # Separator row
    separator_row = "| " + " | ".join(["-" * len(c) for c in header_cells]) + " |"

    # Data rows
    data_rows = []
    for row in df.iter_rows(named=True):
        reader_name = str(row[reader_col])
        row_cells = [reader_name]
        for w in writer_cols:
            row_cells.append(value_to_indicator(row[w]))
        data_rows.append("| " + " | ".join(row_cells) + " |")

    # Combine everything
    markdown = "\n".join([header_row, separator_row] + data_rows)
    return markdown

def main():
    all_csv = Path('./tables/').glob('*.csv')

    df_list = []
    for csv_file in all_csv:
        df = pl.read_csv(csv_file)
        df_list.append(df)
    combined_df = pl.concat(df_list, how="vertical")

    combined_df = combined_df.select(["reader", "writer", "status"])
    combined_df = combined_df.with_columns([
        (pl.when(pl.col("status") == "success").then(1).otherwise(0)).alias("status")
    ])
    pivoted_df =combined_df.pivot(
        values="status",
        index="reader",
        on="writer",
        aggregate_function="mean"
        )

    # Transfrom fractions to percentages
    pivoted_df = pivoted_df.with_columns([
        (pl.col(col) * 100).cast(pl.Int64) for col in pivoted_df.columns if col != "reader"
    ])
    pivoted_df = pivoted_df.sort("reader")
    pivoted_df = pivoted_df.select(
        ["reader"] + sorted([col for col in pivoted_df.columns if col != "reader"])
    )

    markdown_table = df_to_compat_markdown(pivoted_df)
    
    with open("./_README_base.md", "r") as f:
        base_content = f.read()
    
    with open("./README.md", "w") as f:
        f.write(f"{base_content}\n\n## Compatibility Table\n\n{markdown_table}\n")
        
        
if __name__ == "__main__":
    main()