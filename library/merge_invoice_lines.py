import logging

import click
import pandas as pd

# Configure the logger
logging.basicConfig(
    level=logging.INFO,  # Set the minimum level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],  # Log to the console
)

logger = logging.getLogger(__name__)


@click.command()
@click.argument("input_files", nargs=-1, type=click.Path(exists=True))
@click.option("--output", "-o", type=click.Path(), required=True, help="Output file path")
def merge_files(input_files, output):
    """Merge specified Excel files of invoice lines into one file.

    This function takes multiple invoice line Excel files, reads them into pandas
    DataFrames, concatenates them, and saves the result to a single output CSV file.

    Args:
        input_files (tuple of str): Paths to the input CSV files to be merged.
            At least one input file must be provided.
        output (str): Path to the output CSV file where the merged data will be saved.

    Raises:
        click.ClickException: If no input files are provided.

    Example:
        $ python merge_invoice_lines.py input1.csv input2.csv -o output.csv
    """
    if not input_files:
        raise click.ClickException("Error: No input files provided.")

    dfs = []
    for file in input_files:
        df = pd.read_excel(file, sheet_name="sheet1")
        logger.info(f"Loaded file: {file}")
        logger.info(f"  Total entries: {len(df)}")
        if "Invoice Date" in df.columns:
            start_date = df["Invoice Date"].min()
            end_date = df["Invoice Date"].max()
            logger.info(f"  Date range: {start_date} to {end_date}")
        else:
            logger.warning(f"  'Invoice Date' column not found in {file}")
        dfs.append(df)
    sel_cols = [
        "SAP Invoice #",
        "Invoice #",
        "Invoice Date",
        "Header status",
        "PO Number",
        "PO Order Date",
        "PO Status",
        "Commodity",
        "PO Line Commodity",
        "Category",
        "Line #",
        "Total",
        "PO Line #",
        "PO Line Total",
        "Description",
        "Supplier",
        "Supplier #",
        "Billing",
    ]
    merged_df = pd.concat(dfs, ignore_index=True, axis=0)
    # Drop duplicates
    merged_df = merged_df.drop_duplicates()
    # Save the merged data to a specified path
    merged_df[sel_cols].to_csv(output, index=False)
    click.echo(f"Merged {len(input_files)} files into {output}")


if __name__ == "__main__":
    merge_files()
