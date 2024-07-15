"""Split one large CSV file into multiple files"""

import os
import time
from concurrent.futures import Future, ProcessPoolExecutor
from typing import List

import pandas as pd


def get_column_names(df: pd.DataFrame, columns: List[int]):
    """Get column names from column indices"""
    colume_names = []
    for col_idx in columns:
        colume_names.append(df.columns[col_idx])
    return colume_names


def process_chunk(chunk: pd.DataFrame, output_dir: str, subset: List[int]):
    """Process each chunk of the large file"""
    columns = get_column_names(chunk, subset)
    grouped = chunk.groupby(columns)

    for group_key, group_df in grouped:
        # Create a filename based on the unique combination of C and D
        slug = "_".join(group_key)
        filename = f"{output_dir}/output_{slug}.csv"

        # Write to CSV (append if the file exists)
        # skip headers if on appending
        header = not os.path.exists(filename)
        group_df.to_csv(filename, mode="a", header=header, index=False)

    return


def split_large_csv(
    file_path: str,
    output_dir: str,
    subset: List[str | int] = None,
    chunk_size=10**6,
):
    """
    Split one large CSV file into multiple files
    based on the value combinations of selected columns
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with pd.read_csv(file_path, chunksize=chunk_size) as reader:
        with ProcessPoolExecutor() as executor:
            futures: List[Future] = []
            for chunk in reader:
                futures.append(
                    executor.submit(process_chunk, chunk, output_dir, subset)
                )

            # Wait for all futures to complete
            for future in futures:
                future.result()


def verify_processing(original_file, output_dir, subset):
    with open(original_file, "r", encoding="utf-8") as f:
        original_count = sum(1 for _ in f) - 1  # -1 for header
        print(f"Total rows in original file (excluding header): {original_count}")

    unique_combinations = set()
    output_files = []
    output_rows_count = 0

    for filename in os.listdir(output_dir):
        if filename.endswith(".csv"):
            output_files.append(filename)
            df = pd.read_csv(os.path.join(output_dir, filename))
            # Increase total rows count
            output_rows_count += df.shape[0]

            # Extract C and D values from filename
            combination = "_".join(filename[:-4].split("_")[1:3])
            unique_combinations.add(combination)

            # Validate that rows match the filename
            columns = get_column_names(df, subset)
            if (
                not df[columns]
                .astype(str)
                .apply("_".join, axis=1)
                .isin([combination])
                .all()
            ):
                print(f"Data integrity issue in file: {filename}")
                print("Combination", combination)
                print("Data")
                print(df[columns].astype(str).apply("_".join, axis=1))

    print(f"Total rows across output files: {output_rows_count}")

    # Validate that total rows match
    if original_count == output_rows_count:
        print("Row count matches between original and output files.")
    else:
        print("Row count mismatch between original and output files.")
    print(f"Number of unique combinations found: {len(unique_combinations)}")
    print(f"Number of output files created: {len(output_files)}")

    # Validate number of output files matches unique combinations
    if len(unique_combinations) != len(output_files):
        print("Mismatch in the number of unique combinations and output files!")


if __name__ == "__main__":
    INPUT_FILE = "data/Main File for Splitting.csv"
    OUTPUT_FOLDER = "output"

    s = time.time()
    split_large_csv(INPUT_FILE, OUTPUT_FOLDER, [2, 3])
    print("Splitting done in:", round(time.time() - s, 2), "seconds")

    # Usage
    verify_processing(INPUT_FILE, OUTPUT_FOLDER, [2, 3])
