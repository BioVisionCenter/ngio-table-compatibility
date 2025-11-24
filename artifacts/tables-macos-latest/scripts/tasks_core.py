from argparse import ArgumentParser
from pathlib import Path
from common import SAMPLE_FEATURE_TABLE_DF, compare_dataframes, TableCheckResult, save_results_to_csv
import os
# from fractal_tasks_core.tables import 

CURRENT_OS = os.uname().sysname.lower()
CURRENT_LIB = Path(__file__).stem

def parse_args():
    parser = ArgumentParser(description="Create sample NGIO tables.")
    parser.add_argument(
        "--mode",
        choices=["create", "check"],
        type=str,
        required=True,
        help="Mode to run the script in.",
    )
    parser.add_argument(
        "--dir",
        type=str,
        required=True,
        help="Path to save the created table.",
    )
    return parser.parse_args()


def create_sample_feature_table(file_path, backend):
    # Create a sample FeatureTable and write it to the specified file path
    feature_table = FeatureTable(table_data=SAMPLE_FEATURE_TABLE_DF)
    # Add sample data to the feature_table as needed
    write_table(store=file_path, table=feature_table, backend=backend)
        

def check_sample_feature_table(file_path) -> TableCheckResult:
    # Read the FeatureTable from the specified file path
    
    # Extract os_name and lib_name from
    table_os, table_lib, table_backend, _ = file_path.as_posix().split("/")[-4:]
    try:
        feature_table = open_table(store=file_path).dataframe
    except Exception as e:
        error_msg = f"""Error reading file {file_path}:
            - OS: {table_os}
            - Writing Library: {table_lib}
            - Backend: {table_backend}
            - Details: {str(e)}"""
        return TableCheckResult(
            reader=CURRENT_LIB,
            reader_os=CURRENT_OS,
            writer=table_lib,
            writer_os=table_os,
            backend=table_backend,
            table_type="FeatureTable",
            status="failure",
            details=error_msg
        )
    # Comare with the sample data
    status = compare_dataframes(feature_table, SAMPLE_FEATURE_TABLE_DF)
    if status is not None:
        error_msg = f"""Data mismatch in file {file_path}:
            - OS: {table_os}
            - Writing Library: {table_lib}
            - Backend: {table_backend}
            - Details: {status}"""
        return TableCheckResult(
            reader=CURRENT_LIB,
            reader_os=CURRENT_OS,
            writer=table_lib,
            writer_os=table_os,
            backend=table_backend,
            table_type="FeatureTable",
            status="failure",
            details=error_msg
        )
    return TableCheckResult(
        reader=CURRENT_LIB,
        reader_os=CURRENT_OS,
        writer=table_lib,
        writer_os=table_os,
        backend=table_backend,
        table_type="FeatureTable",
        status="success",
        details="",
    )
        

def create(args):
    base = Path(args.dir)
    base.mkdir(parents=True, exist_ok=True)
    for backend in ["anndata", "json", "csv", "parquet"]:
        path = base / CURRENT_OS / CURRENT_LIB / f"{backend}" / "feature_table.zarr"
        create_sample_feature_table(path, backend)
        
def check(args):
    base = Path(args.dir)
    assert base.exists(), "Directory does not exist."
    lib_base = base.glob("*/*")
    results = []
    for lib_path in lib_base:
        for backend in ["anndata", "json", "csv", "parquet"]:
            path = lib_path / f"{backend}" / "feature_table.zarr"
            current_result = check_sample_feature_table(path)
            results.append(current_result)
    
    save_results_to_csv(results=results, output_path=base / "check_results.csv")

if __name__ == "__main__":
    args = parse_args()
    if args.mode == "create":
        create(args)
    elif args.mode == "check":
        result = check(args)