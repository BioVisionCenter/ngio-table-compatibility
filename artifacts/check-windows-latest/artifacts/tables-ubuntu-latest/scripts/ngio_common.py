from ngio.tables._tables_container import write_table, open_table
from ngio.tables.v1._roi_table import _dataframe_to_rois
from ngio.tables import FeatureTable, RoiTable, MaskingRoiTable, ConditionTable
from pathlib import Path
from common import SAMPLE_FEATURE_TABLE_DF, compare_dataframes, TableCheckResult, save_results_to_csv, SAMPLE_MASKING_ROI_TABLE_DF, SAMPLE_ROI_TABLE_DF
import zarr
import pandas as pd

def create_group(path: Path, zarr_format: int):
    if zarr.__version__.startswith("2."):
        return zarr.group(path, overwrite=True)
    else:
        return zarr.create_group(path, zarr_format=zarr_format, overwrite=True) # type: ignore


def create_sample_feature_table(dir_path: Path, backend, zarr_format: int = 2):
    # Create a sample FeatureTable and write it to the specified file path
    feature_table = FeatureTable(table_data=SAMPLE_FEATURE_TABLE_DF)
    # Add sample data to the feature_table as needed
    group = create_group(dir_path / "feature_table.zarr", zarr_format=zarr_format)
    write_table(store=group, table=feature_table, backend=backend)
    
    
def create_sample_roi_table(dir_path: Path, backend, zarr_format: int = 2):
    # Create a sample RoiTable and write it to the specified file path
    rois = _dataframe_to_rois(SAMPLE_ROI_TABLE_DF)
    roi_table = RoiTable(rois=rois.values())
    # Add sample data to the roi_table as needed
    group = create_group(dir_path / "roi_table.zarr", zarr_format=zarr_format)
    write_table(store=group, table=roi_table, backend=backend)
    
def create_sample_masking_roi_table(dir_path: Path, backend, zarr_format: int = 2):
    # Create a sample MaskingRoiTable and write it to the specified file path
    rois = _dataframe_to_rois(SAMPLE_MASKING_ROI_TABLE_DF)
    masking_roi_table = MaskingRoiTable(rois=rois.values())
    # Add sample data to the masking_roi_table as needed
    group = create_group(dir_path / "masking_roi_table.zarr", zarr_format=zarr_format)
    write_table(store=group, table=masking_roi_table, backend=backend)
    
    
def check_table(dir_path: Path, 
                current_os: str, 
                current_lib: str, 
                table_name: str, 
                reference_df: pd.DataFrame, 
                table_type: str) -> TableCheckResult:
    # Read the table from the specified file path
    table_os, table_lib, table_backend = dir_path.as_posix().split("/")[-3:]
    file_path = dir_path / f"{table_name}.zarr"
    try:
        table = open_table(store=file_path).dataframe
    except Exception as e:
        return TableCheckResult(
            reader=current_lib,
            reader_os=current_os,
            writer=table_lib,
            writer_os=table_os,
            backend=table_backend,
            table_type=table_type,
            status="failure",
            details=f"Failed to read table with error: {str(e)}"
        )
    # Comare with the sample data
    status = compare_dataframes(table, reference_df)
    if status is not None:
        return TableCheckResult(
            reader=current_lib,
            reader_os=current_os,
            writer=table_lib,
            writer_os=table_os,
            backend=table_backend,
            table_type=table_type,
            status="failure",
            details=f"Table content mismatch with error: {status}"
        )
    return TableCheckResult(
        reader=current_lib,
        reader_os=current_os,
        writer=table_lib,
        writer_os=table_os,
        backend=table_backend,
        table_type=table_type,
        status="success",
        details="",
    )

def check_sample_feature_table(file_dir: Path, current_os: str, current_lib: str) -> TableCheckResult:
    return check_table(
        dir_path=file_dir,
        current_os=current_os,
        current_lib=current_lib,
        table_name="feature_table",
        reference_df=SAMPLE_FEATURE_TABLE_DF,
        table_type="FeatureTable"
    )
    
def check_sample_roi_table(file_dir: Path, current_os: str, current_lib: str) -> TableCheckResult:
    return check_table(
        dir_path=file_dir,
        current_os=current_os,
        current_lib=current_lib,
        table_name="roi_table",
        reference_df=SAMPLE_ROI_TABLE_DF,
        table_type="RoiTable"
    )

def check_sample_masking_roi_table(file_dir: Path, current_os: str, current_lib: str) -> TableCheckResult:
    return check_table(
        dir_path=file_dir,
        current_os=current_os,
        current_lib=current_lib,
        table_name="masking_roi_table",
        reference_df=SAMPLE_MASKING_ROI_TABLE_DF,
        table_type="MaskingRoiTable"
    )

def ngio_table_create(args, zarr_format: int, current_os: str, current_lib):
    base = Path(args.dir).absolute()
    base.mkdir(parents=True, exist_ok=True)
    for backend in ["anndata", "json", "csv", "parquet"]:
        table_dir = base / f"{current_os}" / f"{current_lib}" / f"{backend}"
        create_sample_feature_table(table_dir, backend, zarr_format=zarr_format)
        create_sample_masking_roi_table(table_dir, backend, zarr_format=zarr_format)
        create_sample_roi_table(table_dir, backend, zarr_format=zarr_format)
        
def ngio_table_validate(args, current_os: str, current_lib: str):
    root = Path(args.dir).absolute()
    base = root.glob("*/*")
    results = []
    for base_pp in base:
        for backend in ["anndata", "json", "csv", "parquet"]:
            for testing_function in [check_sample_feature_table, check_sample_roi_table, check_sample_masking_roi_table]:
                dir_path = base_pp / f"{backend}"
                current_result = testing_function(dir_path, current_os=current_os, current_lib=current_lib)
                results.append(current_result)
    
    save_results_to_csv(results=results, output_path=root / f"{current_os}_check_results.csv")