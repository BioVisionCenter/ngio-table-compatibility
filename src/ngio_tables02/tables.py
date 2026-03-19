from typing import Literal
from ngio.tables.tables_container import write_table, open_table
from ngio.tables.v1._roi_table import _dataframe_to_rois, _rois_to_dataframe
from ngio.tables import FeatureTable, RoiTable, MaskingRoiTable
from pathlib import Path
from ngio_tables02.common import SAMPLE_FEATURE_TABLE_DF, compare_dataframes, TableCheckResult, save_results_to_csv, SAMPLE_MASKING_ROI_TABLE_DF, SAMPLE_ROI_TABLE_DF
import zarr
import pandas as pd


def create_group(path: Path):
    return zarr.group(path, overwrite=True)


def create_sample_feature_table(dir_path: Path):
    feature_table = FeatureTable(dataframe=SAMPLE_FEATURE_TABLE_DF)
    group = create_group(dir_path / "feature_table.zarr")
    write_table(store=group, table=feature_table, backend="anndata_v1")


def create_sample_roi_table(dir_path: Path):
    rois = _dataframe_to_rois(SAMPLE_ROI_TABLE_DF)
    roi_table = RoiTable(rois=rois.values())
    group = create_group(dir_path / "roi_table.zarr")
    write_table(store=group, table=roi_table, backend="anndata_v1")


def create_sample_masking_roi_table(dir_path: Path):
    rois = _dataframe_to_rois(SAMPLE_MASKING_ROI_TABLE_DF)
    masking_roi_table = MaskingRoiTable(rois=rois.values())
    group = create_group(dir_path / "masking_roi_table.zarr")
    write_table(store=group, table=masking_roi_table, backend="anndata_v1")


def check_table(dir_path: Path,
                current_lib: str,
                table_name: str,
                reference_df: pd.DataFrame,
                table_type: Literal["feature_table", "roi_table", "masking_roi_table"]) -> TableCheckResult:
    table_lib = dir_path.as_posix().split("/")[-2]
    file_path = dir_path / f"{table_name}.zarr"
    try:
        table = open_table(store=file_path)
        if table.type() != table_type:
            raise ValueError(f"Expected table type {table_type} but got {table.type()}")
        if hasattr(table, 'dataframe'):
            table = table.dataframe
        else:
            # RoiTableV1 / MaskingRoiTableV1 expose rois, not a dataframe property
            index_key = table._index_key()
            table = _rois_to_dataframe(table._rois, index_key=index_key)
            # Cast index to match expected type (ngio02 stores label indices as strings)
            if reference_df.index.dtype != table.index.dtype:
                table.index = table.index.astype(reference_df.index.dtype)
    except Exception as e:
        return TableCheckResult(
            reader=current_lib,
            writer=table_lib,
            backend="anndata",
            table_type=table_type,
            status="failure",
            details=f"Failed to read table with error: {str(e)}"
        )
    status = compare_dataframes(table, reference_df)
    if status is not None:
        return TableCheckResult(
            reader=current_lib,
            writer=table_lib,
            backend="anndata",
            table_type=table_type,
            status="failure",
            details=f"Table content mismatch with error: {status}"
        )
    return TableCheckResult(
        reader=current_lib,
        writer=table_lib,
        backend="anndata",
        table_type=table_type,
        status="success",
        details="",
    )


def check_sample_feature_table(file_dir: Path, current_lib: str) -> TableCheckResult:
    return check_table(
        dir_path=file_dir,
        current_lib=current_lib,
        table_name="feature_table",
        reference_df=SAMPLE_FEATURE_TABLE_DF,
        table_type="feature_table"
    )


def check_sample_roi_table(file_dir: Path, current_lib: str) -> TableCheckResult:
    return check_table(
        dir_path=file_dir,
        current_lib=current_lib,
        table_name="roi_table",
        reference_df=SAMPLE_ROI_TABLE_DF,
        table_type="roi_table",
    )


def check_sample_masking_roi_table(file_dir: Path, current_lib: str) -> TableCheckResult:
    return check_table(
        dir_path=file_dir,
        current_lib=current_lib,
        table_name="masking_roi_table",
        reference_df=SAMPLE_MASKING_ROI_TABLE_DF,
        table_type="masking_roi_table",
    )


def ngio_table_create(args, zarr_format: int, current_lib: str):
    base = Path(args.dir).absolute()
    base.mkdir(parents=True, exist_ok=True)
    table_dir = base / current_lib / "anndata"
    create_sample_feature_table(table_dir)
    create_sample_masking_roi_table(table_dir)
    create_sample_roi_table(table_dir)


def ngio_table_validate(args, current_lib: str):
    root = Path(args.dir).absolute()
    base = (p for p in root.glob("*") if p.is_dir())
    results = []
    for base_pp in base:
        dir_path = base_pp / "anndata"
        if not dir_path.exists():
            continue
        for testing_function in [check_sample_feature_table, check_sample_roi_table, check_sample_masking_roi_table]:
            current_result = testing_function(dir_path, current_lib=current_lib)
            results.append(current_result)

    save_results_to_csv(results=results, output_path=root.parent / "check_results.csv")
