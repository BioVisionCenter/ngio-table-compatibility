from typing import Literal
import pandas as pd
from pydantic import BaseModel
from pathlib import Path
from argparse import ArgumentParser
import platform

CURRENT_OS = platform.system().lower()

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


SAMPLE_FEATURE_TABLE_DF = pd.DataFrame({
    "label": [1, 2, 3],
    "mean_intensity": [100.0, 150.5, 200.2],
    "area": [50, 75, 100]
})
SAMPLE_FEATURE_TABLE_DF.set_index("label", inplace=True)

SAMPLE_ROI_TABLE_DF = pd.DataFrame({
    "FieldIndex": ["FOV1", "FOV2", "FOV3"],
    "x_micrometer": [10.0, 20.5, 30.2],
    "y_micrometer": [15.0, 25.5, 35.2],
    "z_micrometer": [5.0, 10.5, 15.2],
    "len_x_micrometer": [2.0, 3.5, 4.2],
    "len_y_micrometer": [2.5, 3.0, 4.5],
    "len_z_micrometer": [1.0, 1.5, 2.2],
    "x_micrometer_origin": [-1.0, -2.5, -3.2],
    "y_micrometer_origin": [1.3, -2.3, -3.3],
})
SAMPLE_ROI_TABLE_DF.set_index("FieldIndex", inplace=True)

SAMPLE_MASKING_ROI_TABLE_DF = pd.DataFrame({
    "label": [1, 2, 3],
    "x_micrometer": [5.0, 15.5, 25.2],
    "y_micrometer": [10.0, 20.5, 30.2],
    "z_micrometer": [2.0, 7.5, 12.2],
    "len_x_micrometer": [1.0, 2.5, 3.2],
    "len_y_micrometer": [1.5, 2.0, 3.5],
    "len_z_micrometer": [0.5, 1.0, 1.7],
})
SAMPLE_MASKING_ROI_TABLE_DF.set_index("label", inplace=True)


def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> None | str:
    try:
        pd.testing.assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1))
        return None
    except Exception as e:
        return str(e)
    
    
class TableCheckResult(BaseModel):
    reader: str
    reader_os: str
    writer: str
    writer_os: str
    backend: Literal["anndata", "json", "csv", "parquet"]
    table_type: Literal["FeatureTable", "RoiTable", "MaskingRoiTable", "ConditionTable"]
    status: Literal["success", "failure"]
    details: str = ""
    
    
def save_results_to_csv(results: list[TableCheckResult], output_path: Path) -> None:
    results_df = pd.DataFrame([r.model_dump() for r in results])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        current_results = pd.read_csv(output_path)
        results_df = pd.concat([current_results, results_df], ignore_index=True)
    results_df.to_csv(output_path, index=False)