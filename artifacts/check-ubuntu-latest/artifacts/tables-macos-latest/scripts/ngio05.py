from ngio_common import ngio_table_create, ngio_table_validate
from common import parse_args, CURRENT_OS
from pathlib import Path


if __name__ == "__main__":
    CURRENT_LIB = Path(__file__).stem
    args = parse_args()
        
    if args.mode == "create":
        ngio_table_create(args, zarr_format=3, current_os=CURRENT_OS, current_lib=f"{CURRENT_LIB}_v3")
    elif args.mode == "check":
        ngio_table_validate(args, current_os=CURRENT_OS, current_lib=f"{CURRENT_LIB}_v3")
    
    if args.mode == "create":
        ngio_table_create(args, zarr_format=2, current_os=CURRENT_OS, current_lib=f"{CURRENT_LIB}_v2")
    elif args.mode == "check":
        ngio_table_validate(args, current_os=CURRENT_OS, current_lib=f"{CURRENT_LIB}_v2")