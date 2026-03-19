from ngio_tables05.common import parse_args, CURRENT_OS
from ngio_tables05.tables import ngio_table_create, ngio_table_validate

args = parse_args()
for zarr_format, lib_name in [(3, "ngio05_v3"), (2, "ngio05_v2")]:
    if args.mode == "create":
        ngio_table_create(args, zarr_format=zarr_format, current_os=CURRENT_OS, current_lib=lib_name)
    elif args.mode == "check":
        ngio_table_validate(args, current_os=CURRENT_OS, current_lib=lib_name)
