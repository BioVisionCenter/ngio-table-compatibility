from ngio_tables04.common import parse_args
from ngio_tables04.tables import ngio_table_create, ngio_table_validate

CURRENT_LIB = "ngio04"
args = parse_args()
if args.mode == "create":
    ngio_table_create(args, zarr_format=2, current_lib=CURRENT_LIB)
elif args.mode == "check":
    ngio_table_validate(args, current_lib=CURRENT_LIB)
