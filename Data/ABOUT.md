# About `Data/`

This directory stores artifacts produced by the download and aggregation
workflow. It is not intended for manual editing, but knowing the layout is
useful when running the scripts:

- **`Data/CSV/`** – Target location for all municipal and state CSV exports from
  `municipal_trade_download_2.py`. It also contains the Stata catalogs
  `donefiles.dta`, `donefiles_mun.dta`, and `donefiles_sta.dta` that record which
  files already exist.
- **`Data/DTA/`** – Optional output folder for aggregated `.dta` files produced by
  `aggregate_csvs.py` (see `Code/01_append_donefiles.do` for an example of writing
  labeled outputs here).

Directories are created automatically by the Stata do-files, but you can also
make them manually with `mkdir -p Data/CSV Data/DTA` before running the
pipeline.
