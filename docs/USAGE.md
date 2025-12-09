# Usage Guide

This guide walks through the end-to-end workflow for downloading municipal and
state trade data from DataMéxico, running the supporting Python utilities, and
building analysis-ready Stata datasets.

## 1. Batch download with Stata

The main batch driver is `Code/00_municipal_batch.do`. It builds per-state
command files that call the Python downloader for every municipality×month
combination and avoids re-downloading files that already exist.

Steps:

1. Open Stata and set the working directory to the repository root (adjust the
   `local disk` value in the do-file if your data live elsewhere).
2. Run the do-file:

   ```stata
   do Code/00_municipal_batch.do
   ```

   What happens under the hood:

   - `Code/donefiles_catalog.py` scans `Data/CSV/` and writes
     `Data/CSV/donefiles.dta`, which Stata uses to skip completed downloads.
   - A full calendar of months between `start_year` and `end_year` is crossed
     with all municipalities to create one download command per combination.
   - Per-state batch files are saved in `Code/batch/`, and `myscript.sh` launches
     them in parallel with Stata-MP. Uncomment the final `!./myscript.sh` line to
     start downloads immediately, or execute the batch files manually.

## 2. Individual downloads with Python

For ad-hoc pulls, call the downloader directly without Stata:

```bash
python Code/municipal_trade_download_2.py \
    --month 201012 \
    --municipality 1001 \
    --flows 2 \
    --drilldowns Month,Municipality,HS6,Flow,Country \
    --output Data/CSV/mun1001_201012_exports.csv
```

- Use `--state` instead of `--municipality` to aggregate by state.
- `--drilldowns` controls cube dimensions; defaults are provided based on the
  geography you choose.
- Pass `--json-file path/to/api_response.json` to convert a saved API payload to
  CSV without making any network request.

## 3. Cataloging and aggregating CSV outputs

Once downloads finish, you can combine the outputs into a single Stata dataset.
This is optional but helpful for downstream analysis.

1. **Create or refresh the donefile catalog.** This lists the CSVs on disk and is
   reused by both Stata and Python utilities:

   ```bash
   python Code/donefiles_catalog.py \
       --csv-dir Data/CSV \
       --start-year 2006 \
       --end-year 2025 \
       --output Data/CSV/donefiles.dta
   ```

   The do-file `Code/01_append_donefiles.do` demonstrates how to split this
   catalog into municipal (`donefiles_mun.dta`) and state (`donefiles_sta.dta`)
   subsets for separate aggregation runs.

2. **Aggregate CSVs in parallel.** Pick the appropriate donefile and run:

   ```bash
   python Code/aggregate_csvs.py \
       --donefile Data/CSV/donefiles_mun.dta \
       --csv-dir Data/CSV \
       --output Data/DTA/all_trade_mun.dta \
       --workers 12 \
       --batch-size 2000
   ```

   Notes:
   - Requires `pandas` and `pyarrow` (`pip install pandas pyarrow`).
   - Columns missing from individual CSVs are filled with `NA` to keep the
     schema consistent.
   - Intermediate Parquet shards are written under `<output>.parts/` to manage
     memory usage and speed up the final Arrow load.

3. **Apply labels (optional).** Use the generated label snippets in
   `Code/labels_*.do` to attach value labels in Stata. See
   `Code/01_append_donefiles.do` for an example of reading the aggregated `.dta`
   file, applying labels, and saving a cleaned copy.

## 4. Troubleshooting

- **Missing dependencies:** Run `pip install -r requirements.txt` and `pip install
  pyarrow` to ensure all Python packages are available.
- **Large downloads:** If the API throttles or returns incomplete pages, lower
  `--limit` in the Python downloader so pagination proceeds with smaller page
  sizes.
- **Interrupted runs:** Re-running `Code/00_municipal_batch.do` will skip files
  already present in `Data/CSV/`, letting you resume after a partial download.
