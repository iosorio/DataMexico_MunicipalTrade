# DataMexico Municipal Trade Data

This repository contains a reproducible workflow for downloading, labeling,
and aggregating municipal- and state-level foreign trade data from the
[DataMéxico](https://www.economia.gob.mx/datamexico) API. It combines Stata
do-files that orchestrate large batch jobs with Python utilities that perform
the API calls, catalog completed downloads, and stitch thousands of CSV exports
into analysis-ready datasets.

## Repository structure

- **`Code/00_municipal_batch.do`** – Builds per-state batch files that call the
  Python downloader for every municipality×month combination, skips files that
  already exist, and writes a helper `myscript.sh` to launch the batches in
  parallel using Stata-MP.
- **`Code/municipal_trade_download_2.py`** – Python CLI that queries the
  DataMéxico Tesseract API by municipality or aggregates by state, handles
  pagination, and saves results to CSV. It can also convert a previously saved
  JSON response directly to CSV.
- **`Code/donefiles_catalog.py`** – Scans `Data/CSV/` once to catalog existing
  municipal/state CSV exports between configurable years and writes the list to
  a Stata `.dta` file so Stata can skip already downloaded combinations.
- **`Code/aggregate_csvs.py`** – Parallel CSV aggregator that reads the catalog
  of completed files, concatenates them in Parquet shards, and emits a single
  Stata dataset (UTF-8 by default) for downstream analysis.
- **`Code/01_append_donefiles.do`** – Utility do-file that splits the donefile
  catalog into municipal/state subsets and (optionally) drives aggregation and
  labeling of the unified datasets.
- **`docs/USAGE.md`** – Step-by-step guide for running the batch pipeline,
  executing individual downloads, and aggregating outputs.
- **`Data/`** – Workspace where CSV exports are written (`Data/CSV/`) and where
  aggregated `.dta` files can be stored (`Data/DTA/`). These folders are created
  automatically by the Stata scripts if they do not exist.

## Requirements

- Stata (tested with Stata-MP for parallel batch execution).
- Python 3.8+ with `pandas`, `requests`, and `pyarrow` (for aggregation). Install
  them with:

  ```bash
  pip install -r requirements.txt
  pip install pyarrow
  ```

## Batch download workflow (Stata)

1. Open Stata and set the working directory to the repository root (adjust the
   `local disk` definition in the do-file if needed).
2. Run the batch builder:

   ```stata
   do Code/00_municipal_batch.do
   ```

   - The script catalogs existing CSVs in `Data/CSV/` using
     `Code/donefiles_catalog.py` to avoid re-downloading completed files.
   - It creates per-state batch files under `Code/batch/` and a shell script
     `myscript.sh` that calls each batch with Stata-MP. Enable the final
     `!./myscript.sh` line to start downloads immediately, or run batches
     manually.

## Single download workflow (Python only)

Run the downloader directly for ad-hoc requests:

```bash
python Code/municipal_trade_download_2.py \
    --month 200601 \
    --municipality 33000 \
    --flows 1,2 \
    --drilldowns Month,Municipality,HS6,Flow,Country \
    --output Data/CSV/mun33000_200601.csv
```

Key options:

- `--municipality` or `--state` (mutually exclusive) control the geography.
- `--drilldowns` sets cube dimensions; defaults are chosen based on geography.
- `--json-file` converts a stored API response to CSV without making network
  calls.

## Aggregating CSV outputs

After downloads complete, create unified datasets for analysis:

1. Generate the donefile catalog (if not already present):

   ```bash
   python Code/donefiles_catalog.py --csv-dir Data/CSV --start-year 2006 --end-year 2025 --output Data/CSV/donefiles.dta
   ```

2. Aggregate municipal or state CSVs in parallel and write a Stata file:

   ```bash
   python Code/aggregate_csvs.py \
       --donefile Data/CSV/donefiles_mun.dta \
       --csv-dir Data/CSV \
       --output Data/DTA/all_trade_mun.dta \
       --workers 12 \
       --batch-size 2000
   ```

   The do-file `Code/01_append_donefiles.do` shows how to split the donefile by
   geography, apply variable labels from the generated `Code/labels_*.do`
   snippets, and save cleaned `.dta` outputs.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidance on proposing
changes. All participants are expected to follow our
[Code of Conduct](CODE_OF_CONDUCT.md).

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.
