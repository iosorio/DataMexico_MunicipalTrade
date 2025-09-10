# Usage Guide

This guide explains how to make the most of the files in this
repository.  It covers two primary workflows: running the full batch
download via Stata and calling the Python script directly for
fine‑grained control.  Examples assume a Unix‑like shell and that
the current working directory is the root of the cloned repository.

## 1. Batch download with Stata

Running the Stata do‑file automates the retrieval of trade data for
every municipality and month in the predefined range (January 2006
through December 2025).  Follow these steps:

1. Ensure you have Stata installed and available on your system.  Set
   your working directory in Stata to the repository root:

   ```stata
   cd "path/to/datamexico-municipal-trade"
   ```

2. Verify that the Python script can be executed.  You may need to
   adjust the `python` command in the do‑file if your default
   Python interpreter differs (e.g., `python3` vs. `python`).

3. Execute the do‑file:

   ```stata
   do code/00_Run_Python.do
   ```

   This generates one batch file per two‑digit state code under
   `code/batch/` and a shell script `myscript.sh` that launches
   each batch via Stata‑MP.  By default, `myscript.sh` runs all
   but the last batch in the background, with the final batch
   running in the foreground to synchronise.  You can open
   `myscript.sh` to inspect or modify the commands.  To run
   sequentially, comment out the final `!./myscript.sh` line in
   the do‑file and run each `batchXX.do` manually.

The script performs the following operations:

- Reads the current contents of `data/csv/` to determine which
  municipality/month files have already been downloaded.
- Imports the list of municipalities from `code/municipalities_datamexico.csv`.
- Generates all month identifiers between 2006 and 2025.
- Expands to all combinations of municipality × month and constructs
  a Python command for each pair.
- Writes these commands to a temporary batch file and executes it.
- Skips any combinations for which the corresponding CSV already
  exists in `data/csv/`.

The batch download may take several hours depending on network
conditions and the number of municipalities.

## 2. Individual download with Python

If you want to download data for a single municipality or perform
ad‑hoc queries, you can call the Python script directly.  The
interface uses command‑line arguments:

```bash
python code/municipal_trade_download.py \
    --month <YYYYMM> \
    --municipality <INEGI_CODE> \
    --flows <flows> \
    --limit <n> \
    --output <path/to/output.csv>
```

Parameters:

- `--month` (required when not reading a local JSON file) – six digit
  string representing the year and month (e.g., `200601` for
  January 2006).
- `--municipality` – integer INEGI code identifying the municipality.
- `--flows` – comma‑separated list of trade flows to include: `1`
  for imports, `2` for exports.  Defaults to `1,2` (both).
- `--limit` – number of records to request per page.  The API
  supports pagination; large values (e.g., one billion) effectively
  disable paging.
- `--output` – path to the CSV file where results are saved.  Parent
  directories will be created if necessary.
- `--json-file` – alternatively, pass the path to a JSON file
  previously downloaded from the API; the script will read it and
  produce a CSV without making network calls.

### Example

Download exports only for the municipality with code `1001` in
December 2010:

```bash
python code/municipal_trade_download.py \
    --month 201012 \
    --municipality 1001 \
    --flows 2 \
    --output data/csv/mun1001_201012_exports.csv
```

## 3. Troubleshooting

- **Missing dependency:** If Python complains about missing modules,
  run `pip install -r requirements.txt` to ensure `pandas` and
  `requests` are installed.  The Stata script does not install
  packages automatically.
- **Large downloads:** The API may throttle or return incomplete
  responses when the limit is set very high.  If this happens, try
  lowering the `--limit` to something like `50000` and let the
  script page through the results.
- **Partial data:** If you interrupt the batch process, previously
  downloaded CSV files remain in `data/csv/`.  Re‑running the do‑file
  skips them, so you can resume without starting over.
- **Authentication:** The DataMéxico API is publicly accessible; no
  API key is required as of this writing.  If authentication is
  introduced in the future, you may need to update the Python script
  accordingly.

## 4. API Reference

For detailed information about the available filters, measures and
dimensions in the DataMéxico API, consult the official
documentation at <https://www.economia.gob.mx/datamexico>.  The
municipal foreign trade cube used by this script is `economy_foreign_trade_mun`.