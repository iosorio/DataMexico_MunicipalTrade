# DataMexico Municipal Trade Data

This repository provides a simple, reproducible workflow for downloading
municipal‐level foreign trade data from the DataMéxico API and
converting it into comma–separated value (CSV) files.  It contains
a documented Stata do‑file that orchestrates batch downloads across all
municipalities and months, a standalone Python script for fetching
individual requests, and supporting documentation for contributors.

## Overview

The DataMéxico API exposes detailed information about imports and
exports at various administrative levels.  Municipalities (defined by
their INEGI codes) and months (formatted as `YYYYMM`) are among the
primary filters.  This repository automates the process of generating
requests for every municipality/month combination, saving each result
to the `data/csv/` directory, and avoiding re‑downloads when files
already exist.

### Contents

- **`code/00_Run_Python.do`** – A Stata script that
  1. identifies which CSVs have already been downloaded,
  2. reads the list of municipality identifiers,
  3. generates all month identifiers from 2006 through 2025,
  4. expands to all combinations of municipality × month,
  5. constructs command lines to call the Python downloader,
  6. drops jobs whose CSV already exists, and
7. builds multiple batch files under `code/batch/`—one for each
   two‑digit state code—containing one command per
   municipality/month.  It also generates a shell script
   (`myscript.sh`) that launches each state batch via Stata‑MP
   concurrently.  See **docs/USAGE.md** for details.
- **`code/municipal_trade_download.py`** – A Python script that
  connects to the DataMéxico API (or reads a previously saved
  JSON file), loops over API pages if necessary, and writes the
  result to a CSV file.  It uses `requests` and `pandas` and
  supports both imports (`1`) and exports (`2`) via the `--flows`
  argument.
- **`docs/USAGE.md`** – A practical guide describing how to run the
  Stata do‑file and Python script together or independently.
- **`LICENSE`** – MIT licence granting broad reuse rights.
- **`.gitignore`** – Patterns to exclude generated data, logs and
  other artifacts from version control.
- **`requirements.txt`** – Runtime dependencies (`pandas` and
  `requests`).
- **`requirements-dev.txt`** – Development dependencies such as
  `black`, `ruff` and `pre‑commit` for code style and linting.
- **`.pre-commit-config.yaml`** – Pre‑commit hooks to format code and
  perform static analysis automatically.
- **`.github/`** – Issue and pull request templates, a code of
  conduct, a contributing guide and a CI workflow that lints the
  code on every push or pull request.

## Quickstart

1. **Clone the repository**

   ```bash
   git clone https://github.com/your‑org/datamexico‑municipal‑trade.git
   cd datamexico‑municipal‑trade
   ```

2. **Install dependencies**

   Create a virtual environment (optional) and install the runtime and
   developer dependencies:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt -r requirements-dev.txt
   pre-commit install  # install pre‑commit hooks
   ```

3. **Prepare directories**

   The Stata script expects a `data/csv/` directory to hold the
   downloaded files.  Create it if it does not exist:

   ```bash
   mkdir -p data/csv
   ```

4. **Run the batch download in Stata**

   Open Stata, set the working directory to the root of this
   repository and execute the do‑file:

   ```stata
   do code/00_Run_Python.do
   ```

   Running the do‑file now produces a series of state‑specific
   batch files in `code/batch/` and a shell script `myscript.sh`.
   The script launches each state batch via Stata‑MP in parallel.
   To run sequentially, you can comment out the final `!./myscript.sh`
   line in the do‑file and execute each batch manually.

5. **Run the Python script manually (optional)**

   You can invoke the Python downloader directly for a single
   municipality and month.  For example:

   ```bash
   python code/municipal_trade_download.py \
       --month 200601 \
       --municipality 33000 \
       --flows 1,2 \
       --limit 1000000000 \
       --output data/csv/mun33000_200601.csv
   ```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidance on how to
propose changes, report bugs, or request new features.  All
participants are expected to follow our [Code of
Conduct](CODE_OF_CONDUCT.md).

## License

This project is licensed under the MIT License.  See
[LICENSE](LICENSE) for details.