"""Aggregate many municipal/state trade CSV exports into a single Stata file.

The script:
- Reads the catalog of completed files (default: ``Data/CSV/donefiles.dta``)
- Uses multiprocessing to read CSV files in parallel and write temporary Parquet shards
- Loads the Parquet shards with Apache Arrow and writes a single Stata ``.dta`` file

Example
-------
python scripts/aggregate_csvs.py \
    --donefile Data/CSV/donefiles.dta \
    --csv-dir Data/CSV \
    --output Data/CSV/all_trade.dta \
    --workers 12 \
    --batch-size 2000

Notes
-----
- Requires ``pandas`` and ``pyarrow`` (``pip install pandas pyarrow``).
- Assumes every CSV has the same column set; columns missing in a given file
  are filled with ``NA`` to keep the schema consistent.
- Keeps intermediate Parquet shards under the same parent directory as the
  output file (``<output>.parts``) so the final Arrow load happens from a
  fast local disk.
"""

from __future__ import annotations

import argparse
import math
import multiprocessing as mp
import os
import pathlib
import shutil
import tempfile
from typing import Iterable, List, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def _load_donefiles(donefile_path: pathlib.Path) -> List[str]:
    if not donefile_path.exists():
        raise FileNotFoundError(f"donefile catalog not found: {donefile_path}")

    if donefile_path.suffix.lower() == ".dta":
        catalog = pd.read_stata(donefile_path)
    else:
        catalog = pd.read_csv(donefile_path)

    if "donefile" in catalog.columns:
        return catalog["donefile"].astype(str).tolist()

    # Fall back to the first column if the expected name is not present
    return catalog.iloc[:, 0].astype(str).tolist()


def _discover_column_order(csv_path: pathlib.Path) -> List[str]:
    sample = pd.read_csv(csv_path, nrows=0)
    return sample.columns.tolist()


def _chunks(seq: Sequence[str], chunk_size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), chunk_size):
        yield list(seq[i : i + chunk_size])


def _process_files(
    worker_id: int,
    files: Sequence[str],
    csv_dir: pathlib.Path,
    column_order: Sequence[str],
    batch_size: int,
    output_dir: pathlib.Path,
) -> List[pathlib.Path]:
    output_paths: List[pathlib.Path] = []
    for part_idx, batch in enumerate(_chunks(list(files), batch_size)):
        frames = []
        for filename in batch:
            csv_path = csv_dir / filename
            if not csv_path.exists():
                continue
            df = pd.read_csv(csv_path)
            # Enforce a stable column order and fill missing columns
            df = df.reindex(columns=column_order)
            frames.append(df)

        if not frames:
            continue

        combined = pd.concat(frames, ignore_index=True)
        table = pa.Table.from_pandas(combined, preserve_index=False)
        part_path = output_dir / f"part_{worker_id:02d}_{part_idx:04d}.parquet"
        pq.write_table(table, part_path, compression="zstd")
        output_paths.append(part_path)

    return output_paths


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--donefile",
        default="Data/CSV/donefiles.dta",
        type=pathlib.Path,
        help="Catalog of completed CSVs (dta or csv).",
    )
    parser.add_argument(
        "--csv-dir",
        default="Data/CSV",
        type=pathlib.Path,
        help="Directory containing the CSV exports.",
    )
    parser.add_argument(
        "--output",
        default="Data/CSV/all_trade.dta",
        type=pathlib.Path,
        help="Path to the final Stata .dta file.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(mp.cpu_count() - 1, 1),
        help="Number of parallel workers.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of CSV files to concatenate per Parquet shard.",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    csv_dir: pathlib.Path = args.csv_dir
    csv_dir.mkdir(parents=True, exist_ok=True)

    filenames = _load_donefiles(args.donefile)
    if not filenames:
        raise RuntimeError("No entries found in the donefile catalog.")

    # Pick the first available CSV to define the canonical column order
    sample_path = next((csv_dir / name for name in filenames if (csv_dir / name).exists()), None)
    if sample_path is None:
        raise FileNotFoundError("None of the listed CSV files were found in the CSV directory.")
    column_order = _discover_column_order(sample_path)

    output_dir = args.output.with_suffix(args.output.suffix + ".parts")
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    worker_inputs = []
    worker_count = max(1, args.workers)
    chunk_size = math.ceil(len(filenames) / worker_count)
    for worker_id in range(worker_count):
        worker_files = filenames[worker_id * chunk_size : (worker_id + 1) * chunk_size]
        if not worker_files:
            continue
        worker_inputs.append(
            (
                worker_id,
                worker_files,
                csv_dir,
                column_order,
                args.batch_size,
                output_dir,
            )
        )

    with mp.Pool(processes=worker_count) as pool:
        shard_lists = pool.starmap(_process_files, worker_inputs)

    shards = [path for sublist in shard_lists for path in sublist]
    if not shards:
        raise RuntimeError("No Parquet shards were produced; check CSV directory and catalog contents.")

    dataset = ds.dataset([str(p) for p in shards], format="parquet")
    table = dataset.to_table()  # Arrow uses multithreading internally
    df = table.to_pandas(split_blocks=True, self_destruct=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_stata(args.output, write_index=False)

    shutil.rmtree(output_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
	