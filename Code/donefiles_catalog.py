#!/usr/bin/env python3
"""Efficiently catalog processed CSV files.

This script replaces the Stata loop that scanned `DATA/CSV` for municipal
and state CSV exports. It reads the directory once using ``os.scandir`` and
filters filenames by year, saving the resulting list to a Stata ``.dta`` file.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Iterable, List

import pandas as pd


def collect_files(csv_dir: Path, start_year: int, end_year: int) -> List[str]:
    """Return matching CSV filenames between ``start_year`` and ``end_year``.

    Scans the directory only once using ``os.scandir`` to avoid repeated
    filesystem calls when handling hundreds of thousands of files.
    """

    if start_year > end_year:
        raise ValueError("start_year must be less than or equal to end_year")

    prefix = re.compile(r"^(mun|state)", re.IGNORECASE)
    year_token = re.compile(r"20\d{2}")
    matched: List[str] = []

    with os.scandir(csv_dir) as it:  # type: ignore[arg-type]
        for entry in it:
            if not entry.is_file():
                continue

            if not prefix.match(entry.name):
                continue

            for year_str in year_token.findall(entry.name):
                year = int(year_str)
                if start_year <= year <= end_year:
                    matched.append(entry.name)
                    break

    return matched


def save_donefiles(files: Iterable[str], output_path: Path) -> None:
    """Write the filenames to a Stata ``.dta`` file."""

    df = pd.DataFrame({"donefile": sorted(files)})
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_stata(output_path, write_index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=Path("DATA/CSV"),
        help="Directory containing the municipal/state CSV files",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2006,
        help="First year to include",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help="Last year to include",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("DATA/CSV/donefiles.dta"),
        help="Output path for the Stata dataset",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    files = collect_files(args.csv_dir, args.start_year, args.end_year)
    save_donefiles(files, args.output)
    print(f"Found {len(files)} matching CSV files.")
    print(f"Saved results to {args.output}.")


if __name__ == "__main__":
    main()
	