"""
municipal_trade_download.py
--------------------------------

This script provides a simple interface for downloading and converting
monthly foreign trade data at the municipal level from the DataMéxico
Tesseract API.  The API endpoint used is `/tesseract/data.jsonrecords`,
which accepts a variety of query parameters to filter the dataset.

Usage examples:

    # Download data for municipality 1001 (Aguascalientes) for January
    # 2006 (month code 200601) and save it to a CSV file.
    python municipal_trade_download.py --month 200601 --municipality 1001 --output ags_200601.csv

    # Specify only exports (flow=2) and limit to 100 rows per page
    python municipal_trade_download.py --month 200601 --municipality 1001 \
        --flows 2 --limit 100 --output ags_200601_exports.csv

Alternatively, if you have previously downloaded the JSON file via a
browser and saved it locally, you can convert it to CSV without
connecting to the API using the ``--json-file`` option:

    python municipal_trade_download.py --json-file path/to/data.json --output trade_data.csv

The script requires the ``requests`` and ``pandas`` libraries when
downloading data from the API.  Reading from a local JSON file does
not require ``requests`` but still needs ``pandas``.

python municipal_trade_download.py --month 200601 --municipality 1001 --flows 1,2 --limit 1000000000 --output "CSV/mun1001_200601.csv" 
"""

import argparse
import json
import os
from typing import List, Optional

import pandas as pd


def fetch_trade_data(
    month: str,
    municipality: int,
    flows: Optional[List[int]] = None,
    limit: int = 500,
) -> pd.DataFrame:
    """Fetch trade data from the DataMéxico Tesseract API.

    Parameters
    ----------
    month : str
        The period in ``YYYYMM`` format (e.g., ``"200601"`` for January 2006).
    municipality : int
        Municipality code as defined by DataMéxico (INEGI code).
    flows : list[int], optional
        A list of trade flows to query. 1 corresponds to imports and 2
        corresponds to exports.  If ``None``, both flows will be
        requested.
    limit : int
        Maximum number of records returned per page.  The API supports
        pagination; this function will loop over pages until no more
        records are returned.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the requested trade data.  The columns
        include "Municipality ID", "Municipality", "HS6 ID",
        "HS6" (description) and "Trade Value".
    """
    import requests  # imported here so that reading local JSON does not require it

    if flows is None:
        flows = [1, 2]

    base_url = "https://www.economia.gob.mx/apidatamexico/tesseract/data.jsonrecords"

    all_rows = []
    for flow in flows:
        page = 0
        while True:
            params = {
                "cube": "economy_foreign_trade_mun",
                "Flow": flow,
                "Month": month,
                "Municipality": municipality,
                "drilldowns": "Month,Municipality,HS6,Flow,Country",
                "limit": limit,
                "measures": "Trade Value",
                "page": page,
            }
            resp = requests.get(base_url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", [])
            if not rows:
                break
            all_rows.extend(rows)
            # stop if fewer than ``limit`` records were returned (no more pages)
            if len(rows) < limit:
                break
            page += 1

    df = pd.DataFrame(all_rows)
    return df


def load_json_file(json_path: str) -> pd.DataFrame:
    """Load a JSON file exported from the DataMéxico API into a DataFrame.

    The JSON file is expected to follow the structure returned by the
    ``data.jsonrecords`` endpoint, with a top-level ``data`` key
    containing a list of records and an optional ``source`` key with
    metadata.

    Parameters
    ----------
    json_path : str
        Path to the JSON file.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the data records.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    records = payload.get("data", [])
    df = pd.DataFrame(records)
    return df


def save_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """Save the DataFrame to a CSV file without the index.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to save.
    output_path : str
        Destination CSV file path.  If the directory does not exist
        it will be created.
    """
    directory = os.path.dirname(os.path.abspath(output_path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    df.to_csv(output_path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download and save municipal trade data from DataMéxico.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--json-file",
        type=str,
        help="Path to a locally saved JSON file returned by the DataMéxico API.",
    )
    group.add_argument(
        "--month",
        type=str,
        help="Month to query in YYYYMM format (e.g., 200601 for January 2006).",
    )

    parser.add_argument(
        "--municipality",
        type=int,
        help="Municipality code to query (INEGI code). Required when --month is used.",
    )
    parser.add_argument(
        "--flows",
        type=str,
        default="1,2",
        help="Comma-separated list of trade flows to request: 1=Imports, 2=Exports. Default '1,2'.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Number of records to request per page. Default 500.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="trade_data.csv",
        help="Path to the output CSV file. Default 'trade_data.csv'.",
    )

    args = parser.parse_args()

    if args.json_file:
        # Load from local JSON file
        df = load_json_file(args.json_file)
    else:
        if args.municipality is None:
            parser.error("--municipality is required when fetching from the API")
        flows = [int(f) for f in args.flows.split(",") if f.strip()]
        df = fetch_trade_data(
            month=args.month,
            municipality=args.municipality,
            flows=flows,
            limit=args.limit,
        )

    # Save to CSV
    save_to_csv(df, args.output)
    print(f"Saved {len(df)} records to {args.output}")


if __name__ == "__main__":
    main()