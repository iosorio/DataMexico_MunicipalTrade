"""
municipal_trade_download.py
--------------------------------

This module provides a command‑line interface for downloading monthly
municipal or state‑level trade data from DataMéxico’s Tesseract API
and exporting it as a CSV.  It is designed to be flexible: users can
filter by municipality or state, choose which dimensions to include
via the ``--drilldowns`` option, specify flows (imports or exports),
set a per–page limit, or convert an already downloaded JSON file into
CSV.  When aggregating by state, the script does not rely on the API
recognising a ``State`` parameter; instead it aggregates on the
``State`` dimension and, if a specific state code is provided, filters
the resulting table locally.

Example usages:

    # Convert a previously downloaded JSON file into CSV
    python municipal_trade_download.py --json-file data.json --output trade_data.csv

    # Download municipal‑level data for municipality 1001 in January 2006
    python municipal_trade_download.py --month 200601 --municipality 1001 --output ags_mun.csv

    # Download state‑level data for state 1 (Aguascalientes) in January 2006
    python municipal_trade_download.py --month 200601 --state 1 --drilldowns State,HS6 --output ags_state.csv

    # Aggregate at the national level by HS6 only (no geographic drilldown)
    python municipal_trade_download.py --month 200601 --drilldowns HS6 --output national.csv

The API endpoint used is ``/tesseract/data.jsonrecords`` on the
``economy_foreign_trade_mun`` cube.  Queries are paginated, and this
script will continue requesting pages until no more data are returned.
When specifying custom drilldowns, ensure that the dimension names
match those supported by the cube (e.g., ``Municipality``, ``State``,
``HS2``, ``HS6``).  Unknown dimension names will result in API errors.
"""

import argparse
import json
import os
from typing import List, Optional

import pandas as pd


def fetch_trade_data(
    month: str,
    municipality: Optional[int] = None,
    state: Optional[int] = None,
    flows: Optional[List[int]] = None,
    limit: int = 500,
    drilldowns: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch trade data from the DataMéxico Tesseract API.

    This function supports filtering by municipality or aggregating by
    state and allows the caller to customise which dimensions to
    include as drilldowns.  When a state is specified, the API call
    will not attempt to filter directly by state; instead all records
    will be retrieved for the given month and flows with a state
    drilldown, and the DataFrame will be filtered locally to the
    desired state.

    Parameters
    ----------
    month : str
        Period to query, formatted as ``YYYYMM`` (e.g., ``"200601"`` for
        January 2006).
    municipality : int, optional
        INEGI municipality code.  Only one of ``municipality`` or
        ``state`` may be provided; if both are given a ``ValueError`` is
        raised.
    state : int, optional
        State code (two digits).  If provided, the query will
        aggregate by state; the API call does not attempt to filter
        directly by state, but the returned DataFrame will be filtered
        locally by this code.
    flows : list[int], optional
        Trade flows to request.  ``1`` represents imports and ``2``
        represents exports.  Defaults to both flows (``[1, 2]``).
    limit : int
        Number of records per page.  The API paginates results; this
        function will iterate over pages until all data are fetched.
    drilldowns : str, optional
        Comma‑separated list of dimension names to use as drilldowns.
        If omitted, sensible defaults are applied based on whether
        ``municipality`` or ``state`` is specified.  For example,
        ``"Municipality,HS6"`` is used for municipal queries and
        ``"State,HS6"`` for state queries.

    Returns
    -------
    pandas.DataFrame
        A tidy DataFrame containing the requested records.  Columns
        correspond to the selected drilldowns plus a ``Trade Value``
        column.
    """
    import requests

    if municipality is not None and state is not None:
        raise ValueError(
            "Specify only one of municipality or state; these options are mutually exclusive."
        )

    # Default flows: both imports and exports
    if flows is None:
        flows = [1, 2]

    # Determine drilldowns if not explicitly provided
    if drilldowns is None or not drilldowns.strip():
        if municipality is not None:
            drilldowns = "Municipality,HS6"
        elif state is not None:
            drilldowns = "State,HS6"
        else:
            # Default drilldowns when no geographic filter is provided
            drilldowns = "Municipality,HS6"
    else:
        # Normalise: remove extra spaces and ensure comma separation
        drilldowns = ",".join(
            [d.strip() for d in drilldowns.split(",") if d.strip()]
        )

    base_url = "https://www.economia.gob.mx/apidatamexico/tesseract/data.jsonrecords"

    all_rows: List[dict] = []

    # Construct the static portion of the params (same for each flow)
    common_params = {
        "cube": "economy_foreign_trade_mun",
        "Month": month,
        "drilldowns": drilldowns,
        "limit": limit,
        "measures": "Trade Value",
    }
    # If a municipality is specified, include it as a filter.  The API
    # does not appear to recognise a "State" parameter for the municipal
    # cube, so we do not include such a filter; state filtering is
    # handled after downloading the data.
    if municipality is not None:
        common_params["Municipality"] = municipality

    for flow in flows:
        page = 0
        while True:
            params = common_params.copy()
            params.update({"Flow": flow, "page": page})
            resp = requests.get(base_url, params=params)
            resp.raise_for_status()
            payload = resp.json()
            rows = payload.get("data", [])
            if not rows:
                break
            all_rows.extend(rows)
            # If fewer than limit rows returned, we've reached the final page
            if len(rows) < limit:
                break
            page += 1

    df = pd.DataFrame(all_rows)

    # Filter locally by state if requested
    if state is not None and not df.empty:
        # Try common variations of the state ID column name
        # Some API responses may include "State ID" while others may
        # provide "state_id"; we normalise by lowercasing and
        # replacing spaces with underscores.
        # Collect possible state ID column candidates
        candidates = [
            col
            for col in df.columns
            if col.lower().replace(" ", "_") in {"state_id", "ent_id"}
        ]
        if candidates:
            state_col = candidates[0]
            df = df[df[state_col] == state]
        else:
            # If no numeric state identifier is present but a State
            # name is provided, we cannot filter by numeric code.
            pass

    return df


def load_json_file(json_path: str) -> pd.DataFrame:
    """Load a locally stored JSON file (from the DataMéxico API) into a DataFrame.

    The JSON should conform to the structure returned by the Tesseract
    ``data.jsonrecords`` endpoint: a top‑level ``data`` key
    containing a list of records and an optional ``source`` key.

    Parameters
    ----------
    json_path : str
        Path to the JSON file to load.

    Returns
    -------
    pandas.DataFrame
        A DataFrame constructed from the ``data`` section of the JSON.
    """
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    records = payload.get("data", [])
    return pd.DataFrame(records)


def save_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """Save a DataFrame to CSV without including the index.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame to save.
    output_path : str
        File path for the output CSV.  If the parent directory does
        not exist it will be created.
    """
    directory = os.path.dirname(os.path.abspath(output_path))
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
    df.to_csv(output_path, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Download monthly trade data from DataMéxico and save to CSV, "
            "with options for municipality, state, and custom drilldowns."
        )
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--json-file",
        type=str,
        help=(
            "Path to a local JSON file previously downloaded from DataMéxico. "
            "If provided, no API call will be made."
        ),
    )
    group.add_argument(
        "--month",
        type=str,
        help=(
            "Month to query, formatted as YYYYMM (e.g., 200601 for January 2006)."
        ),
    )

    parser.add_argument(
        "--municipality",
        type=int,
        help=(
            "INEGI municipality code for municipal‑level data.  Do not use "
            "with --state."
        ),
    )
    parser.add_argument(
        "--state",
        type=int,
        help=(
            "State code for state‑level aggregation.  Do not use with --municipality."
        ),
    )
    parser.add_argument(
        "--flows",
        type=str,
        default="1,2",
        help=(
            "Comma‑separated list of flows to query: 1=imports, 2=exports. "
            "Defaults to '1,2'."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help=(
            "Number of records to request per page.  Higher values reduce "
            "pagination overhead but may be limited by the API. Default: 500."
        ),
    )
    parser.add_argument(
        "--drilldowns",
        type=str,
        help=(
            "Comma‑separated list of drilldown dimensions (e.g., 'State,HS6'). "
            "If omitted, sensible defaults based on --municipality or --state "
            "are used."
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default="trade_data.csv",
        help=("Path to the output CSV file.  Default: 'trade_data.csv'."),
    )

    args = parser.parse_args()

    # Determine flows list
    flows: List[int] = [int(f.strip()) for f in args.flows.split(",") if f.strip()]

    if args.json_file:
        # Convert an existing JSON file to CSV
        df = load_json_file(args.json_file)
    else:
        df = fetch_trade_data(
            month=args.month,
            municipality=args.municipality,
            state=args.state,
            flows=flows,
            limit=args.limit,
            drilldowns=args.drilldowns,
        )

    save_to_csv(df, args.output)
    print(f"Saved {len(df)} records to {args.output}")


if __name__ == "__main__":
    main()