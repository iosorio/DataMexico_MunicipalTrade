version 17.0

/*************************************************************************
This script builds batch files to download municipal and state-level trade
statistics from DataMexico. It documents the steps end-to-end so the
workflow is reproducible and easier to maintain.

Prerequisites
-------------
* Update the Python executable if it differs from `!python3` using the
  `local py_exec` definition below.
* Install required Python packages before running the download scripts:
      !pip3 install pandas
      !pip3 install requests
* Example direct download command (for quick testing):
      !python3 "CODE/municipal_trade_download.py" --month 200601 --municipality 33000 --flows 1,2 --limit 1000000000 --output "Data/CSV/mun33000_200601.csv"
*************************************************************************/


* ----------------------------------------------------------------------
* User settings
* ----------------------------------------------------------------------
*local disk "/Volumes/Samsung2TB"
local disk "/Users/Israel"
local start_year 2006
local end_year   2025
local py_exec    "!python3"
local flow_args  "--flows 1,2 --limit 1000000000"

cd "`disk'/Data/DataMexico"
global path "`c(pwd)'"

!python3 Code/aggregate_csvs.py --donefile Data/CSV/donefiles.dta --csv-dir Data/CSV	--output Data/CSV/all_trade.dta --workers 24 --batch-size 12000



	
	