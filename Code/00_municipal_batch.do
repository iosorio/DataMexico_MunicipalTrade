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
local disk "/Volumes/Samsung2TB"
local start_year 2006
local end_year   2025
local py_exec    "!python3"
local flow_args  "--flows 1,2 --limit 1000000000"

cd "`disk'/Data/DataMexico"
global path "`c(pwd)'"

* Ensure expected folders exist for generated files
cap mkdir Code
cap mkdir Code/batch
cap mkdir DATA
cap mkdir DATA/CSV

* ----------------------------------------------------------------------
* Housekeeping: remove stale batch and log files
* ----------------------------------------------------------------------
quietly {
    local erasebatch : dir "Code/batch" files "batch*.do", respectcase
    foreach file of local erasebatch {
        erase "Code/batch/`file'"
    }

    local eraselog : dir "" files "batch*.log", respectcase
    foreach file of local eraselog {
        erase "`file'"
    }
}

* ----------------------------------------------------------------------
* Register already processed files to avoid duplicate downloads
* ----------------------------------------------------------------------
`py_exec' "CODE/donefiles_catalog.py" --csv-dir DATA/CSV --start-year `start_year' --end-year `end_year' --output DATA/CSV/donefiles.dta

* ----------------------------------------------------------------------
* Import municipality identifiers (DataMexico and ENOE coverage)
* ----------------------------------------------------------------------
import delimited "Code/municipalities_datamexico.csv", clear
keep stateid municipalityid
sort stateid municipalityid
tempfile municipalities
save `municipalities', replace

import delimited "Code/municipios_mexico_enoe.csv", stringcols(1 3) clear

gen municipalityid = cve_ent + cve_mun
 destring municipalityid, replace
 destring cve_ent, gen(stateid)
keep stateid municipalityid

append using `municipalities'
duplicates drop
sort stateid municipalityid
save `municipalities', replace

* ----------------------------------------------------------------------
* Build a calendar of months to process (YYYYMM format)
* ----------------------------------------------------------------------
clear
local total_months = 12 * (`end_year' - `start_year' + 1)
set obs `total_months'

gen tm_date = ym(`start_year', 1) + _n - 1
format tm_date %tm

* Construct YYYYMM string once instead of nested loops
quietly {
    gen year  = yofd(dofm(tm_date))
    gen month = month(dofm(tm_date))
}
gen dmonth = string(year,  "%04.0f") + string(month, "%02.0f")
keep dmonth

tempfile months
save `months', replace

* ----------------------------------------------------------------------
* Pair every municipality with every month
* ----------------------------------------------------------------------
use `municipalities', clear
* `cross' creates a Cartesian product when no common variables exist,
* which is what we want here (all municipalities for all months).
cross using `months'

* Normalized identifiers for string concatenation
format municipalityid %05.0f

gen stateidstr        = string(stateid, "%02.0f")
gen municipalityidstr = string(municipalityid, "%05.0f")

gen municipalityidnum = municipalityid

* ----------------------------------------------------------------------
* Compose command lines for municipal downloads
* ----------------------------------------------------------------------
local base_cmd = `"`py_exec' CODE/municipal_trade_download_2.py --drilldowns "Month,Municipality,HS6,Flow,Country""'


gen code = `"`base_cmd' --month "' + dmonth + `" --municipality "' + municipalityidstr + `" `flow_args' --output "Data/CSV/mun"' + municipalityidstr + `"_"' + dmonth + `".csv""'

* ----------------------------------------------------------------------
* Add state-level aggregation rows (first municipality per state/month)
* ----------------------------------------------------------------------
bysort dmonth stateid (municipalityid): gen state_row = (_n == 1)
expand 2 if state_row
bysort dmonth stateid municipalityid: gen statecode = (_N == 2 & state_row & _n == 2)

replace municipalityidstr = "" if statecode
replace municipalityidnum = .  if statecode

local state_cmd = `"`py_exec' CODE/municipal_trade_download_2.py --drilldowns "Month,State,HS6,Flow,Country""'
replace code = `"`state_cmd' --month "' + dmonth + `" --state "' + stateidstr + `" `flow_args' --output "Data/CSV/state"' + stateidstr + `"_"' + dmonth + `".csv""' if statecode

* ----------------------------------------------------------------------
* Drop already completed downloads
* ----------------------------------------------------------------------
gen     donefile = "mun"   + municipalityidstr + "_" + dmonth + ".csv"
replace donefile = "state" + stateidstr        + "_" + dmonth + ".csv" if statecode

merge 1:1 donefile using "DATA/CSV/donefiles.dta", nogenerate keep(1 2)

* ----------------------------------------------------------------------
* Write per-state batch files
* ----------------------------------------------------------------------
levelsof stateidstr, local(allsts)
foreach s of local allsts {
    preserve
    keep if stateidstr == "`s'"
    sort dmonth municipalityidnum
    keep code
    outsheet using "Code/batch/batch`s'.do", replace noquote nonames
    restore
}

* ----------------------------------------------------------------------
* Write helper shell script to run all batch files in parallel
* ----------------------------------------------------------------------
cap erase myscript.sh
cap file close myscript
file open myscript using myscript.sh, write

local maxc : word count `allsts'
local c = 1
foreach s of local allsts {
	if `c'< `maxc' file write myscript `"/usr/local/bin/stata-mp -b do "$path/Code/batch/batch`s'.do" & "' _n
	if `c'==`maxc' file write myscript `"/usr/local/bin/stata-mp -b do "$path/Code/batch/batch`s'.do"   "' _n
	local c = `c'+1
}


file close myscript
!chmod u+rx myscript.sh

* Uncomment the next line to run the full batch immediately
!./myscript.sh
