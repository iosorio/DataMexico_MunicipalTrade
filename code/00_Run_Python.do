* ----------------------------------------------------------------------
* 00_Run_Python.do
*
* Description:
*   Build state‑specific batch files to download municipal trade data
*   from DataMéxico using a Python helper.  This script reads the
*   list of municipalities, constructs all month identifiers from
*   2006 through 2025, filters out combinations for which a CSV
*   already exists, and writes one batch file per two‑digit state
*   code under `code/batch/`.  A shell script (myscript.sh) is
*   generated to invoke each state batch with Stata‑MP in
*   parallel.
*
* Parallel processing:
*   Each state batch can be executed concurrently.  The generated
*   myscript.sh runs all but the last job in the background, and
*   the final job in the foreground.  Update the path to your
*   Stata‑MP binary if it differs from `/usr/local/bin/stata-mp`.
*
* Usage:
*   1. Install Python dependencies: `pip install pandas requests`.
*   2. Adjust the `cd` command and `global path` below to point to
*      the root of this repository.
*   3. Run this do‑file in Stata: `do code/00_Run_Python.do`.
*   4. Inspect `code/batch/` for per‑state batch files and
*      `myscript.sh` for the commands to run them.
* ----------------------------------------------------------------------

clear

* --- Set project root and global path ---
*cd "path/to/datamexico-municipal-trade"
cd "/Users/Israel/Dropbox/Data/DataMexico_MunicipalTrade"
global path "`c(pwd)'"

* --- Find existing CSV files ---
local donefiles : dir "data/csv" files "*.csv", respectcase
local ndf = 0
foreach file of local donefiles {
    local ndf = `ndf' + 1
}
set obs `ndf'
gen donefile = ""
local counter = 1
foreach file of local donefiles {
    replace donefile = "`file'" if _n == `counter'
    local counter = `counter' + 1
}
tempfile donefiles
save `donefiles', replace

* --- Import municipality list ---
import delimited "code/municipalities_datamexico.csv", clear
keep municipalityid
count
local nmun = r(N)
egen idtemp = group(municipalityid)
tempfile municipalityid
save `municipalityid', replace

* --- Generate month identifiers (YYYYMM) for 2006–2025 ---
clear
set obs 500
gen dmonth = ""
local counter = 1
forval yy = 2006/2025 {
    forval mm = 1/12 {
        local ys = "`yy'"
        if `mm' <= 9 local ms = "0" + "`mm'"
        if `mm' >= 10 local ms = "`mm'"
        local dmonth "`ys'`ms'"
        replace dmonth = "`dmonth'" if _n == `counter'
        local counter = `counter' + 1
    }
}
drop if dmonth == ""
expand `nmun'
bys dmonth: gen idtemp = _n
tempfile months
save `months', replace

* --- Merge months and municipalities ---
merge m:1 idtemp using `municipalityid'
drop idtemp _merge
sort municipalityid dmonth
clonevar municipalityidnum = municipalityid
tostring municipalityid, replace

* --- Construct the Python command for each combination ---
gen code = "!python3 code/municipal_trade_download.py" + ///
    " --month " + dmonth + ///
    " --municipality " + municipalityid + ///
    " --flows 1,2" + ///
    " --limit 1000000000" + ///
    " --output data/csv/mun" + municipalityid + "_" + dmonth + ".csv"

* --- Drop jobs where output already exists ---
gen donefile = "mun" + municipalityid + "_" + dmonth + ".csv"
merge 1:1 donefile using `donefiles'
drop if _merge == 3
drop _merge

* --- Build state codes and prepare per‑state batches ---
gen lm = length(municipalityid)
gen mu = ""
replace mu = "0" + municipalityid if lm == 4
replace mu = municipalityid if lm == 5
gen st = substr(mu, 1, 2)
levelsof st, local(allsts)

* Create the batch directory if it does not exist
capture mkdir "code/batch"

foreach s of local allsts {
    preserve
    keep if st == "`s'"
    sort municipalityidnum dmonth
    keep code
    outsheet using "code/batch/batch`s'.do", replace noquote nonames
    restore
}

* --- Create a shell script to run each batch via Stata‑MP ---
capture erase myscript.sh
capture file close myscript
file open myscript using myscript.sh, write

foreach s of local allsts {
    * Run all but the last state in the background
    if `s' <= 33 file write myscript `"/usr/local/bin/stata-mp -b do "$path/code/batch/batch`s'.do" & "' _n
    if `s' == 33 file write myscript `"/usr/local/bin/stata-mp -b do "$path/code/batch/batch`s'.do"   "' _n
}

file close myscript
!chmod u+rx myscript.sh

* Execute the script to launch parallel Stata jobs (comment out if you want to run manually)
!./myscript.sh

********* End of do-file ***********
