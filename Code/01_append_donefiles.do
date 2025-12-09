version 17.0

/*************************************************************************
Purpose
-------
Prepare separate "donefile" registries for municipal and state downloads
and show how to optionally aggregate the CSV archive into labeled Stata
datasets.

Prerequisites
-------------
* Update the Python executable if it differs from `!python3` using the
  `local py_exec` definition below.
* Install required Python packages before running the download scripts:
      !pip3 install pandas
      !pip3 install requests
* Example direct download command (for quick testing):
      !python3 "CODE/municipal_trade_download.py" --month 200601 --municipality 33000 --flows 1,2 --limit 1000000000 --output "Data/CSV/mun33000_200601.csv"

Outputs
-------
* Data/CSV/donefiles_mun.dta — registry of municipal CSVs that finished
  downloading.
* Data/CSV/donefiles_sta.dta — registry of state CSVs that finished
  downloading.
* Commented aggregation block shows how to build all_trade_mun.dta and
  all_trade_sta.dta plus the label files applied to each dataset.
*************************************************************************/


* ----------------------------------------------------------------------
* User settings
* Set the working directory, date window, and shared CLI options used by
* the download and aggregation scripts.
* ----------------------------------------------------------------------
*local disk "/Volumes/Samsung2TB"
local disk "/Users/Israel"
local start_year 2006
local end_year   2025
local py_exec    "!python3"
local flow_args  "--flows 1,2 --limit 1000000000"

* Keep all work inside the project folder so paths align with README.
cd "`disk'/Data/DataMexico"
global path "`c(pwd)'"

* Load the combined donefile log produced by download scripts.
use "Data/CSV/donefiles.dta", clear

* Identify whether each entry corresponds to a municipal or state file.
gen type = .
replace type = 1 if substr(donefile,1,3)=="mun"
replace type = 2 if substr(donefile,1,3)=="sta"


* Export separate registries so aggregation can run by geography level.
preserve
        keep if type==1
        drop type
        save "Data/CSV/donefiles_mun.dta", replace
restore

preserve
        keep if type==2
        drop type
        save "Data/CSV/donefiles_sta.dta", replace
restore

clear

/*
The block below is intentionally commented to keep aggregation optional.
Uncomment to rebuild the municipal-level Stata dataset and associated
label files from the CSV archive once downloads finish.

#delimit ;
!python3 Code/aggregate_csvs.py --donefile Data/CSV/donefiles_mun.dta
                                                                --csv-dir Data/CSV
                                                                --output Data/CSV/all_trade_mun.dta
                                                                --workers 24
                                                                --batch-size 12000
;
#delimit cr

cd "$path"

* Generate label definitions for each identifier column so the
* aggregated dataset opens with readable value labels.
foreach varinuse in Month Municipality HS6 Flow {
        use `varinuse' `varinuse'_ID using "Data/CSV/all_trade_mun.dta", clear
                contract `varinuse' `varinuse'_ID
                tostring `varinuse'_ID, replace

                if "`varinuse'"=="HS6"|"`varinuse'"=="Municipality" {
                        gen code = "label define lbl`varinuse'_ID " + `varinuse'_ID + " `" + `"""' + `varinuse'_ID + " " + `varinuse' + `"""' + `"', modify add"'
                }
                else {
                        gen code = "label define lbl`varinuse'_ID " + `varinuse'_ID + " `" + `"""' + `varinuse' + `"""' + `"', modify add"'
                }

                keep code
        outsheet using "Code/labels_`varinuse'_ID.do", nonames noquote replace
}

use Month_ID Municipality_ID HS6_ID Flow_ID Country_ID Trade_Value using "Data/CSV/all_trade_mun.dta", clear
        replace Country_ID = upper(Country_ID)
        compress
        do "Code/labels_Month_ID.do"
        do "Code/labels_Municipality_ID.do"
        do "Code/labels_Flow_ID.do"
        do "Code/labels_HS6_ID.do"

        foreach varinuse in Month Municipality HS6 Flow {
                label values `varinuse'_ID lbl`varinuse'_ID
        }

        label var Month_ID                      "Month"
        label var Municipality_ID       "Municipality"
        label var HS6_ID                        "HS6 code"
        label var Flow_ID                       "Flow"
        label var Country_ID            "Country Code, 3-letter"
        label var Trade_Value           "Trade Value, US$"

        label data "Trade by Municipality. Source: DataMexico"

save    "Data/DTA/all_trade_mun.dta", replace
erase   "Data/CSV/all_trade_mun.dta"
*/

#delimit ;
!python3 Code/aggregate_csvs.py --donefile Data/CSV/donefiles_sta.dta
                                                                --csv-dir Data/CSV
                                                                --output Data/CSV/all_trade_sta.dta
                                                                --workers 24
                                                                --batch-size 12000
;
#delimit cr


cd "$path"

* Generate label definitions for state-level identifiers and attach them
* to the aggregated dataset.
foreach varinuse in State {
        use `varinuse' `varinuse'_ID using "Data/CSV/all_trade_sta.dta", clear
                contract `varinuse' `varinuse'_ID
                tostring `varinuse'_ID, replace

                if "`varinuse'"=="HS6"|"`varinuse'"=="Municipality"|"`varinuse'"=="State" {
                        gen code = "label define lbl`varinuse'_ID " + `varinuse'_ID + " `" + `"""' + `varinuse'_ID + " " + `varinuse' + `"""' + `"', modify add"'
                }
                else {
                        gen code = "label define lbl`varinuse'_ID " + `varinuse'_ID + " `" + `"""' + `varinuse' + `"""' + `"', modify add"'
                }

                keep code
        outsheet using "Code/labels_`varinuse'_ID.do", nonames noquote replace
}

use Month_ID State_ID HS6_ID Flow_ID Country_ID Trade_Value using "Data/CSV/all_trade_sta.dta", clear
        replace Country_ID = upper(Country_ID)
        compress
        do "Code/labels_Month_ID.do"
        do "Code/labels_State_ID.do"
        do "Code/labels_Flow_ID.do"
        do "Code/labels_HS6_ID.do"

        foreach varinuse in Month State HS6 Flow {
                label values `varinuse'_ID lbl`varinuse'_ID
        }

        label var Month_ID                      "Month"
        label var State_ID                      "State"
        label var HS6_ID                        "HS6 code"
        label var Flow_ID                       "Flow"
        label var Country_ID            "Country Code, 3-letter"
        label var Trade_Value           "Trade Value, US$"

        label data "Trade by State. Source: DataMexico"

save    "Data/DTA/all_trade_sta.dta", replace
erase   "Data/CSV/all_trade_sta.dta"
*/
