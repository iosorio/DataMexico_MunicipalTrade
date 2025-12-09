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

use "Data/CSV/donefiles.dta", clear

gen type = .
replace type = 1 if substr(donefile,1,3)=="mun"
replace type = 2 if substr(donefile,1,3)=="sta"


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
#delimit ;
!python3 Code/aggregate_csvs.py --donefile Data/CSV/donefiles_mun.dta 
								--csv-dir Data/CSV	
								--output Data/CSV/all_trade_mun.dta 
								--workers 24 
								--batch-size 12000
;
#delimit cr

cd "$path"

* Generate Labels
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
	
	label var Month_ID			"Month"
	label var Municipality_ID	"Municipality" 
	label var HS6_ID 			"HS6 code"
	label var Flow_ID 			"Flow"
	label var Country_ID		"Country Code, 3-letter"
	label var Trade_Value		"Trade Value, US$"
	
	label data "Trade by Municipality. Source: DataMexico"
	
save 	"Data/DTA/all_trade_mun.dta", replace
erase 	"Data/CSV/all_trade_mun.dta"
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

* Generate Labels
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
	
	label var Month_ID			"Month"
	label var State_ID			"State" 
	label var HS6_ID 			"HS6 code"
	label var Flow_ID 			"Flow"
	label var Country_ID		"Country Code, 3-letter"
	label var Trade_Value		"Trade Value, US$"
	
	label data "Trade by State. Source: DataMexico"
	
save 	"Data/DTA/all_trade_sta.dta", replace
erase 	"Data/CSV/all_trade_sta.dta"
*/

	
	