* ----------------------------------------------------------------------
* 01_Move_Downloads.do
*
* Description:
*
* Usage:
*
* ----------------------------------------------------------------------

clear

* --- Set project root and global path ---
*cd "path/to/datamexico-municipal-trade"
cd "/Users/Israel/Dropbox/Data/DataMexico_MunicipalTrade"
global path "`c(pwd)'"

* --- Import municipality list ---
import delimited "code/municipalities_datamexico.csv", clear
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

* --- Import municipality list ---
levelsof stateid, local(allstateids)
foreach st of local allstateids {
	cap mkdir "$path/data/csv/batch`st'"
}

tostring stateid, gen(stateids)

gen code = `"capture cp "data/csv/mun"' + municipalityid + `"_"' + dmonth + `".csv" "data/csv/batch"' + stateids + `"/mun"' + municipalityid + `"_"' + dmonth + `".csv"; capture erase "data/csv/mun"' + municipalityid + `"_"' + dmonth + `".csv";"'

sort municipalityidnum dmonth
expand 2 if _n==1
sort municipalityidnum dmonth

replace code = "#delimit ;" if _n==1
keep code
outsheet using "code/other/movefiles.do", replace noquote nonames

do "code/other/movefiles.do"


********* End of do-file ***********
