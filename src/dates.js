let moment = require("moment");

// dateType:
// recent_day: uses a single date from the last loaded month of data (first tuesday). e.g., (DATE = '2023-12-05')
// recent_month: uses the last month of loaded data e.g., (DATE BETWEEN '2023-12-01' AND '2023-12-31')
// first_days: list of all first days in supported months since October 2021 (i.e., 202x-yy-01 for all months present in the archive)
// first_tuesdays: list of all first Tuesdays in supported months since September 2022
// first_and_third_tuesdays: list of all first and third tuesdays in supported months since September 2022 

// cachedData contains a list of BigQuery data records that we have in cache
// from this, we extract all unique dates that exist, and then filter those dates from the returned query list, 
// since they all exist in cache already
// if there are no dates left to fetch, an empty string is returned
function getDateQuery( dateType, cachedData ) {
    // from october 2021 to august 2022, we only have the 1st of each month
    // from september 2022 and after, we have each day

    function toQueryString( allDates ) {
        if( allDates.length === 0 ) {
            return "";
        }

        let allDatesString = "(";

        for ( const [idx, date] of allDates.entries() ) {
            allDatesString += "DATE = \"" + date + "\"";
            if ( idx != allDates.length - 1 )
                allDatesString += " OR ";
        }
        allDatesString += ")";

        return allDatesString;
    }

    const allDatesArray = getDateArray( dateType );

    const cachedDates = getCachedDateSet( cachedData );

    let dateArray = [];
    for( let date of allDatesArray ) {
        if ( !cachedDates.has(date) )
            dateArray.push( date );
    } 

    return toQueryString( dateArray );
}

// NOTE: this function is intended to be updated every time the RUMArchive is refreshed with new data
// TODO: get this from external config files so we don't have to update the code every time? 
function getDateArray( dateType ) {

    if ( dateType === "recent_day" ) {
        const tuesdays = getDateArray("first_tuesdays");
        return [ tuesdays[ tuesdays.length - 1 ] ]; // most recent tuesday
    }
    else if ( dateType === "recent_month" ) {
        return "(DATE BETWEEN '2024-12-01' AND '2024-12-31')";
    }
    else if ( dateType === "first_days" ) {
        // date to start and today
        let date = moment("2021-10-01");
        let today = moment();

        // push first date
        const dates = [];
        dates.push(date.format("YYYY-MM-DD"));

        while (true) {
            date = date.add(1, "months");

            if (date.isAfter(today)) {
                break;
            }

            dates.push(date.format("YYYY-MM-DD"));
        }

        return dates;
    }
    else if ( dateType === "first_tuesdays" ) {
        // date to start and today
        let date = moment("2022-09-06");
        let today = moment();

        // push first date
        const dates = [];
        dates.push(date.format("YYYY-MM-DD"));

        while (true) {
            // go to the next month
            date = date.add(1, "months").startOf("month");

            // find the first Tuesday
            while (date.isoWeekday() !== 2) {
                date.add(1, "day");
            }

            if (date.isAfter(today)) {
                break;
            }

            dates.push(date.format("YYYY-MM-DD"));
        }

        return dates;
    }
    else if ( dateType === "first_and_third_tuesdays" ) {
        // date to start and today
        let date = moment("2022-09-06");
        let today = moment();

        // push first date
        const dates = [];
        dates.push(date.format("YYYY-MM-DD"));

        while (true) {
            // go to the next month
            date = date.add(1, "months").startOf("month");

            // find the first Tuesday
            while (date.isoWeekday() !== 2) {
                date.add(1, "day");
            }

            if (date.isAfter(today)) {
                break;
            }

            // first tuesday
            dates.push(date.format("YYYY-MM-DD"));

            // third tuesday
            dates.push(moment(date).add(2, "weeks").format("YYYY-MM-DD"));
        }

        return dates;
    }
    else {
        throw new Error("dates:getDateArray: invalid dateType : " + dateType);
    }
}

function getCachedDateSet( cachedData ) {
    // cachedData is an array of BigQuery record results
    // Records are of the form {"date":{"value":"2022-09-06"},"device":null,"rowcount":8327,"beaconcount":796420}
    // though only the date field is typically stable for all different queries. Luckily, that's all we need ;) 

    const uniqueDates = new Set();

    for( let record of cachedData ){
        if( record && record.date && record.date.value )
            uniqueDates.add( record.date.value );
        else 
            console.error("dates:getCachedDateSet: no date found for a record. SHOULD NOT HAPPEN! ", record.date, record);
    }

    return uniqueDates;
}

module.exports = {
    getDateQuery
}