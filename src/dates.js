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
        return "(DATE BETWEEN '2024-04-01' AND '2024-04-30')";
    }
    else if ( dateType === "first_days" ) {
        const dates = [];
        dates.push("2021-10-01");
        dates.push("2021-11-01");
        dates.push("2021-12-01");
        dates.push("2022-01-01");
        dates.push("2022-02-01");
        dates.push("2022-03-01");
        dates.push("2022-04-01");
        dates.push("2022-05-01");
        dates.push("2022-06-01");
        dates.push("2022-07-01");
        dates.push("2022-08-01");
        dates.push("2022-09-01"); // from here on we can choose any date in the month. Keep it on 1 for now 
        dates.push("2022-10-01");
        dates.push("2022-11-01");
        dates.push("2022-12-01");
        dates.push("2023-01-01");
        dates.push("2023-02-01");
        dates.push("2023-03-01");
        dates.push("2023-04-01");
        dates.push("2023-05-01");
        dates.push("2023-06-01");
        dates.push("2023-07-01");
        dates.push("2023-08-01");
        dates.push("2023-09-01");
        dates.push("2023-10-01");
        dates.push("2023-11-01");
        dates.push("2023-12-01");
        dates.push("2024-01-01");
        dates.push("2024-02-01");
        dates.push("2024-03-01");
        dates.push("2024-04-01");

        return dates; 
    }
    else if ( dateType === "first_tuesdays" ) {
        const dates = [];
        dates.push("2022-09-06");
        dates.push("2022-10-04");
        dates.push("2022-11-08");
        dates.push("2022-12-06");
        dates.push("2023-01-03");
        dates.push("2023-02-07");
        dates.push("2023-03-07");
        dates.push("2023-04-04");
        dates.push("2023-05-02");
        dates.push("2023-06-06");
        dates.push("2023-07-04");
        dates.push("2023-08-08");
        dates.push("2023-09-05");
        dates.push("2023-10-03");
        dates.push("2023-11-07");
        dates.push("2023-12-05");
        dates.push("2024-01-02");
        dates.push("2024-02-06");
        dates.push("2024-03-05");
        dates.push("2024-04-02");

        return dates; 
    }
    else if ( dateType === "first_and_third_tuesdays" ) {
        const dates = getDateArray("first_tuesdays");
        // now just add the third tuesdays
        // NOTE!!! this assumes queries have ORDER BY DATE in them to properly interlace the dates!!
        dates.push("2022-09-20");
        dates.push("2022-10-03");
        dates.push("2022-10-18");
        dates.push("2022-11-22");
        dates.push("2022-12-20");
        dates.push("2023-01-17");
        dates.push("2023-02-21");
        dates.push("2023-03-21");
        dates.push("2023-04-18");
        dates.push("2023-05-16");
        dates.push("2023-06-20");
        dates.push("2023-07-18");
        dates.push("2023-08-22");
        dates.push("2023-09-19");
        dates.push("2023-10-17");
        dates.push("2023-11-21");
        dates.push("2023-12-19");
        dates.push("2024-01-16");
        dates.push("2024-02-20");
        dates.push("2024-03-19");
        dates.push("2024-04-16");

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