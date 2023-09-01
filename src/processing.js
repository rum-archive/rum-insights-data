
function processRawBigquery(rows, metricFieldName) {
    /*
        Incoming data are rows of raw bigquery results.
        We assume the rows are ordered by ASC date!
        For example:
            {
                date: { value: '2022-12-01' },
                useragent: 'ev-crawler',
                device: 'Desktop',
                rowcount: 1,
                beaconcount: 9
            },

        We need to do multiple things:
        1. Group by devicetype + date to find out the toal row and beacon counts of that device+date 
            --> (so we can have a percentage for each individual value for that date, since not all dates have an equal amount of beacons)
        2. Transform the output to the expected format (same as the one HTTPArchive uses)

        For example, for percentiles:
            {
                "client": "desktop",
                "date": "2023_03_01",
                "p10": "108.95",
                "p25": "243.66",
                "p50": "529.97",
                "p75": "1022.57",
                "p90": "1619.98",
                "timestamp": "1611153000000"
            },

        For example, for direct percentages:
            {
                "client": "desktop",
                "date": "2020_01_01",
                "percent": "0.0",
                "timestamp": "1611153000000"
            },
    */

    // 1. Aggregate counts per device and date so we can calculate percentages later 
    //      the output is grouped by date first, so keep that structure here  (so  dateCount = Map<DATESTRING, Map<DEVICESTRING, Counts>>)
    const dateCounts = new Map();

    for( const row of rows ) {
        const date = row.date.value;
        const device = row.device;

        let dateCount = dateCounts.get(date);
        if ( dateCount ){
            let deviceCount = dateCount.get( device );
            if ( deviceCount ) {
                deviceCount.rowCount += row.rowcount;
                deviceCount.beaconCount += row.beaconcount;
            }
            else {
                // date exists, but this device not yet: add it!
                deviceCount = {
                    rowCount: row.rowcount,
                    beaconCount: row.beaconcount
                };
    
                dateCount.set( device, deviceCount );
            }
        }
        else {
            // date not found, so also no deviceTypes kept yet
            let deviceCount = {
                rowCount: row.rowcount,
                beaconCount: row.beaconcount
            };

            const deviceCounts = new Map();
            deviceCounts.set( device, deviceCount );

            dateCounts.set( date, deviceCounts );
        }
    }

    // 2. Transform to output format + calculate percentages
    const output = [];
    for( const row of rows ) {
        /*
        From
            {
                date: { value: '2022-12-01' },
                useragent: 'ev-crawler',
                device: 'Desktop',
                rowcount: 1,
                beaconcount: 9
            },
        To
            {
                "useragent": "ev-crawler",
                "client": "desktop",
                "date": "2020_01_01",
                "percent": "0.0"
            },
        */

        const datapoint = {};
        
        if ( row.device )
            datapoint.client = row.device.toLowerCase();
        else
            datapoint.client = "unknown";

        datapoint.date = row.date.value.replaceAll("-", "_"); // "2022-12-01" to "2022_12_01"

        const deviceCount = dateCounts.get( row.date.value ).get( row.device );
        datapoint.percent = ((row.beaconcount / deviceCount.beaconCount) * 100).toFixed(1);

        datapoint[metricFieldName] = row[metricFieldName]; // e.g., .protocol, .useragent, etc.

        // highcharts uses raw timestamps, so pre-calculate them
        datapoint.timestamp = "" + (new Date( row.date.value ).getTime());

        output.push( datapoint );
    }

    return output;
}

function processHistogramBigquery(rows, metricFieldName, histogramFieldName) {
    let output = [];
    
    for( let row of rows ) {
        let histogram = JSON.parse( row[histogramFieldName] );

        const datapoint = {};
        
        if ( row.device )
            datapoint.client = row.device.toLowerCase();
        else
            datapoint.client = "unknown";

        datapoint.date = row.date.value.replaceAll("-", "_"); // "2022-12-01" to "2022_12_01"

        // const deviceCount = dateCounts.get( row.date.value ).get( row.device );
        // datapoint.percent = ((row.beaconcount / deviceCount.beaconCount) * 100).toFixed(1);

        datapoint[metricFieldName] = row[metricFieldName]; // e.g., .protocol, .useragent, etc.

        // highcharts uses raw timestamps, so pre-calculate them
        datapoint.timestamp = "" + (new Date( row.date.value ).getTime());

        // keep only high precision buckets without the bucket number
        // TODO: document a bit better what I'm doing here ;) 
        datapoint.histogram = Object.values(Object.entries(histogram).filter( o => parseInt(o[0]) > 0 && parseInt(o[0]) < 101).map( o => o[1] ));

        output.push( datapoint );
    }

    // console.log( output );

    return output;
}


module.exports = {
    processRawBigquery,
    processHistogramBigquery
}