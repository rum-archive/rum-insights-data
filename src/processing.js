function processSingleMetricGlobal(rows, metricFieldName){
    // used when the data isn't pre-split per device type (or the field we're looking at is devicetype itself ;))
    // this means we don't split per client/device type, but rather have a single "global" count for everything per date
    // in essence, a simpler version of processSingleMetricPerDevicetype
    /*
        Incoming data example format:
            {
                date: { value: '2022-12-01' },
                device: 'Desktop',
                rowcount: 1,
                beaconcount: 9
            },
        Outgoing data example:
            {
                "client": "desktop",
                "date": "2020_01_01",
                "percent": "15",
                "timestamp": "1611153000000"
            },
    */
   
    // need to keep a count of total beacons seen for a single date, so we can calculate percentages for that date later
    const dateCounts = new Map(); // Map<DATESTRING, TotalBeaconCount>)
    
    for( const row of rows ) {
        const date = row.date.value;

        let dateCount = dateCounts.get(date);
        if ( !dateCount )
            dateCount = 0;

        dateCount += row.beaconcount;
        dateCounts.set( date, dateCount );
    }

    // step 2: now that we have the total counts per day, calculate percentages and transform to output format
    const output = [];
    for( const row of rows ) {

        const datapoint = {};

        datapoint.date = row.date.value.replaceAll("-", "_"); // "2022-12-01" to "2022_12_01"

        const dateTotalBeaconCount = dateCounts.get( row.date.value );
        let percent = ((row.beaconcount / dateTotalBeaconCount) * 100);
        datapoint.percent = percent.toFixed(1);

        datapoint[metricFieldName] = row[metricFieldName]; // e.g., .device

        // highcharts uses raw timestamps, so pre-calculate them
        datapoint.timestamp = "" + (new Date( row.date.value ).getTime());
        
        if ( percent > 0.1 ) {
            // skip entries that are 0.0 percent (especially in high-cardinality queries, these often account for many megabytes of output data)
            output.push( datapoint );
        }
    }

    return output;
}

function processSingleMetricPerDevicetype(rows, metricFieldName) {
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
        const percent = ((row.beaconcount / deviceCount.beaconCount) * 100);
        datapoint.percent = percent.toFixed(1);

        datapoint[metricFieldName] = row[metricFieldName]; // e.g., .protocol, .useragent, etc.

        // highcharts uses raw timestamps, so pre-calculate them
        datapoint.timestamp = "" + (new Date( row.date.value ).getTime());

        if( percent > 0.1 ) {
            // skip entries that are 0.0 percent (especially in high-cardinality queries, these often account for many megabytes of output data)
            output.push( datapoint );
        }
    }

    return output;
}


function processGroupedMetricPerDevicetype(rows, metricFieldName, groupbyFieldName, includeFullCount = false) {
    /*
        This is similar to processSingleMetricPerDevicetype, but the metrics are grouped in another dimension
        For example, SingleMetric would be
            deviceModel per deviceType
        while GroupedMetric would be
            deviceModel per country per deviceType
        Where the results are again grouped by country.

        Incoming data are rows of raw bigquery results.
        We assume the rows are ordered by ASC date!
        For example:
            {
                date: { value: '2022-12-01' },
                model: 'Apple iPhone',
                country: 'US',
                device: 'Desktop',
                rowcount: 1,
                beaconcount: 9
            },

        We need to do multiple things:
        1. Group by extra dimension (groupbyFieldName) + devicetype + date to find out the toal row and beacon counts of that dimension+device+date 
            --> (so we can have a percentage for each individual value for that date, since not all dates have an equal amount of beacons)
        2. Transform the output to the expected format (same as the one HTTPArchive uses)
            (see processSingleMetricPerDevicetype for more details on that)
    */

    // TODO: this could benefit from a more generic implementation allow any sequence of groupings (i.e., treating date and devicetype as a normal grouping as well)
    //       For now, I decided to keep them split for clarity and to wait until clear patterns emerge before deciding to refactor

    // Note: conceptually, I could use subsequent .filter() calls to get only the individual sets I need, but this would mean iterating over the data multiple times and creating new arrays each time
    //  the approach below is more annoying to program, but should be more efficient (looping over data just once to setup the main data structure, then once to transform to output format)


    // 1. Aggregate counts per groupbyFieldName + device + date so we can calculate percentages later 
    //      the output is grouped by date first, so keep that structure here  (so  dateCount = Map<DATESTRING, Map<DEVICESTRING, MAP<groupbyFieldname, Counts>>>)
    const dateCounts = new Map();

    for( const row of rows ) {
        const date = row.date.value;
        const device = row.device;
        const groupbyDimension = row[ groupbyFieldName ]; // e.g., "country"

        let dateEntry = dateCounts.get( date );
        if ( !dateEntry ){
            dateEntry = new Map();
            dateCounts.set( date, dateEntry );
        }

        let deviceEntry = dateEntry.get( device );
        if ( !deviceEntry ) {
            deviceEntry = new Map();
            dateEntry.set( device, deviceEntry );
        }

        let groupbyEntry = deviceEntry.get( groupbyDimension );
        if ( !groupbyEntry ) {
            groupbyEntry = {
                rowCount: row.rowcount,
                beaconCount: row.beaconcount
            };
            deviceEntry.set( groupbyDimension, groupbyEntry );
        }
        else {
            groupbyEntry.rowCount += row.rowcount;
            groupbyEntry.beaconCount += row.beaconcount;
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

        const groupbyValue = row[ groupbyFieldName ];

        if ( groupbyValue ) {
            datapoint[ groupbyFieldName.toLowerCase() ] = groupbyValue;
        }
        else {
            datapoint[ groupbyFieldName.toLowerCase() ] = "unknown";
        }

        datapoint.date = row.date.value.replaceAll("-", "_"); // "2022-12-01" to "2022_12_01"

        datapoint[metricFieldName] = row[metricFieldName]; // e.g., .protocol, .useragent, .model, etc.

        const dimensionCount = dateCounts.get( row.date.value ).get( row.device ).get( groupbyValue );

        const percent = ((row.beaconcount / dimensionCount.beaconCount) * 100);
        datapoint.percent = percent.toFixed(1);

        // highcharts uses raw timestamps, so pre-calculate them
        datapoint.timestamp = "" + (new Date( row.date.value ).getTime());

        if ( includeFullCount ) {
            datapoint.count = row.beaconcount;
        }

        if( percent > 0.1 ) {
            // skip entries that are 0.0 percent (especially in high-cardinality queries, these often account for many megabytes of output data)
            output.push( datapoint );
        }
    }

    return output;
}

function processHistogramPerDevicetype(rows, metricFieldName, histogramFieldName) {
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

function processCWVperUseragent(data) {
    let outputPerAgent = [];
    let outputPerAgentGroup = [];

    // for CWVCounts, we try to get an idea of how many beacons there are for a certain CWV metric, compared to all beacons
    // this because not all browsers can measure all CWVs and even for those that can, there isn't always a CWV measurement included in all beacons.

    // data contains a list of points per useragent:
    // {"useragent":"Chrome","BeaconsWithCWV":13643033,"rowcount":66342,"topLevelBeacons":18263565,"AllUserAgentBeacons":32912592,"TotalBeacons":49301845}

    // BeaconsWithCWV are the actual amount of beacons that had the CWV set for that user agent
    // AllUserAgentBeacons are all the beacons seen for that user agent (e.g., for Chrome), both with and without CWV set
    // TotalBeacons are all beacons seen for that DATE (should be the same for each datapoint), both with and without CWV set
    // (topLevelBeacons is a bit weird and should not be used here)

    // for some reason, SUM(AllUserAgentBeacons) != TotalBeacons here... it SHOULD, but it doesn't
    // so, to be fairer, manually calculate SUM(AllUserAgentBeacons) ourselves and use that as top limit

    let totalBeacons = 0;
    let totalCWVbeacons = 0;
    for ( let datapoint of data ){
        totalBeacons += datapoint.AllUserAgentBeacons;
        totalCWVbeacons += datapoint.BeaconsWithCWV;
    }

    //console.log( totalBeacons + " ?= " + data[0].TotalBeacons + ", and how many cwv points? " + totalCWVbeacons );

    // now, we want to calculate 2 different output files:
    // a) per useragent: 
    //      - CWVpercentage: percentage of CWV beacons both compared to total CWV beacons (indicate how many CWV beacons come from this browser)
    //      - UserAgentPercentage: percentage of user agent beacons compared to total beacons (indicate how many beacons in general come from this browser)

    // b) per group of useragents: chromium-based vs webkit-based groupings (to get CrUX generalizability indications)
    //      - CWVpercentage: percentage of CWV beacons both compared to total CWV beacons (indicate how many CWV beacons come from this browser group)
    //      - UserAgentPercentage: percentage of user agent beacons compared to total beacons (indicate how many beacons in general come from this browser group)

    // Case a):
    for ( let row of data ){
        // target output format:
        /* 
            {
                "useragent": "ev-crawler",
                "device": "desktop",
                "date": "2020_01_01",
                "cwvpercent": "3.0",
                "useragentpercent": "15.4"
            },
        */

        const datapoint = {};

        datapoint.device = row.device.toLowerCase();
        datapoint.date = row.date.value.replaceAll("-", "_"); // "2022-12-01" to "2022_12_01"

        datapoint.cwvpercent = ((row.BeaconsWithCWV / totalCWVbeacons) * 100).toFixed(1);
        datapoint.useragentpercent = ((row.AllUserAgentBeacons / totalBeacons) * 100).toFixed(1);

        datapoint.useragent = row.useragent;

        outputPerAgent.push( datapoint );
    }


    // Case b):
    // the groupings are different for desktop vs mobile
    // for crux eligibility, we use this guidance https://developer.chrome.com/docs/crux/methodology/#user-eligibility
    // mainly: non-iOS Chrome browsers and webview, excluding other chromium stacks like Edge or Samsung Internet
    // Note: I also confirmed that "Chrome Mobile iOS" is the only version of Chrome on iOS. The rest is android-only (same for Firefox iOS vs Firefox Mobile)

    // logic here is same as above, but first make SUMs of the individual counts per group
    const desktopGroups = [];
        desktopGroups.push({
            name: "Others",
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });
        
        desktopGroups.push({
            name: "Chromium (CWV, no CrUX)",
            useragents: ["Edge", "Opera", "Samsung Internet"],
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });
        
        desktopGroups.push({
            name: "Chrome (CWV + CrUX)", // only CruX-eligible browsers
            useragents: ["Chrome", "Chrome Mobile WebView" ],
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });
        
        desktopGroups.push({
            name: "Safari", 
            useragents: ["Safari"],
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });
        
        desktopGroups.push({
            name: "Firefox", 
            useragents: ["Firefox"],
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });

    const mobileGroups = [];
        mobileGroups.push({
            name: "Others",
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });

        mobileGroups.push({
            name: "Chromium (CWV, no CrUX)", // all chromium-based browsers
            useragents: ["Samsung Internet", "Samsung", "Opera Mobile"], // "Opera Mobile is Android only"
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });
        
        mobileGroups.push({
            name: "Chrome (CWV + CrUX)", // only CruX-eligible browsers
            useragents: ["Chrome Mobile", "Chrome Mobile WebView", "Chrome" ],
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });

        mobileGroups.push({
            name: "Webkit (Others, no CWV)",
            useragents: ["Chrome Mobile iOS", "Firefox iOS", "Edge Mobile" ],
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });

        mobileGroups.push({
            name: "Webkit (Safari, no CWV)",
            useragents: ["Safari", "Mobile Safari", "Mobile Safari UI/WKWebView"],
            datapoints: [],
            beaconsWithCWV: 0,
            allUserAgentBeacons: 0
        });

    let deviceGroups;
    if ( data[0].device == "Desktop" ) // assume we have separate queries for Desktop and Mobile, so devicetype is always the same
        deviceGroups = desktopGroups;
    else
        deviceGroups = mobileGroups;

    for( let row of data ) {
        let group = undefined;

        // start at 1 to skip Others as special case at [0]
        for ( let i = 1; i < deviceGroups.length; ++i ) {
            if ( deviceGroups[i].useragents.indexOf(row.useragent) >= 0 ){
                group = deviceGroups[i];
                break;
            }
        }

        if ( !group ) {
            // no main group found, add to "others"
            group = deviceGroups[0];
        }

        group.datapoints.push( row );
        // already calculate the SUMs here for easier usage later
        group.beaconsWithCWV += row.BeaconsWithCWV;
        group.allUserAgentBeacons += row.AllUserAgentBeacons;
    }

    // console.log("OTHERS", deviceGroups[0]);
    
    // now we have the aggregated counts per group, now we need to calculate the percentages like for case a)

    for ( let group of deviceGroups ) {
        // intended output:
        /* 
            {
                "useragentgroup": "Chromium_CruX",
                "device": "desktop",
                "date": "2020_01_01",
                "cwvpercent": "3.0",
                "useragentpercent": "15.4"
            },
        */

        const datapoint = {};

        // assume all data is for the same device type and date
        datapoint.device = data[0].device.toLowerCase();
        datapoint.date = data[0].date.value.replaceAll("-", "_"); // "2022-12-01" to "2022_12_01"

        datapoint.useragentgroup = group.name;

        datapoint.cwvpercent = ((group.beaconsWithCWV / totalCWVbeacons) * 100).toFixed(1);
        datapoint.useragentpercent = ((group.allUserAgentBeacons / totalBeacons) * 100).toFixed(1);


        outputPerAgentGroup.push( datapoint );
    }


    return {"individual": outputPerAgent, "grouped": outputPerAgentGroup};
}


module.exports = {
    processSingleMetricGlobal,
    processSingleMetricPerDevicetype,
    processGroupedMetricPerDevicetype,
    processHistogramPerDevicetype,
    processCWVperUseragent
}