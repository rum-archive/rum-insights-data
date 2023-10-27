// From: https://github.com/googleapis/nodejs-bigquery/blob/main/samples/query.js

'use strict';

async function processRawBigquery() {
    const processing = require("./src/processing");
    const fs = require("fs").promises;
    
    // const rawData = await fs.readFile("./data-cache/PROTOCOL_DEVICE.json", "utf8");
    // const processedData = processing.processRawBigquery( JSON.parse(rawData.toString()), "protocol" );

    // const data = await fs.readFile("./data-cache/USERAGENTFAMILY_DEVICE.json", "utf8");
    // const processedData = processing.processRawBigquery( JSON.parse(data.toString()), "useragent" );

    // const filename = "NAVIGATIONTYPE_DEVICE.json";
    // const filename = "NAVIGATIONTYPE.json";
    // const metricName = "navigationtype";

    // const filename = "VISIBILITYSTATE_DEVICE.json";
    // const filename = "VISIBILITYSTATE.json";
    // const metricName = "visibilitystate";

    // const filename = "LANDINGPAGE_DEVICE.json";
    const filename = "LANDINGPAGE.json";
    const metricName = "landingpage";


    let data = await fs.readFile("./data-cache/" + filename, "utf8");
    data = JSON.parse(data.toString());
    data = data.filter( point => point[metricName] !== null && point[metricName] !== "null" ); // filter out null-values. WARNING: impacts overall percentages!!
    const processedData = processing.processRawBigquery( data, metricName );

    console.log( processedData );
    // console.log( JSON.stringify(processedData) );

    await fs.writeFile( "./data-output/" + filename, JSON.stringify(processedData), "utf8" );

    return;
}

async function processHistogramBigquery() {
    const processing = require("./src/processing");
    const fs = require("fs").promises;

    // const filename = "NAVIGATIONTYPE_DEVICE_TTFBHIST.json";
    // const metricName = "navigationtype";
    // const histogramName = "TTFBHISTOGRAM";

    const filename = "VISIBILITYSTATE_DEVICE_LCPHIST.json";
    const metricName = "visibilitystate";
    const histogramName = "LCPHISTOGRAM";

    let data = await fs.readFile("./data-cache/" + filename, "utf8");
    data = JSON.parse(data.toString());
    data = data.filter( point => point[metricName] !== null && point[metricName] !== "null" ); // filter out null-values. WARNING: impacts overall percentages!!
    const processedData = processing.processHistogramBigquery( data, metricName, histogramName );

    console.log( processedData );
    // console.log( JSON.stringify(processedData) );

    await fs.writeFile( "./data-output/" + filename, JSON.stringify(processedData), "utf8" );

    return;
}

async function runBigquery(dryRun){
    const {BigQuery} = require('@google-cloud/bigquery');
    const bigquery = new BigQuery();
    
    async function query() {
        // const dryRun = false;
    
        const dates = [];
        // from october 2021 to august 2022, we only have the 1st of each month
        // from september 2022 and after, we have each day
        // dates.push("2021-10-01");
        // dates.push("2021-11-01");
        // dates.push("2021-12-01");
        // dates.push("2022-01-01");
        // dates.push("2022-02-01");
        // dates.push("2022-03-01");
        // dates.push("2022-04-01");
        // dates.push("2022-05-01");
        // dates.push("2022-06-01");
        // dates.push("2022-07-01");
        // dates.push("2022-08-01");
        // dates.push("2022-09-01"); // from here on we can choose any date in the month. Keep it on 1 for now 
        // dates.push("2022-10-01");
        // dates.push("2022-11-01");
        // dates.push("2022-12-01");
        // dates.push("2023-01-01");
        // dates.push("2023-02-01");
        // dates.push("2023-03-01");
        // dates.push("2023-04-01");
        // dates.push("2023-05-01");
        dates.push("2023-06-01");
    
        let datesString = "";
    
        for ( const [idx, date] of dates.entries() ) {
            datesString += "DATE = \"" + date + "\"";
            if ( idx != dates.length - 1 )
                datesString += " OR ";
        }
    
        // const query = `SELECT DATE as date, DEVICETYPE as device, COUNT(*) as amount FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE`;
    
        // const query = `SELECT DATE as date, PROTOCOL as protocol, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE,PROTOCOL ORDER BY DATE ASC, DEVICETYPE ASC, PROTOCOL ASC`;
    
        // const query = `SELECT DATE as date, USERAGENTFAMILY as useragent, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE,USERAGENTFAMILY ORDER BY DATE ASC, DEVICETYPE ASC, USERAGENTFAMILY ASC`;
        const query = `SELECT DATE as date, USERAGENTFAMILY as useragent, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE, USERAGENTFAMILY ORDER BY DATE ASC, USERAGENTFAMILY ASC`;
                
        // const query = `SELECT DATE as date, NAVIGATIONTYPE as navigationtype, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE,NAVIGATIONTYPE ORDER BY DATE ASC, DEVICETYPE ASC, NAVIGATIONTYPE ASC`;
        // const query = `SELECT DATE as date, NAVIGATIONTYPE as navigationtype, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,NAVIGATIONTYPE ORDER BY DATE ASC, NAVIGATIONTYPE ASC`;
    
        // const query = `SELECT DATE as date, VISIBILITYSTATE as visibilitystate, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE,VISIBILITYSTATE ORDER BY DATE ASC, DEVICETYPE ASC, VISIBILITYSTATE ASC`;
        // const query = `SELECT DATE as date, VISIBILITYSTATE as visibilitystate, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,VISIBILITYSTATE ORDER BY DATE ASC, VISIBILITYSTATE ASC`;
    
        // const query = `SELECT DATE as date, LANDINGPAGE as landingpage, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE,LANDINGPAGE ORDER BY DATE ASC, DEVICETYPE ASC, LANDINGPAGE ASC`;
        // const query = `SELECT DATE as date, LANDINGPAGE as landingpage, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,LANDINGPAGE ORDER BY DATE ASC, LANDINGPAGE ASC`;


        // \`akamai-mpulse-rumarchive.rumarchive.COMBINE_HISTOGRAMS\`(ARRAY_AGG(PLTHISTOGRAM)) AS PLTHISTOGRAM,
        // const query = `SELECT DATE as date, NAVIGATIONTYPE as navigationtype, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, 
        //                 \`akamai-mpulse-rumarchive.rumarchive.COMBINE_HISTOGRAMS\`(ARRAY_AGG(TTFBHISTOGRAM)) AS TTFBHISTOGRAM,
        //                 FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` 
        //                 WHERE ${datesString} 
        //                 GROUP BY DATE, DEVICETYPE, NAVIGATIONTYPE 
        //                 ORDER BY DATE ASC, DEVICETYPE ASC, NAVIGATIONTYPE ASC`;
    
        // const query = `SELECT DATE as date, NAVIGATIONTYPE as navigationtype, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, 
        //                 \`akamai-mpulse-rumarchive.rumarchive.COMBINE_HISTOGRAMS\`(ARRAY_AGG(TTFBHISTOGRAM)) AS TTFBHISTOGRAM,
        //                 FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` 
        //                 WHERE ${datesString} 
        //                 GROUP BY DATE, NAVIGATIONTYPE 
        //                 ORDER BY DATE ASC, NAVIGATIONTYPE ASC`;

        // const query = `SELECT DATE as date, VISIBILITYSTATE as visibilitystate, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, 
        //                 \`akamai-mpulse-rumarchive.rumarchive.COMBINE_HISTOGRAMS\`(ARRAY_AGG(LCPHISTOGRAM)) AS LCPHISTOGRAM,
        //                 FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` 
        //                 WHERE ${datesString} 
        //                 GROUP BY DATE, DEVICETYPE, VISIBILITYSTATE 
        //                 ORDER BY DATE ASC, DEVICETYPE ASC, VISIBILITYSTATE ASC`;


        // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
        const options = {
            query: query,
            //   // Location must match that of the dataset(s) referenced in the query.
            //   location: 'US',
            dryRun: dryRun,
        };
    
        // Run the query as a job
        const [job] = await bigquery.createQueryJob(options);
        console.log(`Job ${job.id} started.`);
    
        if ( dryRun ) {
            // console.log('Status:');
            // console.log(job.metadata.status);
            console.log('\nJob Statistics:');
            console.log("Query dryrun: " + query);
            console.log("Estimated mebibytes: " + (job.metadata.statistics.totalBytesProcessed / (1024 * 1024)));
            console.log(job.metadata.statistics);
        }
        else {
            // Wait for the query to finish
            const [rows] = await job.getQueryResults();
    
            // Print the results
            console.log('Rows:');
            rows.forEach(row => console.log(row));
    
            // console.log( JSON.stringify(rows, null, 4) );
            // console.log( JSON.stringify(rows) );


            const fs = require("fs").promises;

            await fs.writeFile( "./data-cache/QUERY_OUTPUT.json", JSON.stringify(rows), "utf8" );
        }
    }

    await query();
}

function main() {
     runBigquery(true); // pass true for dry run
    // processRawBigquery();
   // processHistogramBigquery();
}

main(...process.argv.slice(2));