// From: https://github.com/googleapis/nodejs-bigquery/blob/main/samples/query.js

'use strict';

async function processRawBigquery() {
    const processing = require("./src/processing");
    const fs = require("fs").promises;
    
    // const rawData = await fs.readFile("./data-cache/PROTOCOL_DEVICE.json", "utf8");
    // const processedData = processing.processRawBigquery( JSON.parse(rawData.toString()), "protocol" );

    const data = await fs.readFile("./data-cache/USERAGENTFAMILY_DEVICE.json", "utf8");
    const processedData = processing.processRawBigquery( JSON.parse(data.toString()), "useragent" );

    console.log( processedData );
    console.log( JSON.stringify(processedData) );

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
        // for all dates for the protocol query, took about 737 MB. For useragent, was 1 GB
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
        dates.push("2022-09-01"); // here we can choose any date. Keep it on 1 for now 
        dates.push("2022-10-01");
        dates.push("2022-11-01");
        dates.push("2022-12-01");
        dates.push("2023-01-01");
        dates.push("2023-02-01");
        dates.push("2023-03-01");
    
        let datesString = "";
    
        for ( const [idx, date] of dates.entries() ) {
            datesString += "DATE = \"" + date + "\"";
            if ( idx != dates.length - 1 )
                datesString += " OR ";
        }
    
        // const query = `SELECT DATE as date, DEVICETYPE as device, COUNT(*) as amount FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE`;
    
        // const query = `SELECT DATE as date, PROTOCOL as protocol, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE,PROTOCOL ORDER BY DATE ASC, DEVICETYPE ASC, PROTOCOL ASC`;
    
        const query = `SELECT DATE as date, USERAGENTFAMILY as useragent, DEVICETYPE as device, COUNT(*) as rowcount, SUM(BEACONS) as beaconcount, FROM \`akamai-mpulse-rumarchive.rumarchive.rumarchive_page_loads\` WHERE ${datesString} GROUP BY DATE,DEVICETYPE,USERAGENTFAMILY ORDER BY DATE ASC, DEVICETYPE ASC, USERAGENTFAMILY ASC`;
    

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
            console.log( JSON.stringify(rows) );


            const fs = require("fs").promises;

            await fs.writeFile( "./data-cache/QUERY_OUTPUT.json", JSON.stringify(rows), "utf8" );
        }
    }

    await query();
}

function main() {
    // runBigquery(true);
    processRawBigquery();
}

main(...process.argv.slice(2));