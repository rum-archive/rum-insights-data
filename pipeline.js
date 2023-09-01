const fs = require('fs/promises');
const path = require('path');
const {BigQuery} = require('@google-cloud/bigquery');

const processing = require("./src/processing");

'use strict';

// set to true to get more output and dryrun queries
let GLOBAL_DEBUG = false;
// set to true to force each query to run for just a single date (cheaper/less data)
let FORCE_SINGLE_DATE = false;

async function processResults( bigQueryResults ) {
    for ( const query of bigQueryResults ) {
        if ( query.error ) {
            // something happened in the previous stage, can't process results that don't exist, skipping
            continue;
        }

        try {
            const metricName = query.extractmetric;
            const outputPath = "./data-output/" + query.filename + ".json";

            let data = await fs.readFile( query.cachePath, "utf8");
            data = JSON.parse(data.toString());

            // filter out null-values by default. WARNING: impacts overall percentages!!
            // null values are data that can't be attributed to the queried dimension (e.g., an unknown useragent, or a beacon with a missing visibilitystate value)
            data = data.filter( point => point[metricName] !== null && point[metricName] !== "null" );

            let processedData = [];
            if ( query.processingtype === "metric" ) {
                processedData = processing.processRawBigquery( data, metricName );
            }
            else if ( query.processingtype === "histogram" ) {
                processedData = processing.processHistogramBigquery( data, metricName, query.extracthistogram );
            }
            else {
                throw Error("processResults: unknown processingtype on this query: " + query.processingtype);
            }

            // console.log( processedData );

            await fs.writeFile( outputPath, JSON.stringify(processedData), "utf8" );
        }
        catch(e) {
            query.error = e;
            console.error("processResults: processing error: ", e);
        }
    }

    return bigQueryResults;
}

async function runQueries( queries ) {
    // queries is a list of the transformed .jsonl files from /queries
    // query.sql should be ready to pass to a bigQuery API call
    for ( const query of queries ) {
        try {
            query.cachePath = ""; // if error, this will remain empty for this query
            const cachePath = "./data-cache/" + query.filename + ".json";

            await runBigQuery( query.sql, cachePath, GLOBAL_DEBUG );
            query.cachePath = cachePath;
        }
        catch(e) {
            query.error = e;
            console.error("runQueries: bigQuery error: ", e);
        }
    }

    return queries; // we just keep passing around the same array, as we update properties of the query objects with new info for the next stage
}

async function runBigQuery(querySQLString, outputPath, dryRun) {

    // From: https://github.com/googleapis/nodejs-bigquery/blob/main/samples/query.js
    const bigquery = new BigQuery();

    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const options = {
        query: querySQLString,
        //   // Location must match that of the dataset(s) referenced in the query.
        //   location: 'US',
        dryRun: dryRun,
    };

    // Run the query as a job
    const [job] = await bigquery.createQueryJob(options);
    // console.log(`Job ${job.id} started.`);

    if ( dryRun ) {
        console.log('runBigQuery: Job Statistics:');
        console.log('Status:', job.metadata.status);
        console.log("Query dryrun: " + querySQLString);
        console.log("Estimated mebibytes: " + (job.metadata.statistics.totalBytesProcessed / (1024 * 1024)));
        console.log(job.metadata.statistics);

        console.log("No output written to ", outputPath);

        throw Error("runBigQuery: DryRun enabled: no actual query executed");
    }
    else {
        const [rows] = await job.getQueryResults();

        // console.log('Rows:');
        // rows.forEach(row => console.log(row));

        // console.log( JSON.stringify(rows, null, 4) );
        // console.log( JSON.stringify(rows) );

        // this will overwrite by default (no append)
        await fs.writeFile( outputPath, JSON.stringify(rows), "utf8" );
    }
}

function getFullDateRange() {
    // TODO: should not just take the first (01) of each month 
    // but something like "the first monday of each month" to get more consistent results

    const dates = [];
    // from october 2021 to august 2022, we only have the 1st of each month
    // from september 2022 and after, we have each day
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

    return dates;
}

async function getQueries() {
    const QUERYDIR = "./queries";

    // mechanism to allow running only specific queries during local testing/debugging
    // if the array is empty, we get query names from the filesystem (listing in the /queries dir)
    // if the array is not empty, we only use those query names
    // example override: ["useragentfamily_devicetype.jsonl"]; // this would only execute this one query
    // let queryNames = ["useragentfamily_devicetype.jsonl"];
    // let queryNames = ["LCP_visibilitystate_devicetype.jsonl"];
    let queryNames = [];

    if( queryNames.length === 0 ){
        queryNames = await fs.readdir(QUERYDIR);

        if ( queryNames.length === 0 ) {
            throw Error("getQueries: no queries found to execute, aborting...");
        }
    }

    let allDates = getFullDateRange();
    const lastDate = allDates[ allDates.length - 1 ];
    if ( FORCE_SINGLE_DATE ) { // queries are much cheaper when only running for a single date
        allDates = [ lastDate ];
    }

    // TODO: add logic to read cached bigquery results and determine the actual dates needed to get new data for (cut down on processing costs)

    // concatenate the dates to be used in the sql queries
    let allDatesString = "";
    for ( const [idx, date] of allDates.entries() ) {
        allDatesString += "DATE = \"" + date + "\"";
        if ( idx != allDates.length - 1 )
            allDatesString += " OR ";
    }
    const lastDateString = "DATE = \"" + lastDate + "\"";

    // run the actual query definitions from filesystem
    // expected JSON structs:
    /*
        {
            // required
            datetype: "timeseries" | "single",
            processingtype: "metric" | "histogram",
            extractmetric: string, // needed for both metric and histogram
            sql: multiline-string,

            // semi-optional
            extracthistogram: string, // only if processingtype === histogram

            // optional
            description: string,
            filename: string
        }
    */

    const output = [];

    for ( let queryName of queryNames ) {
        let queryContentRaw = await fs.readFile( path.join(QUERYDIR, queryName), "utf8" );
        // get rid of newlines in strings from .jsonl
        // note: there are still swaths of extra spaces/tabs in the SQL string now, but bigQuery can deal with those just fine
        queryContentRaw = queryContentRaw.toString().replaceAll("\n","");

        const queryContent = JSON.parse(queryContentRaw);

        if( queryContent.datetype === "timeseries" )
            queryContent.sql = queryContent.sql.replaceAll("{{TIMESERIES_DATES}}", allDatesString);
        else if ( queryContent.datetype === "single" )
            queryContent.sql = queryContent.sql.replaceAll("{{LAST_DATE}}", lastDateString);

        if ( !queryContent.filename ) // allow manual overrides in the input file
            queryContent.filename = path.parse(queryName).name; // filename without the extension, used later in the pipeline for storing results

        output.push( queryContent );

        console.log("getQueries:queryContent:", queryName, queryContent);
    }

    return output;
}

async function runPipeline() {
    // 1. gather queries to be executed
    // 2. execute queries, store results in cache
    // 3. transform cached results to actual output

    const queries = await getQueries();
    const bigQueryResults = await runQueries(queries);
    const processedResults = await processResults(bigQueryResults);

    let successCount = 0;
    let errorCount = 0;

    for ( let result of processedResults ) {
        (result.error) ? ++errorCount : ++successCount;
    }

    console.log("Queries executed successfully: ", successCount);
    console.log("Queries with errors: ", errorCount);

    for ( let result of processedResults ) {
        if ( result.error ){
            console.log( result.filename, result.error );
        }
    }
}

function main() {
    
    GLOBAL_DEBUG = false;
    FORCE_SINGLE_DATE = false;

    runPipeline();
}

main(...process.argv.slice(2));