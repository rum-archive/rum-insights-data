const fs = require('fs/promises');
const fsSync = require('fs');
const path = require('path');
const {BigQuery} = require('@google-cloud/bigquery');

const processing = require("./src/processing");
const dates = require("./src/dates");
const bcd = require("./src/browser-compat-calc");

'use strict';

// set to true to get more output and dryrun queries
let GLOBAL_DEBUG = false;
// set to true to force each query to run for just a single date (cheaper/less data)
let FORCE_SINGLE_DATE = false;
// determines how we use the cached values in data-cache 
let MERGE_CACHE_INSTEAD_OF_OVERRIDE = true; // true is "smart", default

async function processResults( bigQueryResults ) {
    for ( const query of bigQueryResults ) {
        if ( query.error ) {
            // something happened in the previous stage, can't process results that don't exist, skipping
            continue;
        }

        const outputPath = "./data-output/" + query.filename + ".json";

        let data = await fs.readFile( query.cachePath, "utf8");
        data = JSON.parse(data.toString());

        try {
            if ( query.processingtype === "metricPerDevice" || query.processingtype === "histogramPerDevice" )
            {
                const metricName = query.extractmetric;

                // filter out null-values by default. WARNING: impacts overall percentages!!
                // null values are data that can't be attributed to the queried dimension (e.g., an unknown useragent, or a beacon with a missing visibilitystate value)
                data = data.filter( point => point[metricName] !== null && point[metricName] !== "null" );

                let processedData = [];
                if ( query.processingtype === "metricPerDevice" ) {
                    processedData = processing.processSingleMetricPerDevicetype( data, metricName );
                }
                else if ( query.processingtype === "histogramPerDevice" ) {
                    processedData = processing.processHistogramPerDevicetype( data, metricName, query.extracthistogram );
                }

                // console.log( processedData );

                await fs.writeFile( outputPath, JSON.stringify(processedData), "utf8" );

            }
            else if ( query.processingtype === "groupedMetricPerDevice" ){
                let processedData = [];

                // quick and dirty adding full counts for the recent versions for web features baseline testing
                // TODO: make this into a proper new processingtype? 
                if ( query.filename.includes("recent_useragentversion_useragentfamily_devicetype") )
                    processedData = processing.processGroupedMetricPerDevicetype( data, query.extractmetric, query.groupby, true );
                else 
                    processedData = processing.processGroupedMetricPerDevicetype( data, query.extractmetric, query.groupby, false );

                await fs.writeFile( outputPath, JSON.stringify(processedData), "utf8" );
            }
            else if ( query.processingtype === "metricGlobal" ){
                let processedData = [];
                processedData = processing.processSingleMetricGlobal( data, query.extractmetric );

                await fs.writeFile( outputPath, JSON.stringify(processedData), "utf8" );
            }
            else if ( query.processingtype === "CWVCountPerUseragent" ){
                let processedData = [];
                processedData = processing.processCWVperUseragent( data );

                await fs.writeFile( outputPath, JSON.stringify(processedData), "utf8" );
            }
            else {
                throw Error("processResults: unknown processingtype on this query: " + query.processingtype);
            }            
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

            if ( query.forceSkipBigQuery === true ){
                console.log("runQueries: query is already up to date; not executing in BigQuery again.", query.filename);
                query.cachePath = cachePath;
                continue;
            }
            else if( !MERGE_CACHE_INSTEAD_OF_OVERRIDE && fsSync.existsSync(cachePath) ) {
                // we have 2 modes of operation driven by MERGE_CACHE_INSTEAD_OF_OVERRIDE:
                // - naive: MERGE_CACHE_INSTEAD_OF_OVERRIDE = false : assume if there's something in the data-cache for a query, that is fully up to date and can just be re-used. Delete the file manually to re-run full query.
                // - smart: MERGE_CACHE_INSTEAD_OF_OVERRIDE = true : assume data-cache is outdated, determine what new data we need (which dates), then merge with what was already on disk
                // Note: some of the "smart" checks are done before this, so if we get here, we already know there is an update needed

                // naive mode: assume that if we have something in the data-cache for this query, it can be re-used
                // if you want to have the query run again, remove or rename the file in the data-cache
                console.log(`runQueries: Note: Query ${query.filename} not executed in BigQuery because already in data-cache.`);
                query.cachePath = cachePath;
                continue;
            }
            else {
                console.log(`runQueries: Executing Query ${query.filename} via BigQuery`);
            }

            await runBigQuery( query, cachePath, GLOBAL_DEBUG );
            query.cachePath = cachePath;
        }
        catch(e) {
            query.error = e;
            console.error("runQueries: bigQuery error: ", e);
        }
    }

    return queries; // we just keep passing around the same array, as we update properties of the query objects with new info for the next stage
}

async function runBigQuery(query, outputPath, dryRun) {

    // From: https://github.com/googleapis/nodejs-bigquery/blob/main/samples/query.js
    const bigquery = new BigQuery();

    // For all options, see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/query
    const options = {
        query: query.sql,
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
        console.log("Query dryrun: " + query.sql);
        console.log("Estimated mebibytes: " + (job.metadata.statistics.totalBytesProcessed / (1024 * 1024)));
        console.log(job.metadata.statistics);

        console.log("No output written to ", outputPath);

        throw Error("runBigQuery: DryRun enabled: no actual query executed");
    }
    else {
        let [rows] = await job.getQueryResults();

        // only merge if we're actually doing multiple days
        // if the query is only for a recent day, we always want to override
        if ( query.datetype !== "recent_day" && MERGE_CACHE_INSTEAD_OF_OVERRIDE ) {
            // "smart" mode: need to merge with existing cache
            if( fsSync.existsSync(outputPath) ) {
                const localDataRaw = await fs.readFile( outputPath, "utf8" );
                const cachedData = JSON.parse( localDataRaw );

                console.log("runBigQuery: merging new results with cached results", outputPath, "previous datapoints:", cachedData.length, "new datapoints:", rows.length);

                rows = [...cachedData, ...rows];
            }
            // else: probably new query, nothing cached yet, so nothing to merge
        }
        
        await fs.writeFile( outputPath, JSON.stringify(rows), "utf8" );
    }
}

async function getQueries() {
    const QUERYDIR = "./queries";

    // mechanism to allow running only specific queries during local testing/debugging
    // if the array is empty, we get query names from the filesystem (listing in the /queries dir)
    // if the array is not empty, we only use those query names
    // example override: ["useragentfamily_devicetype.jsonl"]; // this would only execute this one query
    // let queryNames = ["useragentfamily_devicetype.jsonl"];
    // let queryNames = ["LCP_visibilitystate_devicetype.jsonl"];
    // let queryNames = ["LCPCount_useragentfamily_desktop.jsonl"];
    // let queryNames = ["LCPCount_useragentfamily_mobile.jsonl"];
    // let queryNames = ["LCPCount_useragentfamily_desktop.jsonl", "LCPCount_useragentfamily_mobile.jsonl"];
    // let queryNames = ["devicetype.jsonl", "os_devicetype.jsonl"];
    // let queryNames = ["recent_useragentversion_useragentfamily_devicetype_countrytemplate.jsonl"];
    let queryNames = [];

    if( queryNames.length === 0 ){
        queryNames = await fs.readdir(QUERYDIR);

        if ( queryNames.length === 0 ) {
            throw Error("getQueries: no queries found to execute, aborting...");
        }
    }

    // run the actual query definitions from filesystem
    // expected JSON structs:
    /*
        {
            // required
            datetype: "recent_day" | "recent_month" | "first_days" | "first_tuesdays" | "first_and_third_tuesdays",
            processingtype: "metricGlobal" | "metricPerDevice" | "histogramPerDevice" | "CWVCountPerUseragent",
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
    const queryList = [];

    // we loop over the queries in two phases, so we can generate extra queries based on templates that are then processed as if they came from full files
    for ( let queryName of queryNames ) {
        let queryContentRaw = await fs.readFile( path.join(QUERYDIR, queryName), "utf8" );
        // get rid of newlines in strings from .jsonl
        // note: there are still swaths of extra spaces/tabs in the SQL string now, but bigQuery can deal with those just fine
        queryContentRaw = queryContentRaw.toString().replaceAll("\n","");

        const queryContent = JSON.parse(queryContentRaw);

        if ( !queryContent.filename ) // allow manual overrides in the input file
            queryContent.filename = path.parse(queryName).name; // filename without the extension, used later in the pipeline for storing results

        // TODO: don't use filename maybe, but rather look into the raw string contents if {{COUNTRY}} is present? 
        if ( queryName.includes("countrytemplate") ) {
            // templates allow us to have 1 query file that leads to multiple queries/output files
            // There are the top 15 countries in the dataset on April 1st 2025 (by beaconcount)
            let countryList = ["US", "GB", "JP", "IN", "CA", "FR", "MX", "AU", "IT", "KR", "ES", "NL", "BR", "TR", "PL"];

            for ( country of countryList ) {
                const countryQueryContent = JSON.parse(queryContentRaw); // to ensure we get a copy
                countryQueryContent.sql = countryQueryContent.sql.replaceAll("{{COUNTRY}}", `(COUNTRY = '${country}')`);

                console.log("getQueries: added country template for ", queryContent.filename, country);

                // need to always override
                // myquery_with_metrics_countrytemplate becomes myquery_with_metrics_US
                countryQueryContent.filename = queryContent.filename.replace("countrytemplate", country);

                queryList.push( countryQueryContent );
            }
        }
        else 
            queryList.push( queryContent ); // normal query, ready for further processing below
    }

    // now that we have all the conceptual queries in output, generate the rest of the data for them
    for ( let queryContent of queryList ) {
        // we want to only run the queries for the data we actually need (or we'd use too much BigQuery quota)
        // so, we read the already cached data in data-cache and see which dates we already have
        // we then only keep the dates that we don't have in cache yet, and only query for those
        let cachedData = [];
        const cachePath = "./data-cache/" + queryContent.filename + ".json";

        if( fsSync.existsSync(cachePath) ) {
            const localDataRaw = await fs.readFile( cachePath, "utf8" );
            cachedData = JSON.parse(localDataRaw);
        }

        let dateType = queryContent.datetype;
        if( FORCE_SINGLE_DATE )
            dateType = "recent_day";

        let dateString = "";
        try {
            dateString = dates.getDateQuery(dateType, cachedData); 
        }
        catch(err) {
            console.error("getQueries: unsupported dateType... aborting query ", queryName, err);
            continue;
        }

        if ( dateString === "" ){
            // query is already fully up to date in the local cache; no need to fetch new data
            // we however only skip bigQuery, since we might have changed some of the processing logic, 
            // and we want to keep running that in the pipeline
            queryContent.forceSkipBigQuery = true;
        }

        queryContent.sql = queryContent.sql.replaceAll("{{DATES}}", dateString);

        output.push( queryContent );

        // console.log("getQueries:queryContent:", queryName, queryContent);
        console.log("getQueries: added ", queryContent.filename);
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

async function runJustProcessorDEBUG(){

    const queries = await getQueries();
    queries[0].cachePath = "./data-cache/" + queries[0].filename + ".json";;
    const processedResults = await processResults(queries);
}

function main() {
    
    GLOBAL_DEBUG = false; // should be false
    FORCE_SINGLE_DATE = false; // should be false
    MERGE_CACHE_INSTEAD_OF_OVERRIDE = true; // probably should be true

    runPipeline();
    // runJustProcessorDEBUG(); // bypass the bigquery execution if we already have recent data in /data-cache

    bcd.transformBrowserCompatData("data-output");
}

main(...process.argv.slice(2));