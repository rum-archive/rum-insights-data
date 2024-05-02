const bcd = require('@mdn/browser-compat-data');
const fs = require('fs/promises');

async function transformBrowserCompatData( output_dir_name ){
    // console.log("Calculating bcd!", output_dir_name );

    // For the Baseline calculations, we need to map from RUM Archive user agents to web-features user agents with the correct version numbers.
    // For some browsers, this is trivial (e.g., Chrome Mobile maps to chrome_android, and the version numbers are the same.
    // HOWEVER, for some others browsers, it's more complex
    // An example is Opera, which uses Blink/Chromium under the hood, but uses its own version numbers... 
    // Since we want to be able to group everything under the web-features user agents 
    //      (e.g., both RUM Archive's Chrome Mobile and Opera Mobile should be counted as chrome_android for calculations there)
    // we need to make those version number mappings explicit.

    // we want to be able to do something like this in the code that uses this
    // let versionMappings = allMappings["mobile"];
    // let RUMArchiveUA = "Opera Mobile";
    // let mapping = versionMappings[RUMArchiveUA];
    // let webFeaturesUA = mapping.maps_to_ua // returns chrome_android
    // let engineVersion = mapping.versions["109"] // returns 123 (e.g., opera 109 in RUM Archive maps to Blink 123, which we need for web-features)

    // For this, we can use the BCD project which tracks these things as well, at https://github.com/mdn/browser-compat-data/tree/main/browsers

    let RUMArchiveToWebFeaturesMappings = {
        "desktop": {
            "Opera": {
                maps_to_ua: "chrome",
                versions: {}
            },
        },

        "mobile": {
            "Samsung Internet": {
                maps_to_ua: "chrome_android",
                versions: {}
            },
            "Opera Mobile": {
                maps_to_ua: "chrome_android",
                versions: {}
            },
        }
    };

    let RUMArchiveToBCDMappings = {
        "Opera": "opera",
        "Samsung Internet": "samsunginternet_android",
        "Opera Mobile": "opera_android"
    };

    for ( let clientType of Object.keys(RUMArchiveToWebFeaturesMappings) ) {
        let uasForClient = RUMArchiveToWebFeaturesMappings[ clientType ];

        for ( let RUMArchiveUA of Object.keys(uasForClient) ) {

            const BCDUA = RUMArchiveToBCDMappings[ RUMArchiveUA ];

            const BCDinfo = bcd.browsers[ BCDUA ];

            for ( let RUMArchiveVersionNr of Object.keys(BCDinfo.releases) ) {
                let BCDversionNr = BCDinfo.releases[ RUMArchiveVersionNr ][ "engine_version" ];
                uasForClient[ RUMArchiveUA ].versions[ RUMArchiveVersionNr ] = BCDversionNr;
            }
        }
    }

    const outputPath = "./" + output_dir_name + "/baseline-browser-mappings.json";
    await fs.writeFile( outputPath, JSON.stringify(RUMArchiveToWebFeaturesMappings), "utf8" ); // overwrites by default
}

module.exports = {
    transformBrowserCompatData
}
