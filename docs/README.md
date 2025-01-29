# RUM Insights data pipeline

The raw RUM Archive data is published in Google BigQuery.
To generate the RUM Insights visualizations, we don't want to constantly query that live for every user though.

This repository contains queries and scripts to create a "cached" version of the needed data that is stored separately (currently just on github) and can be accessed by the visualizations as a ready-to-use JSON file.

This process contains several steps which are detailed below:

1. Figure out which queries to run.
2. Run the queries through BigQuery to get the full raw results. Store these in a cache.
3. Process the raw results into a more easy-to-use output format for the RUM Insights visualizations.

## 1. Determine queries

We have several queries to run, typically 1 per visualization (though some can share query outputs).
Some queries need the full temporal range since the RUM Archive start, while others only need the latest data (last available month).

The queries are stored as JSONL in `/queries` and contain a combination of the raw SQL commands (together with some placeholder text of the form `{{TO_BE_REPLACED}}` for certain variables (typically date ranges)) and some query metadata (e.g., serializable (file)name, type of query, description, enabled/disabled, etc.).

```text
Note: the we use a non-standard JSON variant `.jsonl` that allows newlines in strings to make the SQL queries more readable. We work around this by removing newlines from the read files before passing them to the JSON parser.
```

TODO: discuss looking at cached previous results and only run queries for newer data

## 2. Run BigQuery

TODO: note that this repo does not contain full instructions on how to setup the bigquery API credentials etc. Currently this is being done periodically and manually on a local PC.

## 3. Transform results

TODO: discuss that one goal is to get output format similar to HTTPArchive so we can more easily compare data across the datasets in the same visualizations
