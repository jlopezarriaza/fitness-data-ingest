# Fitbit Data Ingestion Service

This service is responsible for ingesting data from the Fitbit API into Amazon S3 in Apache Parquet format.

## Usage

To use this service, you will need to:

1. Create a Fitbit developer account and obtain your API key and secret.
2. Set the following environment variables:
    - `FITBIT_API_KEY`
    - `FITBIT_API_SECRET`
    - `PERSONAL_BUCKET_NAME` (the name of the S3 bucket to which you want the data to be ingested)
3. Run the `fitbit_service.py` script.

The script will request authorization from the Fitbit API and, once authorized, will begin ingesting data. The data will be stored in the specified S3 bucket in the following format:

s3://{PERSONAL_BUCKET_NAME}/intraday/{resource}/{resource}_{date}.parquet


where `{resource}` is one of the following:

- `calories`
- `distance`
- `elevation`
- `floors`
- `steps`
- `swimming-strokes`
- `heart`
- `active-zone-minutes`

and `{date}` is the date of the data.

## Additional Notes

- The script is designed to ingest intraday Fitbit data starting from a hardcoded date (`2017-01-10` in the provided script) up to a specified end date.
- The script handles Fitbit API rate limiting by pausing execution when necessary.
- All activity is logged to the console.
- The script uses OAuth 2.0 for authentication with the Fitbit API.  It opens a web browser for the user to authorize the application.
- Data is written to S3 in Parquet format optimized for Apache Spark.
- The script checks for existing files in S3 to avoid redundant data downloads.


## File Descriptions

* **`fitbit_service.py`**: This script handles the OAuth 2.0 authentication flow, retrieves data from the Fitbit API, and saves it to S3.
* **`fitbit_utils.py`**: This script contains utility functions, including functions to construct Fitbit API endpoints and constants for S3 bucket names and resource types.
