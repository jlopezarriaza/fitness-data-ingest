# Strava Data Processing

This directory contains scripts and utilities for interacting with the Strava API, retrieving activity data, and processing stream data. The main functionalities include fetching activities, merging activity data with stream data, and saving the results in Parquet format on AWS S3 for efficient storage and analysis.

## Directory Structure

- `get_activity_data.py`: Script to fetch and save activity data from Strava.
- `get_stream_data.py`: Script to fetch and save stream data associated with activities.
- `utils.py`: Helper functions for various tasks, including data normalization, S3 interactions, OAuth 2.0 authorization, and centralized logging/path generation.
- `globals.py`: Project-wide constants and schemas.

## Workflow

The scripts are designed to be run in sequence:

1. **`get_activity_data.py`**: Fetches summary activity data for a specified year range (default: 2017-2026). The data is normalized and saved as a Parquet file in the specified S3 bucket.
2. **`get_stream_data.py`**: Reads the most recent activity data file from S3 and fetches detailed stream data (e.g., heart rate, GPS coordinates, power) for each activity. It saves the stream data as individual Parquet files in S3 and concatenates them for analysis.

## Requirements

- **Python Packages:**
  ```bash
  pip install -r requirements.txt
  ```
- **Environment Variables:**
  - `STRAVA_CLIENT_ID`: Your Strava API client ID.
  - `STRAVA_CLIENT_SECRET`: Your Strava API client secret.
- **AWS Credentials:** Properly configured AWS credentials are required (e.g., via `~/.aws/credentials`).

## Usage

1. **Configure Environment:** Set up `.env` with your Strava credentials.
2. **Fetch Activity Data:**
   ```bash
   python get_activity_data.py
   ```

3. **Fetch Stream Data:**
   `get_stream_data.py` supports filtering activities by date:
   ```bash
   # Fetch all streams for activities in the latest activity file
   python get_stream_data.py

   # Fetch streams for activities since a specific date
   python get_stream_data.py --since_date 2024-01-01

   # Fetch streams for a specific date range
   python get_stream_data.py --start_date 2024-01-01 --end_date 2024-12-31
   ```

## Utilities and Refactoring

The codebase has been refactored to use centralized utilities in `utils.py`:
- `setup_logging()`: Configures consistent logging across all scripts.
- `generate_s3_path()`: Centralizes S3 path generation for activities and streams.
- `normalize_activity()`: Ensures consistent data schema for activities.

## S3 Storage

Data is stored in the S3 bucket specified by `PERSONAL_BUCKET_NAME` in `globals.py`.
- `strava/activities/`: Summary activity data files.
- `strava/streams/`: Detailed stream data files (individual and daily snapshots).

## Authentication

The `get_access_token()` function in `utils.py` handles the OAuth 2.0 flow, automatically opening a browser for authorization.

## Future Improvements

- Implementation of data validation and schema enforcement.
- Automated tests for data normalization and S3 interactions.
- Enhanced error recovery for network-related issues.
