import argparse
import logging
from datetime import date, datetime

import boto3
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from utils import (
    generate_s3_path,
    get_access_token,
    get_streamdata,
    list_s3_files,
    setup_logging,
)

# loading variables from .env file
load_dotenv()
setup_logging()


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch Strava stream data.")
    parser.add_argument("--start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--since_date", type=str, help="Date since when to fetch data (YYYY-MM-DD)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    access_token = get_access_token()

    if args.start_date and args.end_date:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    elif args.since_date:
        start_date = date.fromisoformat(args.since_date)
        end_date = date.today()
    else:
        start_date = None
        end_date = None

    s3 = boto3.client("s3")
    from globals import PERSONAL_BUCKET_NAME

    # This part gets the latest activity file to know which IDs to fetch
    activities_files = list_s3_files(s3, PERSONAL_BUCKET_NAME, "strava/activities/")
    if not activities_files:
        logging.error("No activity files found in S3.")
        return

    file_dates = [
        datetime.strptime(
            x["Key"].split("/")[-1].split("_")[-1].split(".")[0], "%Y-%m-%d"
        )
        for x in activities_files
    ]
    latest_file = activities_files[np.argmax(file_dates)]["Key"]

    activities_dataframe = pd.read_parquet(f"s3://{PERSONAL_BUCKET_NAME}/{latest_file}")
    activities_dataframe["start_date_local"] = pd.to_datetime(
        activities_dataframe["start_date_local"]
    )

    if start_date:
        activities_dataframe = activities_dataframe.query(
            "start_date_local.dt.date >= @start_date"
        )
    if end_date:
        activities_dataframe = activities_dataframe.query(
            "start_date_local.dt.date <= @end_date"
        )

    activity_ids = activities_dataframe["id"].drop_duplicates().to_list()
    if not activity_ids:
        logging.info("No activities found for the specified date range.")
        return

    logging.info(f"Fetching streams for {len(activity_ids)} activities.")
    get_streamdata(s3_client=s3, access_token=access_token, activity_ids=activity_ids)


if __name__ == "__main__":
    main()
