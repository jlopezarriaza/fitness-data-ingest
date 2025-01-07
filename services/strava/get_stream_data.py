import logging
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from globals import PERSONAL_BUCKET_NAME
from utils import get_access_token, get_streamdata, list_s3_files

# loading variables from .env file
load_dotenv()
logging.basicConfig(level=logging.INFO)

access_token = get_access_token()


import argparse
from datetime import date, timedelta

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Strava stream data.")
    parser.add_argument("--start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--since_date", type=str, help="Date since when to fetch data (YYYY-MM-DD)"
    )

    args = parser.parse_args()

    start_date_str = args.start_date
    end_date_str = args.end_date
    since_date_str = args.since_date

    if start_date_str and end_date_str:
        start_date = date.fromisoformat(start_date_str)
        end_date = date.fromisoformat(end_date_str)
    elif since_date_str:
        since_date = date.fromisoformat(since_date_str)
        start_date = since_date
        end_date = date.today()
    else:
        start_date = None
        end_date = None

    s3 = boto3.client("s3")

    streams_key_path = f'strava/streams/all_strava_streams_{datetime.now().strftime("%Y-%m-%d")}.parquet'
    streams_save_file_path = f"s3://{PERSONAL_BUCKET_NAME}/" + streams_key_path

    activities_files = list_s3_files(s3, PERSONAL_BUCKET_NAME, "strava/activities/")

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
    stream_df = get_streamdata(
        s3_client=s3, access_token=access_token, activity_ids=activity_ids
    )

    # joined_stream_df = pd.merge(
    #     stream_df,
    #     activities_dataframe[["sport_type", "id", "start_date_local", "name"]],
    #     left_on="activity_id",
    #     right_on="id",
    # )
    # stream_table = pa.Table.from_pandas(stream_df, preserve_index=False)
    # pq.write_table(stream_table, streams_save_file_path, flavor="spark")

    # logging.info(f"Writing streams file {streams_save_file_path}")
