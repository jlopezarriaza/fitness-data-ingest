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


if __name__ == "__main__":
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

    activities_dataframe = activities_dataframe = pd.read_parquet(
        f"s3://{PERSONAL_BUCKET_NAME}/{latest_file}"
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
    #stream_table = pa.Table.from_pandas(stream_df, preserve_index=False)
    #pq.write_table(stream_table, streams_save_file_path, flavor="spark")

    l#ogging.info(f"Writing streams file {streams_save_file_path}")
