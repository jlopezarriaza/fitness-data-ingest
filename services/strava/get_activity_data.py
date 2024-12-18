import logging
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from globals import PERSONAL_BUCKET_NAME
from utils import get_access_token, get_activities_for_year, normalize_activities
from dotenv import load_dotenv

# loading variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO)

access_token = get_access_token()

activities_data = get_activities_for_year(
    access_token=access_token, years=range(2017, 2025)
)

activities_dataframe = pd.DataFrame(normalize_activities(activities_data))
activities_table = pa.Table.from_pandas(activities_dataframe, preserve_index=False)
activities_key_path = f'strava/activities/all_strava_activities_{datetime.now().strftime("%Y-%m-%d")}.parquet'
activities_save_file_path = f"s3://{PERSONAL_BUCKET_NAME}/" + activities_key_path

logging.info(f"Writing activity file {activities_save_file_path}")

pq.write_table(activities_table, activities_save_file_path, flavor="spark")
