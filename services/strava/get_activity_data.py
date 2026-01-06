import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from utils import (
    generate_s3_path,
    get_access_token,
    get_activities_for_year,
    normalize_activities,
    setup_logging,
)

# loading variables from .env file
load_dotenv()
setup_logging()


def main(years=range(2017, 2027)):
    access_token = get_access_token()

    activities_data = get_activities_for_year(access_token=access_token, years=years)
    activities_dataframe = pd.DataFrame(normalize_activities(activities_data))

    activities_table = pa.Table.from_pandas(activities_dataframe, preserve_index=False)
    activities_save_file_path = generate_s3_path("activities")

    logging.info(f"Writing activity file {activities_save_file_path}")

    pq.write_table(activities_table, activities_save_file_path, flavor="spark")


if __name__ == "__main__":
    main()
