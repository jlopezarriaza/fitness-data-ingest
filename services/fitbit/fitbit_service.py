import logging
import os
from datetime import datetime

import boto3
from fitbit_utils import (
    fetch_and_save_data,
    get_oauth_token,
)

from globals import REDIRECT_URI, SCOPE, PERSONAL_BUCKET_NAME

# Configuration and constants
logging.basicConfig(level=logging.INFO)

FITBIT_API_KEY = os.getenv("FITBIT_KEY")
FITBIT_API_SECRET = os.getenv("FITBIT_SECRET")


def main():
    s3 = boto3.client("s3")

    start_date_str = "2017-01-10"
    end_date_str = "2024-12-16"

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError as e:
        logging.error(f"Invalid date format: {e}")
        return  # Exit if dates are invalid

    token, fitbit = get_oauth_token(
        FITBIT_API_KEY, FITBIT_API_SECRET, REDIRECT_URI, SCOPE
    )

    if token:  # Check if token retrieval was successful
        fetch_and_save_data(s3, fitbit, start_date, end_date, PERSONAL_BUCKET_NAME)


if __name__ == "__main__":
    main()
