import datetime
import json
import logging
import os
import threading
import time
import webbrowser
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Dict, Tuple, Any, Optional, List

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dateutil.relativedelta import relativedelta

from globals import (
    FITBIT_URL,
    INTRADAY_RESOURCES,
    THIRTY_DAY_RESOURCES,
    TOKEN_URL,
    AUTHORIZE_URL,
)
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from requests_oauthlib import OAuth2Session
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"


RETRY_STRATEGY = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)


def file_exists(s3_client: boto3.client, bucket_name: str, file_name: str) -> bool:
    """Checks if a file exists in an S3 bucket.

    Parameters
    ----------
    s3_client : boto3.client
        Boto3 S3 client.
    bucket_name : str
        Name of the S3 bucket.
    file_name : str
        Name of the file to check.

    Returns
    -------
    bool
        True if the file exists, False otherwise.
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except Exception:  # Catch generic exception, log if needed
        return False


def get_oauth_token(
    client_id: str, client_secret: str, redirect_uri: str, scope: List[str]
) -> Tuple[Dict[str, Any], OAuth2Session]:
    """Retrieves an OAuth 2.0 token from Fitbit.

    Parameters
    ----------
    client_id : str
        Fitbit client ID.
    client_secret : str
        Fitbit client secret.
    redirect_uri : str
        Redirect URI.
    scope : List[str]
        List of scopes to request.

    Returns
    -------
    Tuple[Dict[str, Any], OAuth2Session
        A tuple containing the token and the OAuth2Session object.
    """
    fitbit = OAuth2Session(client_id=client_id, redirect_uri=redirect_uri, scope=scope)
    fitbit.mount("http://", HTTPAdapter(max_retries=RETRY_STRATEGY))
    fitbit.mount("https://", HTTPAdapter(max_retries=RETRY_STRATEGY))

    authorization_url, _ = fitbit.authorization_url(AUTHORIZE_URL)
    server = get_callback_url()

    while not hasattr(server, "callback_url"):
        webbrowser.open(authorization_url)
        time.sleep(10)

    authorization_response = f"http://localhost:1410{server.callback_url}"
    token = fitbit.fetch_token(
        TOKEN_URL,
        authorization_response=authorization_response,
        client_secret=client_secret,
    )
    return token, fitbit


def get_callback_url() -> HTTPServer:
    """Starts a local server to handle the OAuth callback.

    Returns
    -------
    HTTPServer
        The HTTP server instance.
    """
    server_address = ("", 1410)
    httpd = HTTPServer(server_address, OAuthCallbackHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """Handles the OAuth callback."""

    def do_GET(self) -> None:  # Added return type hint
        self.server.callback_url = self.path  # type: ignore
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(
            b"<html><body><h1>Authorization received. You can close this tab now.</h1></body></html>"
        )


def fetch_and_save_data(
    s3_client: boto3.client,
    fitbit: OAuth2Session,
    start_date: datetime,
    end_date: datetime,
    bucket_name: str,
) -> None:
    """Fetches Fitbit data and saves it to S3.

    Parameters
    ----------
    s3_client : boto3.client
        Boto3 S3 client.
    fitbit : OAuth2Session
        Fitbit OAuth2 session.
    start_date : datetime
        Start date for data retrieval.
    end_date : datetime
        End date for data retrieval.
    bucket_name : str
        Name of the S3 bucket.

    Returns
    -------
    None
    """
    date_sequence = pd.date_range(start=start_date, end=end_date).to_list()
    date_sequence.reverse()

    for this_date in date_sequence:
        logging.info(f'Getting data for {this_date.strftime("%Y-%m-%d")}')
        for resource in INTRADAY_RESOURCES:
            key_path = f'intraday/{resource}/{resource}_{this_date.strftime("%Y-%m-%d")}.parquet'
            save_file_path = f"s3://{bucket_name}/" + key_path

            if file_exists(s3_client, bucket_name, key_path):
                logging.info(f"File exists for {key_path}")
                continue

            process_resource_data(fitbit, resource, this_date, save_file_path)


def process_resource_data(
    fitbit: OAuth2Session, resource: str, date: datetime, save_file_path: str
) -> None:  # Helper function
    """Processes data for a specific resource and saves it as Parquet.
    Parameters
    ----------
    fitbit : OAuth2Session
         Fitbit OAuth2 session.
    resource : str
       The Fitbit resource to fetch (e.g., 'heart').
    date : datetime
        The date for which to fetch data.
    save_file_path : str
        The S3 path to save the Parquet file.

    Returns
    -------
    None


    """

    resource_url = get_fitbit_resource_endpoint(resource, date)  # type: ignore

    try:
        response = fitbit.get(resource_url)
        response.raise_for_status()
    except HTTPError as exc:
        logging.error(f"HTTPError fetching {resource}: {exc.response.status_code}")
        return

    log_rate_limit(response)

    if (resource != "active-zone-minutes") and (
        len(
            response.json()
            .get(f"activities-{resource}-intraday", {})
            .get("dataset", [])
        )
        == 0
    ):  # Improved error handling here
        return

    intraday_dataset = prepare_data(response, resource, date)
    if (
        intraday_dataset is None
    ):  # Handle the case where prepare_data may return None due to errors.
        return
    intraday_table = pa.Table.from_pandas(intraday_dataset, preserve_index=False)
    logging.info(f"Writing file {save_file_path}")
    pq.write_table(intraday_table, save_file_path, flavor="spark")

    handle_rate_limit(response)


def log_rate_limit(response: requests.Response) -> None:
    """Logs the remaining Fitbit API calls.

    Parameters
    ----------
    response : requests.Response
        The response from the Fitbit API.


    Returns
    -------
    None
    """
    remaining_calls = response.headers.get("fitbit-rate-limit-remaining", "N/A")
    logging.info(f"{remaining_calls} calls remaining")


def handle_rate_limit(response: requests.Response) -> None:
    """Handles Fitbit API rate limiting by pausing if necessary.

    Parameters
    ----------
    response : requests.Response
        The response from the Fitbit API.

    Returns
    -------
    None
    """
    remaining_calls = int(response.headers.get("fitbit-rate-limit-remaining", 0))
    reset_time = int(response.headers.get("fitbit-rate-limit-reset", 0))

    if remaining_calls < 10 and reset_time > 0:  # Check both values before sleeping
        wait_minutes = round(reset_time / 60, 2)
        logging.info(
            f"Rate limit almost reached, waiting {wait_minutes} minutes, the time is: {datetime.now().strftime('%H:%M:%S')} "
        )
        time.sleep(reset_time)


def prepare_data(
    response: requests.Response, resource: str, date: datetime
) -> Optional[pd.DataFrame]:  # Improved data handling
    """Prepares the Fitbit data into a Pandas DataFrame.
    Parameters
    ----------
    response : requests.Response
        The response object from the Fitbit API call.
    resource : str
        The name of the resource being fetched (e.g., 'heart', 'calories').
    date : datetime.datetime
        The date for which the data is being fetched.


    Returns
    -------
    Optional[pd.DataFrame]
       A Pandas DataFrame containing the prepared data, or None if an error occurred.

    """
    try:
        if resource == "active-zone-minutes":
            minutes_data = response.json()[
                f"activities-{resource}-intraday"
            ]  # Get the intraday data

            if minutes_data:
                # Access the 'minutes' list correctly
                minutes_list = minutes_data[0]["minutes"]
                intraday_dataset = pd.DataFrame(
                    {
                        "value": [
                            x["value"]["activeZoneMinutes"] for x in minutes_list
                        ],
                        "time": [
                            pd.to_datetime(x["minute"]).time() for x in minutes_list
                        ],
                    }
                )
            else:
                intraday_dataset = pd.DataFrame()

        else:
            intraday_dataset = pd.DataFrame(
                response.json()
                .get(f"activities-{resource}-intraday", {})
                .get("dataset", [])
            )

        if not intraday_dataset.empty:
            intraday_dataset["datetime"] = pd.to_datetime(
                intraday_dataset["time"].apply(
                    lambda t: f'{date.strftime("%Y-%m-%d")} {t}'
                )
            )
            intraday_dataset["date"] = date.strftime("%Y-%m-%d")
        return intraday_dataset
    except (KeyError, TypeError, ValueError, IndexError) as e:  # Include IndexError
        logging.error(
            f"Error preparing data for {resource}: {e}, Response: {response.text}"
        )
        return None


def get_fitbit_resource_endpoint(resource: str, date_: datetime.date) -> str:
    """Constructs the Fitbit API endpoint for intraday resources.

    Parameters
    ----------
    resource : str
        The name of the Fitbit resource (e.g., "calories", "heart").
    date_ : datetime.date
        The date for which to retrieve data.

    Returns
    -------
    str
        The Fitbit API endpoint URL.

    Raises
    ------
    ValueError
        If the provided resource is not supported.
    """
    if resource not in INTRADAY_RESOURCES:
        raise ValueError(f"Unsupported intraday resource: {resource}")

    date_str = date_.strftime("%Y-%m-%d")
    detail_level = "1sec" if resource == "heart" else "1min"
    return (
        f"{FITBIT_URL}/1/user/-/activities/{resource}/date/"
        f"{date_str}/1d/{detail_level}/time/00:00/23:59.json"
    )


def get_fitbit_thirty_day_resource_endpoint(
    start_date: datetime.date,
) -> Tuple[Dict[str, str], datetime.date]:
    """Constructs Fitbit API endpoints for 30-day resources.

    Parameters
    ----------
    start_date : datetime.date
        The start date for the 30-day period.

    Returns
    -------
    Tuple[Dict[str, str], datetime.date]
        A dictionary where keys are resource names and values are corresponding API endpoints,
        and the end date of the 30-day period.

    """
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date = start_date + datetime.timedelta(days=29)
    end_date_str = end_date.strftime("%Y-%m-%d")

    api_endpoint_dict = {
        "cardioscore": f"{FITBIT_URL}/1/user/-/cardioscore/date/{start_date_str}/{end_date_str}.json",
        "weight": f"{FITBIT_URL}/1/user/-/body/log/weight/date/{start_date_str}/{end_date_str}.json",
        "temp": f"{FITBIT_URL}/1/user/-/temp/skin/date/{start_date_str}/{end_date_str}.json",
        "sleep": f"{FITBIT_URL}/1.2/user/-/sleep/date/{start_date_str}/{end_date_str}.json",
        "br": f"{FITBIT_URL}/1/user/-/br/date/{start_date_str}/{end_date_str}/all.json",
        "hrv": f"{FITBIT_URL}/1/user/-/hrv/date/{start_date_str}/{end_date_str}/all.json",
        "spo2": f"{FITBIT_URL}/1/user/-/spo2/date/{start_date_str}/{end_date_str}/all.json",
    }
    return api_endpoint_dict, end_date


FITBIT_ENDPOINTS: Dict[str, Callable[[datetime.date], str]] = {
    resource: lambda date_: get_fitbit_resource_endpoint(resource, date_)
    for resource in INTRADAY_RESOURCES
}

THIRTY_DAY_ENDPOINTS: Dict[str, Callable[[datetime.date], str]] = (
    {  # Fixed: Use the correct function
        resource: lambda start_date: get_fitbit_thirty_day_resource_endpoint(
            start_date
        )[0][
            resource
        ]  # Access the correct endpoint from the returned dictionary
        for resource in THIRTY_DAY_RESOURCES
    }
)
