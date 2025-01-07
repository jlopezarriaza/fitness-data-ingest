import logging
import os
import threading
import time
import webbrowser
from datetime import datetime
from typing import Dict, List, Optional, TypedDict, Union
from urllib.parse import urlencode

import boto3
import folium
import pandas as pd
import polyline
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError
from flask import Flask, request
from folium import plugins

from globals import (
    API_URL,
    AUTHORIZATION_BASE_URL,
    BASE_STREAM_TYPES,
    PERSONAL_BUCKET_NAME,
    STREAM_SCHEMA,
    TOKEN_URL,
)

logging.basicConfig(level=logging.INFO)


def get_activities_for_year(access_token: str, years: List[int]) -> List[dict]:
    """
    Retrieve all Strava activities for specified years.

    Parameters
    ----------
    access_token : str
        Strava API access token for authentication
    years : List[int]
        List of years for which to retrieve activities

    Returns
    -------
    List[dict]
        A list of dictionaries containing activity data from Strava API

    Notes
    -----
    This function paginates through Strava API to collect all activities
    within the specified year range.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    start_date = datetime(min(years), 1, 1)
    end_date = datetime(max(years) + 1, 1, 1)

    after = int(start_date.timestamp())
    before = int(end_date.timestamp())

    all_activities = []
    page = 1
    while True:
        print(f"Getting activities for page {page}")
        activities_url = f"{API_URL}/athlete/activities"
        params = {"per_page": 200, "page": page, "after": after, "before": before}

        activities_response = requests.get(
            activities_url, headers=headers, params=params
        )
        activities_data = activities_response.json()
        print(f"Got {len(activities_data)} activities")
        if not activities_data:
            break

        all_activities.extend(activities_data)
        page += 1

    return all_activities


class AthleteMetaDict(TypedDict):
    id: int
    resource_state: int


class MapDict(TypedDict):
    id: str
    summary_polyline: str
    resource_state: int


class SummaryActivity(TypedDict):
    resource_state: int
    athlete: AthleteMetaDict
    name: str
    distance: float
    moving_time: int
    elapsed_time: int
    total_elevation_gain: float
    type: str
    sport_type: str
    workout_type: Optional[int]
    id: int
    start_date: str
    start_date_local: str
    timezone: str
    utc_offset: float
    location_city: Optional[str]
    location_state: Optional[str]
    location_country: Optional[str]
    achievement_count: int
    kudos_count: int
    comment_count: int
    athlete_count: int
    photo_count: int
    map: MapDict
    trainer: bool
    commute: bool
    manual: bool
    private: bool
    visibility: str
    flagged: bool
    gear_id: Optional[str]
    start_latlng: Optional[List[float]]
    end_latlng: Optional[List[float]]
    average_speed: float
    max_speed: float
    has_heartrate: bool
    average_heartrate: Optional[float]
    max_heartrate: Optional[float]
    heartrate_opt_out: bool
    display_hide_heartrate_option: bool
    elev_high: Optional[float]
    elev_low: Optional[float]
    upload_id: Optional[int]
    upload_id_str: Optional[str]
    external_id: Optional[str]
    from_accepted_tag: bool
    pr_count: int
    total_photo_count: int
    has_kudoed: bool
    suffer_score: Optional[float]


def normalize_activity(raw_activity: dict) -> SummaryActivity:
    """
    Normalize a single activity dictionary to match Strava's SummaryActivity schema.

    Args:
        raw_activity (dict): Raw activity data from the API

    Returns:
        SummaryActivity: Normalized activity data matching Strava's schema
    """
    # Handle heart rate data
    has_heartrate = raw_activity.get("has_heartrate", False)
    average_heartrate = raw_activity.get("average_heartrate") if has_heartrate else None
    max_heartrate = raw_activity.get("max_heartrate") if has_heartrate else None

    activity: SummaryActivity = {
        # Core activity data
        "resource_state": raw_activity["resource_state"],
        "athlete": {
            "id": raw_activity["athlete"]["id"],
            "resource_state": raw_activity["athlete"]["resource_state"],
        },
        "name": raw_activity["name"],
        "distance": raw_activity["distance"],
        "moving_time": raw_activity["moving_time"],
        "elapsed_time": raw_activity["elapsed_time"],
        "total_elevation_gain": raw_activity["total_elevation_gain"],
        "type": raw_activity["type"],
        "sport_type": raw_activity["sport_type"],
        "workout_type": raw_activity.get("workout_type"),
        "id": raw_activity["id"],
        # Timestamps and location
        "start_date": raw_activity["start_date"],
        "start_date_local": raw_activity["start_date_local"],
        "timezone": raw_activity["timezone"],
        "utc_offset": raw_activity["utc_offset"],
        "location_city": raw_activity.get("location_city"),
        "location_state": raw_activity.get("location_state"),
        "location_country": raw_activity.get("location_country"),
        # Achievement and social data
        "achievement_count": raw_activity["achievement_count"],
        "kudos_count": raw_activity["kudos_count"],
        "comment_count": raw_activity["comment_count"],
        "athlete_count": raw_activity["athlete_count"],
        "photo_count": raw_activity["photo_count"],
        # Map data
        "map": {
            "id": raw_activity["map"]["id"],
            "summary_polyline": raw_activity["map"]["summary_polyline"],
            "resource_state": raw_activity["map"]["resource_state"],
        },
        # Activity flags
        "trainer": raw_activity.get("trainer", False),
        "commute": raw_activity.get("commute", False),
        "manual": raw_activity.get("manual", False),
        "private": raw_activity.get("private", False),
        "visibility": raw_activity.get("visibility", "everyone"),
        "flagged": raw_activity.get("flagged", False),
        # Coordinates
        "start_latlng": raw_activity.get("start_latlng"),
        "end_latlng": raw_activity.get("end_latlng"),
        # Performance metrics
        "average_speed": raw_activity["average_speed"],
        "max_speed": raw_activity["max_speed"],
        "has_heartrate": has_heartrate,
        "average_heartrate": average_heartrate,
        "max_heartrate": max_heartrate,
        "heartrate_opt_out": raw_activity.get("heartrate_opt_out", False),
        "display_hide_heartrate_option": raw_activity.get(
            "display_hide_heartrate_option", True
        ),
        "elev_high": raw_activity.get("elev_high"),
        "elev_low": raw_activity.get("elev_low"),
        # Upload information
        "upload_id": raw_activity.get("upload_id"),
        "upload_id_str": raw_activity.get("upload_id_str"),
        "external_id": raw_activity.get("external_id"),
        # Additional metadata
        "from_accepted_tag": raw_activity.get("from_accepted_tag", False),
        "pr_count": raw_activity.get("pr_count", 0),
        "total_photo_count": raw_activity.get("total_photo_count", 0),
        "has_kudoed": raw_activity.get("has_kudoed", False),
        "suffer_score": raw_activity.get("suffer_score"),
        # Required fields that might be missing in some responses
        "gear_id": raw_activity.get("gear_id"),
    }

    return activity


def normalize_activities(activities: List[dict]) -> List[SummaryActivity]:
    """
    Normalize a list of activities.

    Args:
        activities (List[dict]): List of raw activity dictionaries

    Returns:
        List[SummaryActivity]: List of normalized activities
    """
    return [normalize_activity(activity) for activity in activities]


def list_s3_files(
    s3_client: boto3.client, bucket_name: str, prefix: str = ""
) -> list[dict]:
    """
    List all files in an S3 bucket.

    Parameters
    ----------
    s3_client : boto3.client
        The S3 client used to interact with the S3 service.
    bucket_name : str
        Name of the S3 bucket.
    prefix : str, optional
        Optional prefix to filter results (folder path).

    Returns
    -------
    list[dict]
        List of dictionaries containing file information.
    """
    try:

        # List all objects in the bucket
        files = []
        paginator = s3_client.get_paginator("list_objects_v2")

        # Handle pagination
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(
                        {
                            "Key": obj["Key"],
                            "Size": obj["Size"],
                            "LastModified": obj["LastModified"],
                            "StorageClass": obj["StorageClass"],
                        }
                    )

        return files

    except ClientError as e:
        print(f"Error: {e}")
        return None


def get_single_streamdata(
    access_token: str,
    activity_id: int,
    keys: List[str] = BASE_STREAM_TYPES,
) -> Optional[pd.DataFrame]:
    """
    Retrieve stream data for a single Strava activity.

    Parameters
    ----------
    access_token : str
        Strava API access token for authentication
    activity_id : int
        Unique identifier for the Strava activity
    keys : List[str], optional
        List of stream data keys to retrieve, by default includes
        time, distance, location, and performance metrics

    Returns
    -------
    Optional[pd.DataFrame]
        DataFrame containing stream data for the activity,
        or None if data cannot be retrieved

    Notes
    -----
    This function checks for existing Parquet files before
    making an API request. It handles rate limiting and
    saves retrieved data to S3.
    """
    stream_key_path = f"strava/streams/strava_stream_{activity_id}.parquet"
    stream_save_file_path = f"s3://{PERSONAL_BUCKET_NAME}/" + stream_key_path
    # if file_exists(
    #     PERSONAL_BUCKET_NAME,
    #     stream_key_path,
    # ):
    #     logging.info(f"File exits for {stream_key_path}")
    #     return pd.read_parquet(stream_save_file_path)
    headers = {"Authorization": f"Bearer {access_token}"}
    streams_url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams?keys={','.join(keys)}"

    stream_data_response = requests.get(streams_url, headers=headers)
    limit_15_min, limit_daily = [
        int(val)
        for val in stream_data_response.headers["x-readratelimit-limit"].split(",")
    ]
    usage_15_min, usage_dailyl = [
        int(val)
        for val in stream_data_response.headers["x-readratelimit-usage"].split(",")
    ]
    if limit_15_min - usage_15_min < 5:
        logging.info("15 minute limit usage limit almost reached ... waiting")
        time.sleep(15 * 60)

    if stream_data_response.status_code == 404:
        logging.info(stream_data_response.reason)
        return None
    stream_data = stream_data_response.json()
    stream_df = (
        pd.DataFrame()
        .assign(**{x["type"]: x["data"] for x in stream_data})
        .assign(activity_id=activity_id)
    )
    if "latlng" in stream_df.columns:
        stream_df[["latitude", "longitude"]] = pd.DataFrame(
            stream_df["latlng"].tolist(), index=stream_df.index
        )
    else:
        stream_df["latlng"] = [[None, None] for x in stream_df.index]
    stream_df = stream_df.assign(
        **{
            one_key: None
            for one_key in keys + ["latitude", "longitude"]
            if one_key not in stream_df.columns
        }
    )
    logging.info(f"Writing data for activity {activity_id}")
    stream_table = pa.Table.from_pandas(
        stream_df, preserve_index=False, schema=STREAM_SCHEMA
    )
    pq.write_table(stream_table, stream_save_file_path, flavor="spark")
    return stream_df


def get_streamdata(
    s3_client: boto3.client, access_token: str, activity_ids: List[int]
) -> Optional[pd.DataFrame]:
    """
    Retrieve stream data for multiple Strava activities.

    Parameters
    ----------
    access_token : str
        Strava API access token for authentication
    activity_ids : List[int]
        List of unique activity identifiers

    Returns
    -------
    Optional[pd.DataFrame]
        Concatenated DataFrame of stream data for all activities,
        or None if no data could be retrieved

    Notes
    -----
    This function iterates through activity IDs and calls
    get_single_streamdata() for each activity.
    """

    existing_stream_files = [
        x["Key"]
        for x in list_s3_files(s3_client, PERSONAL_BUCKET_NAME, "strava/streams/")
    ]

    pd_list = []
    for activity_id in activity_ids:
        logging.info(f"processing activity_id {activity_id}, reading from S3")
        matching_key = [x for x in existing_stream_files if str(activity_id) in x]
        if any(matching_key):
            logging.info(f"File exists for activity_id {activity_id}")
            stream_data = pd.read_parquet(
                f"s3://{PERSONAL_BUCKET_NAME}" + "/" + matching_key[0]
            )
            continue
        stream_data = get_single_streamdata(
            access_token=access_token, activity_id=activity_id
        )
        if stream_data is not None:
            pd_list.append(stream_data)

    return pd.concat(pd_list).reset_index(drop=True) if pd_list else None


def get_access_token() -> str:
    """
    Retrieve a Strava API access token through an automated OAuth 2.0 authorization flow.

    This function:
    1. Sets up a local Flask server to receive the OAuth callback
    2. Opens the Strava authorization URL in the default web browser
    3. Waits for the authorization code to be received
    4. Exchanges the authorization code for an access token

    Returns
    -------
    str
        The Strava API access token for authenticated requests
    """
    # Create a Flask app for handling the OAuth callback
    app = Flask(__name__)
    authorization_code = [None]  # Using a list to make it mutable in nested function

    @app.route("/callback")
    def callback():
        authorization_code[0] = request.args.get("code")
        return "Authorization successful! You can now close this window."

    # Start the Flask app in a separate thread
    def run_flask():
        app.run(port=8000)

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Prepare authorization parameters
    params = {
        "client_id": os.environ["STRAVA_CLIENT_ID"],
        "redirect_uri": "http://localhost:8000/callback",
        "response_type": "code",
        "scope": "activity:read_all",
    }

    # Generate and open the authorization URL
    authorization_url = f"{AUTHORIZATION_BASE_URL}?{urlencode(params)}"
    webbrowser.open(authorization_url)

    # Wait for the authorization code to be received
    while authorization_code[0] is None:
        import time

        time.sleep(1)

    # Exchange authorization code for access token
    token_params = {
        "client_id": os.environ["STRAVA_CLIENT_ID"],
        "client_secret": os.environ["STRAVA_CLIENT_SECRET"],
        "code": authorization_code[0],
        "grant_type": "authorization_code",
    }

    token_response = requests.post(TOKEN_URL, data=token_params)
    access_token = token_response.json()["access_token"]

    return access_token
