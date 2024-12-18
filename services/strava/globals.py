import pyarrow as pa

AUTHORIZATION_BASE_URL = "https://www.strava.com/oauth/authorize"
TOKEN_URL = "https://www.strava.com/oauth/token"
API_URL = "https://www.strava.com/api/v3"
REDIRECT_URI = "http://localhost:1410"
PERSONAL_BUCKET_NAME = "juan.lopez.arriaza-fitbit-data"
STREAM_SCHEMA = schema = pa.schema(
    [
        ("moving", pa.int64()),
        ("latlng", pa.list_(pa.float64(), 2)),
        ("velocity_smooth", pa.float64()),
        ("grade_smooth", pa.float64()),
        ("distance", pa.float64()),
        ("altitude", pa.float64()),
        ("heartrate", pa.int64()),
        ("time", pa.int64()),
        ("activity_id", pa.int64()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("cadence", pa.float64()),
        ("watts", pa.float64()),
        ("temp", pa.float64()),
    ]
)

BASE_STREAM_TYPES = [
    "time",
    "distance",
    "latlng",
    "altitude",
    "velocity_smooth",
    "heartrate",
    "cadence",
    "watts",
    "temp",
    "moving",
    "grade_smooth",
]
