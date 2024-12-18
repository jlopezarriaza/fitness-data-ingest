PERSONAL_BUCKET_NAME = "juan.lopez.arriaza-fitbit-data"

FITBIT_URL = "https://api.fitbit.com"

INTRADAY_RESOURCES = [
    "calories",
    "distance",
    "elevation",
    "floors",
    "steps",
    "swimming-strokes",
    "heart",
    "active-zone-minutes",
]
THIRTY_DAY_RESOURCES = ["br", "hrv", "spo2", "temp", "cardioscore", "weight", "sleep"]


TOKEN_URL = "https://api.fitbit.com/oauth2/token"
AUTHORIZE_URL = "https://www.fitbit.com/oauth2/authorize"
REDIRECT_URI = "http://localhost:1410"
SCOPE = [
    "activity",
    "heartrate",
    "location",
    "sleep",
    "respiratory_rate",
    "weight",
    "temperature",
    "cardio_fitness",
    "respiratory_rate",
]
