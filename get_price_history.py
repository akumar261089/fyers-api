import os
from fyers_auth import authenticate, get_user, is_token_valid
from fyers_apiv3 import fyersModel
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
# ── Authentication ────────────────────────────────────────────────────────────

# System default browser
token = os.getenv("access_token")  # or call authenticate() to get a new token
client_id = os.getenv("client_id")
if is_token_valid():
    print("Token is valid ✓")
else:
    print("Token is invalid – run authenticate() again.")
    token = authenticate()

fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=token, log_path="")

data = {
    "symbol":"NSE:SBIN-EQ",
    "resolution":"D",
    "date_format":"1",
    "range_from":"2026-03-01",
    "range_to":"2026-03-31",
    "cont_flag":"1",
    "oi_flag":"1"
}

response = fyers.history(data=data)
print(response)   