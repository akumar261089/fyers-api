"""
example_usage.py
----------------
Shows how to import and use fyers_auth from any other script.
"""
import os
from fyers_auth import authenticate, get_user, is_token_valid
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
# ── Authentication ────────────────────────────────────────────────────────────

# System default browser
token = os.getenv("access_token")  # or call authenticate() to get a new token
if is_token_valid():
    print("Token is valid ✓")
else:
    print("Token is invalid – run authenticate() again.")
    token = authenticate()


# Force Chrome  (falls back to system default if Chrome is not installed)
#token = authenticate(browser="chrome")

# Force Firefox (falls back to system default if Firefox is not installed)
# token = authenticate(browser="firefox")

# Headless – skip the browser, reuse the auth_code already saved in .env
# token = authenticate(headless=True)

# Headless + specific browser (ignored in headless mode, included for parity)
# token = authenticate(headless=True, browser="chrome")


# ── CLI equivalent ────────────────────────────────────────────────────────────
#   python -m fyers_auth.auth
#   python -m fyers_auth.auth --browser chrome
#   python -m fyers_auth.auth --browser firefox
#   python -m fyers_auth.auth --headless


# ── Validate token ────────────────────────────────────────────────────────────



# ── Fetch user profile ────────────────────────────────────────────────────────

profile = get_user()                       # reads access_token from .env
# profile = get_user(access_token=token)  # or pass explicitly
print(profile)
print("Name :", profile["data"]["name"])
print("Email:", profile["data"]["email_id"])
print("PAN  :", profile["data"]["PAN"])
