"""
fyers_auth/user.py
------------------
User / profile helpers for Fyers API v3.

Usage:
    from fyers_auth.user import get_user

    profile = get_user()                     # uses access_token from .env
    profile = get_user(access_token="...")   # pass token explicitly
"""

import os

from dotenv import load_dotenv
from fyers_apiv3 import fyersModel


def _build_fyers(client_id: str, access_token: str) -> fyersModel.FyersModel:
    return fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,
        log_path="",
    )


def get_user(access_token: str | None = None) -> dict:
    """
    Fetch the Fyers user profile and validate the access token.

    Parameters
    ----------
    access_token : str, optional
        Fyers access token.  When omitted the value is read from the
        ``access_token`` environment variable (or .env file).

    Returns
    -------
    dict
        Raw profile response from Fyers.
        ``response["s"] == "ok"`` means the token is valid.

    Raises
    ------
    EnvironmentError
        If required credentials are missing.
    PermissionError
        If the access token is rejected by Fyers.

    Examples
    --------
    >>> from fyers_auth.user import get_user
    >>> profile = get_user()
    >>> print(profile["data"]["name"])
    """
    load_dotenv(override=True)

    client_id = os.getenv("client_id")
    if not client_id:
        raise EnvironmentError("'client_id' not found in environment / .env")

    token = access_token or os.getenv("access_token")
    if not token:
        raise EnvironmentError(
            "'access_token' not found. Pass it explicitly or set it in .env. "
            "Run authenticate() first to generate one."
        )

    fyers = _build_fyers(client_id, token)
    response = fyers.get_profile()

    if response.get("s") != "ok":
        raise PermissionError(
            f"Fyers rejected the access token. Response: {response}"
        )

    return response


def is_token_valid(access_token: str | None = None) -> bool:
    """
    Quick boolean check – returns *True* if the token is accepted by Fyers.

    Parameters
    ----------
    access_token : str, optional
        Falls back to ``access_token`` env var when omitted.
    """
    try:
        get_user(access_token=access_token)
        return True
    except (PermissionError, EnvironmentError):
        return False


# ── CLI convenience ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    profile = get_user()
    print(json.dumps(profile, indent=2))
