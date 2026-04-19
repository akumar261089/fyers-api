"""
fyers_auth/auth.py
------------------
Authentication module for Fyers API v3.

Usage:
    from fyers_auth.auth import authenticate

    # default system browser
    access_token = authenticate()

    # specific browser
    access_token = authenticate(browser="chrome")
    access_token = authenticate(browser="firefox")

    # skip browser step, use saved auth_code
    access_token = authenticate(headless=True)
"""

import os
import platform
import shutil
import subprocess
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv, set_key
from fyers_apiv3 import fyersModel

# ── constants ─────────────────────────────────────────────────────────────────

_ENV_FILE         = ".env"
_AUTH_CODE_KEY    = "auth_code"
_ACCESS_TOKEN_KEY = "access_token"
_REDIRECT_PORT    = 5000

# Candidate executable names per browser per OS.
# Checked left-to-right; first one found on PATH (or at known location) wins.
_BROWSER_CANDIDATES: dict[str, dict[str, list[str]]] = {
    "chrome": {
        "linux": [
            "google-chrome",
            "google-chrome-stable",
            "chromium",
            "chromium-browser",
        ],
        "windows": [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        ],
    },
    "firefox": {
        "linux": [
            "firefox",
            "firefox-esr",
        ],
        "windows": [
            r"C:\Program Files\Mozilla Firefox\firefox.exe",
            r"C:\Program Files (x86)\Mozilla Firefox\firefox.exe",
        ],
    },
}


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_env() -> dict:
    load_dotenv(override=True)
    return {
        "client_id":    os.getenv("client_id"),
        "secret_key":   os.getenv("secret_key"),
        "redirect_uri": os.getenv("redirect_uri", f"http://localhost:{_REDIRECT_PORT}/"),
        "browser":      os.getenv("browser"),
    }


def _save_to_env(key: str, value: str) -> None:
    """Persist a key-value pair to the .env file."""
    set_key(_ENV_FILE, key, value)
    os.environ[key] = value  # also update the running process


def _detect_os() -> str:
    """Return 'windows' or 'linux' (macOS falls through to linux/PATH lookup)."""
    return "windows" if platform.system().lower() == "windows" else "linux"


def _find_browser_executable(browser: str) -> str | None:
    """
    Locate the executable for *browser* on the current OS.
    Returns the full resolved path, or None if not found anywhere.
    """
    os_key     = _detect_os()
    candidates = _BROWSER_CANDIDATES.get(browser, {}).get(os_key, [])

    for candidate in candidates:
        # Absolute / Windows-style path – check file existence directly
        if os.sep in candidate or (os_key == "windows" and ":" in candidate):
            expanded = os.path.expandvars(candidate)
            if os.path.isfile(expanded):
                return expanded
        else:
            # Short name – look it up on PATH
            found = shutil.which(candidate)
            if found:
                return found

    return None


def _open_url_with_browser(url: str, browser: str | None) -> None:
    """
    Open *url* in the requested browser.

    Parameters
    ----------
    url     : The URL to open.
    browser : 'chrome', 'firefox', or None (use system default).
    """
    # ── system default ────────────────────────────────────────────────────────
    if browser is None:
        webbrowser.open(url)
        return

    browser_key = browser.strip().lower()

    if browser_key not in _BROWSER_CANDIDATES:
        supported = ", ".join(_BROWSER_CANDIDATES.keys())
        raise ValueError(
            f"Unsupported browser '{browser}'. "
            f"Supported values: {supported}  (or None for system default)."
        )

    exe = _find_browser_executable(browser_key)

    # ── fallback if browser not installed ────────────────────────────────────
    if exe is None:
        print(
            f"[auth] WARNING: '{browser}' was not found on this system. "
            "Falling back to system default browser."
        )
        webbrowser.open(url)
        return

    print(f"[auth] Launching {browser_key}  →  {exe}")

    # subprocess.Popen is non-blocking – Python continues to run the callback
    # server while the browser window opens.
    extra = {}
    if _detect_os() == "windows":
        # Prevent a console flash on Windows
        extra["creationflags"] = subprocess.CREATE_NO_WINDOW

    subprocess.Popen(
        [exe, url],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        **extra,
    )


# ── local redirect-server ─────────────────────────────────────────────────────

class _CallbackHandler(BaseHTTPRequestHandler):
    """Tiny HTTP handler that captures the OAuth redirect from Fyers."""

    auth_code: str | None = None

    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        code   = params.get("auth_code", [None])[0]
        status = params.get("s", [""])[0]

        if status == "ok" and code:
            _CallbackHandler.auth_code = code
            body = b"<h2>Authentication successful! You may close this tab.</h2>"
            self.send_response(200)
        else:
            body = b"<h2>Authentication failed. Please try again.</h2>"
            self.send_response(400)

        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # suppress server logs
        pass


def _run_callback_server(port: int = _REDIRECT_PORT) -> str:
    """
    Start a one-shot HTTP server on *port*, wait for the Fyers redirect,
    return the captured auth_code, then shut down.
    """
    _CallbackHandler.auth_code = None
    server = HTTPServer(("localhost", port), _CallbackHandler)

    def _serve():
        while _CallbackHandler.auth_code is None:
            server.handle_request()

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    thread.join(timeout=120)   # wait up to 2 minutes
    server.server_close()

    if _CallbackHandler.auth_code is None:
        raise TimeoutError("Timed out waiting for Fyers OAuth redirect.")

    return _CallbackHandler.auth_code


# ── step 1 – get auth code ────────────────────────────────────────────────────

def get_auth_code(env: dict, browser: str | None = None) -> str:
    """
    Generate the Fyers login URL, open it in *browser*, spin up a local
    server to capture the redirect, and return the auth_code.

    Parameters
    ----------
    env     : Credentials dict from _load_env().
    browser : 'chrome', 'firefox', or None (system default).
    """
    session = fyersModel.SessionModel(
        client_id=env["client_id"],
        secret_key=env["secret_key"],
        redirect_uri=env["redirect_uri"],
        response_type="code",
    )

    login_url = session.generate_authcode()
    label = browser or "system default"
    print(f"\n[auth] Opening {label} for Fyers login…\n  URL: {login_url}\n")

    _open_url_with_browser(login_url, browser)

    print(f"[auth] Waiting for redirect on port {_REDIRECT_PORT}…")
    auth_code = _run_callback_server()
    print("[auth] Auth code received.")

    _save_to_env(_AUTH_CODE_KEY, auth_code)
    return auth_code


# ── step 2 – exchange auth code for access token ──────────────────────────────

def get_access_token(env: dict, auth_code: str) -> str:
    """Exchange *auth_code* for a Fyers access token and persist it."""
    session = fyersModel.SessionModel(
        client_id=env["client_id"],
        secret_key=env["secret_key"],
        redirect_uri=env["redirect_uri"],
        response_type="code",
        grant_type="authorization_code",
    )
    session.set_token(auth_code)

    response = session.generate_token()
    if "access_token" not in response:
        raise RuntimeError(f"Token generation failed: {response}")

    access_token = response["access_token"]
    _save_to_env(_ACCESS_TOKEN_KEY, access_token)
    print("[auth] Access token saved to .env")
    return access_token


# ── public entry-point ────────────────────────────────────────────────────────

def authenticate(
    headless: bool = False,
    browser: str | None = None,
) -> str:
    """
    Full Fyers authentication flow.

    Parameters
    ----------
    headless : bool
        If *True*, skip the browser step and use the ``auth_code`` already
        present in the .env file (useful for re-generating an access token
        without repeating the login).

    browser : str | None
        Which browser to open the Fyers login page in.

        +-----------+----------------------------------------------------+
        | Value     | Behaviour                                          |
        +===========+====================================================+
        | ``None``  | System default browser (via Python webbrowser mod) |
        | "chrome"  | Google Chrome or Chromium                          |
        | "firefox" | Mozilla Firefox                                    |
        +-----------+----------------------------------------------------+

        Works on both **Windows** and **Linux** (and macOS).
        Gracefully falls back to the system default if the requested
        browser is not installed.

    Returns
    -------
    str
        A valid Fyers access token.

    Examples
    --------
    >>> from fyers_auth import authenticate
    >>> token = authenticate()                    # system default
    >>> token = authenticate(browser="chrome")   # force Chrome
    >>> token = authenticate(browser="firefox")  # force Firefox
    >>> token = authenticate(headless=True)       # reuse saved auth_code
    """
    env = _load_env()

    missing = [k for k, v in env.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing required .env variables: {', '.join(missing)}"
        )

    if headless:
        auth_code = os.getenv(_AUTH_CODE_KEY)
        if not auth_code:
            raise EnvironmentError(
                "headless=True requires 'auth_code' to be set in .env"
            )
        print("[auth] Using saved auth_code (headless mode).")
    else:
        auth_code = get_auth_code(env, browser=browser)

    return get_access_token(env, auth_code)


# ── CLI convenience ───────────────────────────────────────────────────────────
# python -m fyers_auth.auth [--headless] [--browser chrome|firefox]

if __name__ == "__main__":
    _headless = "--headless" in sys.argv
    _browser: str | None = None

    if "--browser" in sys.argv:
        idx = sys.argv.index("--browser")
        try:
            _browser = sys.argv[idx + 1]
        except IndexError:
            print("ERROR: --browser requires a value  (chrome | firefox)")
            sys.exit(1)

    _token = authenticate(headless=_headless, browser=_browser)
    print(f"\nAccess token:\n{_token}")
