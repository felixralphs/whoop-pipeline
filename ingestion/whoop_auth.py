import os
import webbrowser
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import requests
from dotenv import load_dotenv
import secrets

load_dotenv()

CLIENT_ID = os.getenv("WHOOP_CLIENT_ID")
CLIENT_SECRET = os.getenv("WHOOP_CLIENT_SECRET")
REDIRECT_URI = os.getenv("WHOOP_REDIRECT_URI")
AUTH_URL = "https://api.prod.whoop.com/oauth/oauth2/auth"
TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
SCOPES = "read:recovery read:sleep read:workout read:cycles read:profile read:body_measurement offline"

auth_code = None

class CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            auth_code = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Auth successful! You can close this tab.")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Auth failed.")

    def log_message(self, format, *args):
        pass  # suppress server logs

def get_auth_code():
    state = secrets.token_urlsafe(16)  # generates a secure random string
    
    url = (
        f"{AUTH_URL}?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={SCOPES}"
        f"&state={state}"
    )
    
    server = HTTPServer(("localhost", 8000), CallbackHandler)
    print("Opening browser for WHOOP login...")
    threading.Timer(1, lambda: webbrowser.open(url)).start()
    server.handle_request()
    return auth_code

def get_tokens(code):
    response = requests.post(TOKEN_URL, data={
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
    })
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    code = get_auth_code()
    if code:
        tokens = get_tokens(code)
        print("Access token retrieved successfully!")
        print(f"Token type: {tokens.get('token_type')}")
        print(f"Expires in: {tokens.get('expires_in')} seconds")
        
        # Save tokens to file (gitignored)
        import json
        with open("token.json", "w") as f:
            json.dump(tokens, f, indent=2)
        print("Tokens saved to token.json")
    else:
        print("Failed to get auth code")