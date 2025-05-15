from flask import Flask, redirect, request, jsonify
import requests
from urllib.parse import urlencode
from highlevel_sdk.config import HighLevelConfig
# from config import HighLevelConfig


app = Flask(__name__)


@app.route("/initiate")
def initiate_auth():
    app_config = {
        "clientId": HighLevelConfig.CLIENT_ID,
        "baseUrl": HighLevelConfig.AUTH_BASE_URL,
    }

    options = {
        "requestType": "code",
        "redirectUri": HighLevelConfig.REDIRECT_URI,
        "clientId": app_config["clientId"],
        "scopes": HighLevelConfig.SCOPES,
    }

    params = {
        "response_type": options["requestType"],
        "redirect_uri": options["redirectUri"],
        "client_id": options["clientId"],
        "scope": " ".join(options["scopes"]),
    }

    authorize_url = f"{app_config['baseUrl']}/oauth/chooselocation?{urlencode(params)}"
    print(authorize_url)
    return redirect(authorize_url)


@app.route("/oauth/callback")
def handle_callback():
    app_config = {
        "clientId": HighLevelConfig.CLIENT_ID,
        "clientSecret": HighLevelConfig.CLIENT_SECRET,
    }

    data = {
        "client_id": app_config["clientId"],
        "client_secret": app_config["clientSecret"],
        "grant_type": "authorization_code",
        "code": request.args.get("code"),
        "user_type": "Location",
        "redirect_uri": HighLevelConfig,
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    response = requests.post(
        f"{HighLevelConfig.API_BASE_URL}/oauth/token", data=data, headers=headers
    )

    return response.json()


def refresh_token(refresh_token):
    app_config = {
        "clientId": HighLevelConfig.CLIENT_ID,
        "clientSecret": HighLevelConfig.CLIENT_SECRET,
    }

    data = {
        "client_id": app_config["clientId"],
        "client_secret": app_config["clientSecret"],
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "user_type": "Location",
        "redirect_uri": HighLevelConfig,
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(
        f"{HighLevelConfig.API_BASE_URL}/oauth/token", data=data, headers=headers
    )

    return response.json()


if __name__ == "__main__":
    app.run(port=3000)
