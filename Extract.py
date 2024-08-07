
import threading
import time
import signal
import sys
from flask import Flask, request, redirect
import requests
import base64
import json
import webbrowser
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
dotenv_path = "C:/Users/asus/Documents/Learning Materials/ETL/secrets.env"
load_dotenv(dotenv_path)

# Access environment variables
CLIENT_ID = os.getenv('client_id')
CLIENT_SECRET = os.getenv('client_secret')
REDIRECT_URI = os.getenv('redirect_uri')
SCOPE = os.getenv('scope')

app = Flask(__name__)

# Replace with your actual client ID and client secret
# Replace with your actual client ID and client secret
#CLIENT_ID = 'a68dc95e9b5b45379a7b70df4848cda6'
#CLIENT_SECRET = '14b4ab897408428d9f328ec44c19bb47'
#REDIRECT_URI = 'http://localhost:8888/callback'
#SCOPE = 'user-read-recently-played'

# Global variables
access_token = None
song_df = None  # Global variable to store the DataFrame


def get_auth_url():
    return (
        f"https://accounts.spotify.com/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope={SCOPE}"
    )


def get_token(auth_code):
    token_url = 'https://accounts.spotify.com/api/token'
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth_str = base64.urlsafe_b64encode(auth_str.encode()).decode()

    headers = {
        'Authorization': f'Basic {b64_auth_str}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    payload = {
        'grant_type': 'authorization_code',
        'code': auth_code,
        'redirect_uri': REDIRECT_URI
    }

    response = requests.post(token_url, headers=headers, data=payload)
    return response.json()


@app.route('/')
def index():
    auth_url = get_auth_url()
    return redirect(auth_url)


@app.route('/callback')
def callback():
    global access_token, song_df
    auth_code = request.args.get('code')
    token_info = get_token(auth_code)
    access_token = token_info.get('access_token')

    if access_token:
        print(f"Access Token: {access_token}")
        fetch_and_save_recently_played(access_token)
        # Stop the server after fetching the data
        shutdown_server()
    return 'Token received! You can close this tab.'


def fetch_and_save_recently_played(token):
    global song_df  # Declare the global variable
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get('https://api.spotify.com/v1/me/player/recently-played', headers=headers)

    if response.status_code == 200:
        data = response.json()
        # Print the entire JSON response
        print(json.dumps(data, indent=2))

    if response.status_code == 200:
        data = response.json()
    
        song_names = []
        artist_names = []
        played_ats = []
        timestamps = []

        if 'items' in data:
            for song in data['items']:
                song_names.append(song['track']['name'])
                artist_names.append(song['track']['album']['artists'][0]['name'])
                played_ats.append(song['played_at'])
                timestamps.append(song['played_at'])  # Adjust if 'timestamp' field is different
        else:
            print("Key 'items' not found in the data dictionary")

        song_dict = {
            "song_name": song_names,
            "artist_name": artist_names,
            "played_at": played_ats,
            "timestamp": timestamps
        }

        song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
        print(song_df)

        # Save the DataFrame to a CSV file
        song_df.to_csv('recently_played.csv', index=False)
    else:
        print(f"API request failed with status code {response.status_code}")
        print(response.text)


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def run_app():
    webbrowser.open('http://localhost:8888')
    app.run(port=8888)

def start_oauth_flow():
    server_thread = threading.Thread(target=run_app)
    server_thread.start()
    server_thread.join()

def initiate_data_fetch():
    start_oauth_flow()

def return_dataframe():
    global song_df
    if song_df is None:
        raise ValueError("Data has not been loaded yet")
    return song_df
