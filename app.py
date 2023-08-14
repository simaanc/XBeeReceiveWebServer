from flask import Flask, request
import os
import csv
import configparser
from pathlib import Path

# Paths and configuration
source_path = Path(__file__).resolve()
source_dir = source_path.parent
config = configparser.ConfigParser()
configLocation = str(source_dir) + "/configfile.ini"

app = Flask(__name__)

def write_file():
    with open(configLocation, "w") as configfile:
        config.write(configfile)

if not os.path.exists(configLocation):
    config["ServerConf"] = {
        "api_key": "your_api_key_here",
    }
    write_file()
else:
    config.read(configLocation)
    print(config.sections())

API_KEY = config["ServerConf"]["api_key"]

def validate_token(token):
    return token == API_KEY

CSV_FILE = "received_data.csv"

def write_data_to_csv(data):
    headers = ["source_address_64", "date_time", "data"]
    mode = "a" if os.path.exists(CSV_FILE) else "w"

    with open(CSV_FILE, mode, newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        if mode == "w":
            csv_writer.writeheader()
        csv_writer.writerow(data)

@app.route('/receive', methods=['POST'])
def receive_data():
    try:
        # Get the Bearer token from the Authorization header
        bearer_token = request.headers.get('Authorization')
        if not bearer_token:
            return "Unauthorized", 401

        # Extract the token value from the Bearer token
        token = bearer_token.split(' ')[1]

        if not validate_token(token):
            return "Unauthorized", 401

        data = request.get_json()
        print("Received POST request data:")
        print(data)

        # Store data in CSV file
        write_data_to_csv(data)

        return "Data received and stored successfully", 200
    except Exception as e:
        print("Error:", e)
        return "Error processing data", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)